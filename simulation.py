# simulation.py

import streamlit
import io
import pandas as pd
import math
import numpy as np

def run_simulation(
    master_workbook,          # str path or file-like buffer
    settings: pd.DataFrame,
    lgs: pd.DataFrame,
    fps: pd.DataFrame,
    vehicles: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Runs a two-phase simulation with category-aware demand:
      - derives FPS demand from counts OR uses Monthly_Demand_tons override when provided
      - splits each dispatch into AAY_tons, PHH_tons, APL_tons and NFSA_tons (AAY+PHH)
    Returns: (dispatch_cg, dispatch_lg, stock_levels)
    """

    # -----------------------------
    # 0) Read key parameters safely
    # -----------------------------
    def _get_setting(param_name, default=None, cast=float):
        try:
            val = settings.loc[settings["Parameter"] == param_name, "Value"].iloc[0]
            return cast(val)
        except Exception:
            if default is None:
                raise ValueError(f"Missing required setting: {param_name}")
            return cast(default)

    DAYS       = _get_setting("Distribution_Days", default=30, cast=int)
    TRUCK_CAP  = _get_setting("Vehicle_Capacity_tons", default=11.5, cast=float)
    TOT_V      = _get_setting("Vehicles_Total", default=30, cast=int)
    MAX_TRIPS  = _get_setting("Max_Trips_Per_Vehicle_Per_Day", default=3, cast=int)
    DEFAULT_LEAD = _get_setting("Default_Lead_Time_days", default=3, cast=float)

    # category eligibility (kg)
    AAY_kg = _get_setting("AAY_kg_per_card", 35.0, float)
    PHH_kg = _get_setting("PHH_kg_per_beneficiary", 5.0, float)
    APL_kg = _get_setting("APL_kg_per_card", 0.0, float)

    # The maximum number of pre-days we may try when searching feasibility
    MAX_PRE_DAYS = 30

    # -----------------------------
    # Date mapping (cover pre-days too), skip Sundays
    # -----------------------------
    # Optional "Start_Date" setting (YYYY-MM-DD); fallback to today.
    start_date_val = None
    try:
        start_date_val = settings.loc[settings["Parameter"] == "Start_Date", "Value"].iloc[0]
    except Exception:
        start_date_val = None

    if pd.notna(start_date_val):
        try:
            start_date = pd.to_datetime(start_date_val).normalize()
        except Exception:
            start_date = pd.Timestamp.today().normalize()
    else:
        start_date = pd.Timestamp.today().normalize()

    # If start_date is Sunday, shift forward to next non-Sunday
    while start_date.weekday() == 6:  # Sunday == 6
        start_date += pd.Timedelta(days=1)

    # Forward dates for day 1..DAYS (excluding Sundays)
    forward_dates = []
    cur = start_date
    while len(forward_dates) < DAYS:
        if cur.weekday() != 6:
            forward_dates.append(cur.date())
        cur += pd.Timedelta(days=1)

    # Backward dates for day 0, -1, -2, ... (excluding Sundays)
    backward_dates = []
    cur = start_date - pd.Timedelta(days=1)
    while len(backward_dates) < MAX_PRE_DAYS:
        if cur.weekday() != 6:
            backward_dates.append(cur.date())  # backward_dates[0] -> day 0
        cur -= pd.Timedelta(days=1)

    # Build day_to_date mapping with fallback coverage
    day_to_date = {}
    for i, dt in enumerate(forward_dates, start=1):
        day_to_date[i] = dt
    for idx, dt in enumerate(backward_dates):
        day_to_date[0 - idx] = dt

    # -----------------------------
    # 1) Prepare LG & FPS mappings
    # -----------------------------
    lgs = lgs.copy()
    if "LG_ID" not in lgs.columns or "LG_Name" not in lgs.columns:
        raise ValueError("LGs sheet must contain columns: LG_ID, LG_Name")

    lgid_by_name = {str(nm).strip().lower(): int(lg_id) for lg_id, nm in zip(lgs["LG_ID"], lgs["LG_Name"])}
    valid_lg_ids = set(int(x) for x in lgs["LG_ID"])

    def normalize_lg_ref(val):
        if pd.isna(val):
            return None
        s = str(val).strip()
        try:
            i = int(float(s))
            return i if i in valid_lg_ids else None
        except Exception:
            return lgid_by_name.get(s.lower())

    # Validate FPS required columns exist (Monthly_Demand_tons may be missing but column not required here)
    req_cols = {"FPS_ID", "Max_Capacity_tons", "Linked_LG_ID"}
    missing = req_cols - set(fps.columns)
    if missing:
        raise ValueError(f"FPS sheet missing required columns: {missing}")

    fps = fps.copy()
    # Ensure Lead_Time_days exists and fill NaN with default
    if "Lead_Time_days" not in fps.columns:
        fps["Lead_Time_days"] = DEFAULT_LEAD
    else:
        fps["Lead_Time_days"] = fps["Lead_Time_days"].fillna(DEFAULT_LEAD)

    # counts columns: ensure present and numeric
    fps["AAY_Count"] = pd.to_numeric(fps.get("AAY_Count", 0), errors="coerce").fillna(0.0)
    fps["PHH_Beneficiaries"] = pd.to_numeric(fps.get("PHH_Beneficiaries", 0), errors="coerce").fillna(0.0)
    fps["APL_Count"] = pd.to_numeric(fps.get("APL_Count", 0), errors="coerce").fillna(0.0)

    # compute counts-derived monthly kg
    fps["Monthly_from_counts_kg"] = (
        fps["AAY_Count"] * AAY_kg
        + fps["PHH_Beneficiaries"] * PHH_kg
        + fps["APL_Count"] * APL_kg
    )

    # Monthly_Demand_tons: if user provided positive value -> use it; otherwise use counts-derived
    fps["Monthly_Demand_tons"] = pd.to_numeric(fps.get("Monthly_Demand_tons", pd.NA), errors="coerce")
    counts_derived_tons = (fps["Monthly_from_counts_kg"] / 1000.0).fillna(0.0)
    fps["Monthly_Demand_tons"] = fps["Monthly_Demand_tons"].where(fps["Monthly_Demand_tons"].notna() & (fps["Monthly_Demand_tons"] > 0), counts_derived_tons)
    fps["Daily_Demand_tons"] = fps["Monthly_Demand_tons"] / 30.0
    fps["Reorder_Threshold_tons"] = fps["Daily_Demand_tons"] * fps["Lead_Time_days"]

    fps["LG_ID"] = fps["Linked_LG_ID"].apply(normalize_lg_ref)
    if fps["LG_ID"].isna().any():
        bad_rows = fps[fps["LG_ID"].isna()][["FPS_ID", "Linked_LG_ID"]]
        raise ValueError(
            "Some FPS rows couldn't map Linked_LG_ID to a valid LG_ID. "
            f"Examples:\n{bad_rows.head(5).to_string(index=False)}"
        )
    fps["LG_ID"] = fps["LG_ID"].astype(int)

    # -----------------------------
    # 2) Vehicles mapping
    # -----------------------------
    vehicles = vehicles.copy()
    if vehicles.empty:
        vehicles = pd.DataFrame({
            "Vehicle_ID": list(range(1, TOT_V + 1)),
            "Capacity_tons": [TRUCK_CAP] * TOT_V,
            "Mapped_LG_IDs": [",".join(str(x) for x in sorted(valid_lg_ids))] * TOT_V
        })
    else:
        if "Vehicle_ID" not in vehicles.columns:
            raise ValueError("Vehicles sheet must contain 'Vehicle_ID'")
        if "Capacity_tons" not in vehicles.columns:
            vehicles["Capacity_tons"] = TRUCK_CAP
        if "Mapped_LG_IDs" not in vehicles.columns:
            vehicles["Mapped_LG_IDs"] = ",".join(str(x) for x in sorted(valid_lg_ids))

    def parse_lg_list(val):
        if pd.isna(val):
            return []
        out = []
        for token in str(val).split(","):
            token = token.strip()
            if not token:
                continue
            try:
                i = int(float(token))
                if i in valid_lg_ids:
                    out.append(i)
                    continue
            except Exception:
                pass
            mapped = normalize_lg_ref(token)
            if mapped is not None:
                out.append(mapped)
        return sorted(set(out))

    vehicles["Mapped_LGs_List"] = vehicles["Mapped_LG_IDs"].apply(parse_lg_list)
    if vehicles["Mapped_LGs_List"].apply(len).eq(0).any():
        bad = vehicles[vehicles["Mapped_LGs_List"].apply(len).eq(0)][["Vehicle_ID", "Mapped_LG_IDs"]]
        raise ValueError(
            "Some vehicles couldn't map any LGs from 'Mapped_LG_IDs'. "
            f"Examples:\n{bad.head(5).to_string(index=False)}"
        )

    # -----------------------------
    # 3) LG -> FPS simulation
    # -----------------------------
    if "Initial_Allocation_tons" not in lgs.columns:
        lgs["Initial_Allocation_tons"] = 0.0

    lg_stock = {int(row["LG_ID"]): float(row["Initial_Allocation_tons"]) for _, row in lgs.iterrows()}
    fps_stock = {int(fid): 0.0 for fid in fps["FPS_ID"]}

    dispatch_lg_rows = []
    stock_rows = []

    # Precompute FPS composition fractions (based on counts if available)
    # Use counts-derived kg (Monthly_from_counts_kg) to compute fractions; if all zero -> fractions zero.
    fps_comp = {}
    for _, r in fps.iterrows():
        fid = int(r["FPS_ID"])
        total_kg = float(r.get("Monthly_from_counts_kg", 0.0))
        if total_kg > 0:
            aay_kg = float(r.get("AAY_Count", 0.0)) * AAY_kg
            phh_kg = float(r.get("PHH_Beneficiaries", 0.0)) * PHH_kg
            apl_kg = float(r.get("APL_Count", 0.0)) * APL_kg
            # guard: if small mismatch due to rounding, normalize
            s = aay_kg + phh_kg + apl_kg
            if s <= 0:
                aay_frac = phh_frac = apl_frac = 0.0
            else:
                aay_frac = aay_kg / s
                phh_frac = phh_kg / s
                apl_frac = apl_kg / s
        else:
            aay_frac = phh_frac = apl_frac = 0.0
        fps_comp[fid] = {"AAY_frac": aay_frac, "PHH_frac": phh_frac, "APL_frac": apl_frac}

    for day in range(1, DAYS + 1):
        # consumption
        for _, r in fps.iterrows():
            fid = int(r["FPS_ID"])
            fps_stock[fid] = max(0.0, fps_stock[fid] - float(r["Daily_Demand_tons"]))

        needs = []
        for _, r in fps.iterrows():
            fid  = int(r["FPS_ID"])
            lgid = int(r["LG_ID"])
            current = fps_stock[fid]
            threshold = float(r["Reorder_Threshold_tons"])
            max_cap  = float(r["Max_Capacity_tons"])
            if current <= threshold:
                available_at_lg = lg_stock.get(lgid, 0.0)
                need_qty = min(max_cap - current, available_at_lg)
                if need_qty > 0:
                    urgency = (threshold - current) / float(r["Daily_Demand_tons"]) if r["Daily_Demand_tons"] > 0 else 0
                    needs.append((urgency, fid, lgid, need_qty))
        needs.sort(reverse=True, key=lambda x: x[0])

        vehicles["Trips_Used"] = 0

        for urgency, fid, lgid, need_qty in needs:
            cand = vehicles[vehicles["Mapped_LGs_List"].apply(lambda lst: lgid in lst)].copy()
            cand = cand[cand["Trips_Used"] < MAX_TRIPS]
            if cand.empty:
                continue

            cand["is_shared"] = cand["Mapped_LGs_List"].apply(lambda lst: len(lst) > 1)
            cand = cand.sort_values(["is_shared"], ascending=False)
            chosen = cand.iloc[0]

            vid = chosen["Vehicle_ID"]
            cap = float(chosen["Capacity_tons"])
            qty = min(cap, need_qty, lg_stock.get(lgid, 0.0))
            if qty <= 0:
                continue

            # compute category split using fps_comp fractions for that FPS
            comp = fps_comp.get(fid, {"AAY_frac": 0.0, "PHH_frac": 0.0, "APL_frac": 0.0})
            aay_tons = qty * comp["AAY_frac"]
            phh_tons = qty * comp["PHH_frac"]
            apl_tons = qty * comp["APL_frac"]
            nfsa_tons = aay_tons + phh_tons

            # safe date lookup
            row_date = day_to_date.get(int(day), start_date.date())

            dispatch_lg_rows.append({
                "Day": int(day),
                "Date": row_date,
                "Vehicle_ID": vid,
                "LG_ID": int(lgid),
                "FPS_ID": int(fid),
                "Quantity_tons": float(qty),
                "AAY_tons": float(aay_tons),
                "PHH_tons": float(phh_tons),
                "APL_tons": float(apl_tons),
                "NFSA_tons": float(nfsa_tons)
            })

            lg_stock[lgid] = lg_stock.get(lgid, 0.0) - qty
            fps_stock[fid] = fps_stock.get(fid, 0.0) + qty
            vehicles.loc[vehicles["Vehicle_ID"] == vid, "Trips_Used"] += 1

        # record end-of-day stocks (use safe date lookup)
        for lgid, st in lg_stock.items():
            stock_rows.append({
                "Day": int(day),
                "Date": day_to_date.get(int(day), start_date.date()),
                "Entity_Type": "LG",
                "Entity_ID": int(lgid),
                "Stock_Level_tons": float(st)
            })
        for fid, st in fps_stock.items():
            stock_rows.append({
                "Day": int(day),
                "Date": day_to_date.get(int(day), start_date.date()),
                "Entity_Type": "FPS",
                "Entity_ID": int(fid),
                "Stock_Level_tons": float(st)
            })

    dispatch_lg = pd.DataFrame(dispatch_lg_rows, columns=[
        "Day", "Date", "Vehicle_ID", "LG_ID", "FPS_ID", "Quantity_tons",
        "AAY_tons", "PHH_tons", "APL_tons", "NFSA_tons"
    ])
    stock_levels = pd.DataFrame(stock_rows, columns=["Day", "Date", "Entity_Type", "Entity_ID", "Stock_Level_tons"])

    if dispatch_lg.empty:
        dispatch_lg = pd.DataFrame(columns=[
            "Day", "Date", "Vehicle_ID", "LG_ID", "FPS_ID", "Quantity_tons",
            "AAY_tons", "PHH_tons", "APL_tons", "NFSA_tons"
        ])

    # -----------------------------------------------
    # 4) Derive LG daily requirement from dispatch_lg
    # -----------------------------------------------
    required_co_
