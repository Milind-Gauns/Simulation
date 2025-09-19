# simulation.py
import pandas as pd
import numpy as np
import math

def run_simulation(
    master_workbook,          # str path or file-like buffer
    settings: pd.DataFrame,
    lgs: pd.DataFrame,
    fps: pd.DataFrame,
    vehicles: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Two-phase simulation (LG->FPS then CG->LG)
    Returns (dispatch_cg, dispatch_lg, stock_levels)
    """

    # -----------------------------
    # Helpers
    # -----------------------------
    def _get_setting(param_name, default=None, cast=float):
        try:
            val = settings.loc[settings["Parameter"] == param_name, "Value"].iloc[0]
            return cast(val)
        except Exception:
            if default is None:
                raise ValueError(f"Missing required setting: {param_name}")
            return cast(default)

    def _find_col(df: pd.DataFrame, candidates: list[str]):
        """
        Return the first matching column name from candidates (case-sensitive first),
        otherwise try case-insensitive match. Returns None if not found.
        """
        for c in candidates:
            if c in df.columns:
                return c
        low_map = {col.lower(): col for col in df.columns}
        for c in candidates:
            lc = c.lower()
            if lc in low_map:
                return low_map[lc]
        return None

    # -----------------------------
    # 0) Read key parameters safely
    # -----------------------------
    DAYS       = _get_setting("Distribution_Days", default=30, cast=int)
    TRUCK_CAP  = _get_setting("Vehicle_Capacity_tons", default=11.5, cast=float)
    TOT_V      = _get_setting("Vehicles_Total", default=30, cast=int)
    MAX_TRIPS  = _get_setting("Max_Trips_Per_Vehicle_Per_Day", default=3, cast=int)
    DEFAULT_LEAD = _get_setting("Default_Lead_Time_days", default=3, cast=float)

    # The maximum number of pre-days we may try when searching feasibility (configurable)
    MAX_PRE_DAYS = int(_get_setting("Max_Pre_Days", default=30, cast=int))

    # -----------------------------
    # Date mapping (cover pre-days too), skip Sundays
    # -----------------------------
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
    # tolerate capitalization/variant column names for LG_ID / LG_Name
    lg_id_col = _find_col(lgs, ["LG_ID", "Lg_ID", "lg_id"])
    lg_name_col = _find_col(lgs, ["LG_Name", "LG_NAME", "Lg_Name", "lg_name"])
    if lg_id_col is None or lg_name_col is None:
        raise ValueError("LGs sheet must contain columns: LG_ID, LG_Name (case-insensitive).")

    # normalize LG_ID and LG_Name to expected names for internal use
    lgs = lgs.rename(columns={lg_id_col: "LG_ID", lg_name_col: "LG_Name"})
    # Ensure LG_ID are ints
    lgs["LG_ID"] = pd.to_numeric(lgs["LG_ID"], errors="coerce").astype("Int64")
    if lgs["LG_ID"].isna().any():
        raise ValueError("LGs.LG_ID contains non-numeric or missing values.")

    lgid_by_name = {str(nm).strip().lower(): int(lg_id) for lg_id, nm in zip(lgs["LG_ID"], lgs["LG_Name"])}
    valid_lg_ids = set(int(x) for x in lgs["LG_ID"].dropna().astype(int))

    def normalize_lg_ref(val):
        if pd.isna(val):
            return None
        s = str(val).strip()
        try:
            i = int(float(s))
            return i if i in valid_lg_ids else None
        except Exception:
            return lgid_by_name.get(s.lower())

    # Validate FPS required columns exist (allow variants)
    fps = fps.copy()
    fps_id_col = _find_col(fps, ["FPS_ID", "Fps_ID", "fps_id"])
    max_cap_col = _find_col(fps, ["Max_Capacity_tons", "Max_Capacity_Tons", "Max_Capacity"])
    linked_lg_col = _find_col(fps, ["Linked_LG_ID", "Linked_LGID", "Linked_LG", "Linked_LG_Id", "Linked_LG"])
    if fps_id_col is None or max_cap_col is None or linked_lg_col is None:
        missing = []
        if fps_id_col is None: missing.append("FPS_ID")
        if max_cap_col is None: missing.append("Max_Capacity_tons")
        if linked_lg_col is None: missing.append("Linked_LG_ID")
        raise ValueError(f"FPS sheet missing required columns: {missing}")

    # rename to expected internal names
    fps = fps.rename(columns={fps_id_col: "FPS_ID", max_cap_col: "Max_Capacity_tons", linked_lg_col: "Linked_LG_ID"})

    # Ensure Lead_Time_days exists and fill NaN with default
    if "Lead_Time_days" not in fps.columns:
        fps["Lead_Time_days"] = DEFAULT_LEAD
    else:
        fps["Lead_Time_days"] = fps["Lead_Time_days"].fillna(DEFAULT_LEAD)

    # -----------------------------
    # 2) Demand from RC counts (AAY, PHH, APL)
    # -----------------------------
    AAY_kg = _get_setting("AAY_kg_per_card", default=35.0, cast=float)
    PHH_kg = _get_setting("PHH_kg_per_beneficiary", default=5.0, cast=float)
    APL_kg = _get_setting("APL_kg_per_card", default=0.0, cast=float)

    # normalize/ensure count columns exist (tolerant to variants)
    aay_col = _find_col(fps, ["AAY_Count", "AAY_Counts", "Aay_Count"])
    phh_col = _find_col(fps, ["PHH_Beneficiaries", "PHH_Beneficiary", "Phh_Beneficiaries"])
    apl_col = _find_col(fps, ["APL_Count", "APL_Counts", "Apl_Count"])
    # create canonical columns
    fps["AAY_Count"] = pd.to_numeric(fps[aay_col], errors="coerce").fillna(0.0) if aay_col else 0.0
    fps["PHH_Beneficiaries"] = pd.to_numeric(fps[phh_col], errors="coerce").fillna(0.0) if phh_col else 0.0
    fps["APL_Count"] = pd.to_numeric(fps[apl_col], errors="coerce").fillna(0.0) if apl_col else 0.0

    fps["Monthly_from_counts_kg"] = (
        fps["AAY_Count"] * AAY_kg
        + fps["PHH_Beneficiaries"] * PHH_kg
        + fps["APL_Count"] * APL_kg
    )

    # Respect existing Monthly_Demand_tons if provided (>0), otherwise derive from counts.
    monthly_col = _find_col(fps, ["Monthly_Demand_tons", "Monthly_Demand_Tons", "MonthlyDemand_tons"])
    if monthly_col:
        fps["Monthly_Demand_tons"] = pd.to_numeric(fps[monthly_col], errors="coerce").fillna(0.0)
    else:
        fps["Monthly_Demand_tons"] = 0.0
    # only fill from counts where user hasn't provided a positive value
    mask_use_counts = fps["Monthly_Demand_tons"].fillna(0.0) <= 0.0
    fps.loc[mask_use_counts, "Monthly_Demand_tons"] = (fps.loc[mask_use_counts, "Monthly_from_counts_kg"] / 1000.0)

    fps["Daily_Demand_tons"] = fps["Monthly_Demand_tons"] / 30.0
    fps["Reorder_Threshold_tons"] = fps["Daily_Demand_tons"] * fps["Lead_Time_days"]

    # Attach LG_ID (normalized) to each FPS
    fps["LG_ID"] = fps["Linked_LG_ID"].apply(normalize_lg_ref)
    if fps["LG_ID"].isna().any():
        bad_rows = fps[fps["LG_ID"].isna()][["FPS_ID", "Linked_LG_ID"]].head(5)
        raise ValueError(
            "Some FPS rows couldn't map Linked_LG_ID to a valid LG_ID. "
            f"Examples:\n{bad_rows.to_string(index=False)}\n"
            "Ensure Linked_LG_ID is either a valid LG_ID or a valid LG_Name."
        )
    fps["LG_ID"] = fps["LG_ID"].astype(int)

    # -----------------------------
    # 3) Vehicles mapping
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
    # 4) LG -> FPS simulation
    # -----------------------------
    # tolerate various initial columns: Initial_LG_Stock, Initial_LG_stock, Initial_Allocation_tons
    init_candidates = ["Initial_LG_Stock", "Initial_LG_stock", "Initial_Allocation_tons", "Initial_Allocation_Tons"]
    init_col = _find_col(lgs, init_candidates)
    if init_col:
        lgs["_initial_stock_used"] = pd.to_numeric(lgs[init_col], errors="coerce").fillna(0.0)
    else:
        # fallback to zero if nothing present
        lgs["_initial_stock_used"] = 0.0

    # Ensure we have a numeric LG_ID for iteration
    lgs["LG_ID"] = pd.to_numeric(lgs["LG_ID"], errors="coerce").astype(int)

    lg_stock = {int(row["LG_ID"]): float(row["_initial_stock_used"]) for _, row in lgs.iterrows()}
    fps_stock = {int(fid): 0.0 for fid in fps["FPS_ID"]}

    dispatch_lg_rows = []
    stock_rows = []

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

            # category split
            fps_row = fps.loc[fps["FPS_ID"] == fid].iloc[0]
            total_kg = float(fps_row.get("Monthly_from_counts_kg", 0.0))
            if total_kg > 0:
                aay_kg = float(fps_row.get("AAY_Count", 0.0)) * AAY_kg
                phh_kg = float(fps_row.get("PHH_Beneficiaries", 0.0)) * PHH_kg
                apl_kg = float(fps_row.get("APL_Count", 0.0)) * APL_kg
                aay_frac = aay_kg / total_kg if total_kg else 0.0
                phh_frac = phh_kg / total_kg if total_kg else 0.0
                apl_frac = apl_kg / total_kg if total_kg else 0.0
            else:
                aay_frac = phh_frac = apl_frac = 0.0

            aay_tons = qty * aay_frac
            phh_tons = qty * phh_frac
            apl_tons = qty * apl_frac
            nfsa_tons = aay_tons + phh_tons

            # safe date lookup with fallback
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
                "Date":
