# simulation.py
import pandas as pd
import numpy as np
import math
from typing import Tuple

def run_simulation(
    master_workbook,          # str path or file-like buffer
    settings: pd.DataFrame = None,
    lgs: pd.DataFrame = None,
    fps: pd.DataFrame = None,
    vehicles: pd.DataFrame = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Two-phase simulation (LG->FPS then CG->LG)
    Returns (dispatch_cg, dispatch_lg, stock_levels)

    master_workbook: path or buffer (used to read optional LG_Capacity sheet)
    settings, lgs, fps, vehicles: DataFrames already loaded by caller
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

    def _find_col(df: pd.DataFrame, candidates):
        """Find first candidate column (case-sensitive), otherwise case-insensitive. Return None if not found."""
        if df is None:
            return None
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
    DAYS       = int(_get_setting("Distribution_Days", default=30, cast=int))
    TRUCK_CAP  = float(_get_setting("Vehicle_Capacity_tons", default=11.5, cast=float))
    TOT_V      = int(_get_setting("Vehicles_Total", default=30, cast=int))
    MAX_TRIPS  = int(_get_setting("Max_Trips_Per_Vehicle_Per_Day", default=3, cast=int))
    DEFAULT_LEAD = float(_get_setting("Default_Lead_Time_days", default=3, cast=float))
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
    lgs["LG_ID"] = pd.to_numeric(lgs["LG_ID"], errors="coerce")
    if lgs["LG_ID"].isna().any():
        raise ValueError("LGs.LG_ID contains non-numeric or missing values.")
    lgs["LG_ID"] = lgs["LG_ID"].astype(int)

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

    # -----------------------------
    # Validate FPS columns & compute demand
    # -----------------------------
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

    # Rename to canonical internal names
    fps = fps.rename(columns={fps_id_col: "FPS_ID", max_cap_col: "Max_Capacity_tons", linked_lg_col: "Linked_LG_ID"})

    # Ensure Lead_Time_days exists and fill NaN with default
    if "Lead_Time_days" not in fps.columns:
        fps["Lead_Time_days"] = DEFAULT_LEAD
    else:
        fps["Lead_Time_days"] = pd.to_numeric(fps["Lead_Time_days"], errors="coerce").fillna(DEFAULT_LEAD)

    # -----------------------------
    # Demand from RC counts (AAY, PHH, APL)
    # -----------------------------
    AAY_kg = _get_setting("AAY_kg_per_card", default=35.0, cast=float)
    PHH_kg = _get_setting("PHH_kg_per_beneficiary", default=5.0, cast=float)
    APL_kg = _get_setting("APL_kg_per_card", default=0.0, cast=float)

    # locate possible count columns
    aay_col = _find_col(fps, ["AAY_Count", "AAY_Counts", "Aay_Count"])
    phh_col = _find_col(fps, ["PHH_Beneficiaries", "PHH_Beneficiary", "Phh_Beneficiaries"])
    apl_col = _find_col(fps, ["APL_Count", "APL_Counts", "Apl_Count"])

    # create canonical numeric columns
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
        lgs["_initial_stock_used"] = 0.0

    # Ensure LG_ID int (already done above)
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

    # Build DataFrames
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
    # 5) Derive LG daily requirement from dispatch_lg
    # -----------------------------------------------
    required_cols = {"LG_ID", "Day", "Quantity_tons"}
    missing = required_cols - set(dispatch_lg.columns)
    if missing:
        raise ValueError(f"dispatch_lg is missing required columns: {missing}")

    if dispatch_lg.empty:
        lg_daily_req = (
            pd.MultiIndex.from_product([sorted(valid_lg_ids), range(1, DAYS + 1)], names=["LG_ID","Day"])
            .to_frame(index=False)
            .assign(Daily_Requirement_tons=0.0)
        )
    else:
        lg_daily_req = (
            dispatch_lg
            .groupby(["LG_ID", "Day"])["Quantity_tons"]
            .sum()
            .reset_index()
            .rename(columns={"Quantity_tons": "Daily_Requirement_tons"})
        )

    req_pivot = lg_daily_req.pivot_table(
        index="LG_ID", columns="Day",
        values="Daily_Requirement_tons",
        aggfunc="sum", fill_value=0.0
    )

    # -----------------------------------------------
    # 6) CG -> LG PRE-DISPATCH (same DAYS timeline)
    # -----------------------------------------------
    try:
        cap_df = pd.read_excel(master_workbook, sheet_name="LG_Capacity")
        if {"LG_ID", "Capacity_tons"} <= set(cap_df.columns):
            capacity = {int(r["LG_ID"]): float(r["Capacity_tons"]) for _, r in cap_df.iterrows()}
        else:
            raise ValueError
    except Exception:
        # tolerate variants in lgs for capacity
        cap_col = _find_col(lgs, ["Storage_Capacity_tons", "Storage_Capacity_Tons", "Capacity_tons"])
        if cap_col is None:
            raise ValueError("Provide LG_Capacity sheet or 'Storage_Capacity_tons' in LGs.")
        capacity = {int(r["LG_ID"]): float(r[cap_col]) for _, r in lgs.iterrows()}

    # initial LG stock — tolerate several column name variants and precedence
    init_candidates = ["Initial_LG_Stock", "Initial_LG_stock", "Initial_Allocation_tons", "Initial_Allocation_Tons"]
    init_col_lgs = _find_col(lgs, init_candidates)
    if init_col_lgs:
        lg_stock_base = {int(r["LG_ID"]): float(r.get(init_col_lgs, 0.0)) for _, r in lgs.iterrows()}
    else:
        # fallback to zero
        lg_stock_base = {int(r["LG_ID"]): 0.0 for _, r in lgs.iterrows()}

    # Align types for req_pivot
    req_pivot = req_pivot.copy()
    req_pivot.index = [int(x) for x in req_pivot.index]
    req_pivot.columns = [int(c) for c in req_pivot.columns]

    lg_ids = list(req_pivot.index) if not req_pivot.empty else sorted(list(valid_lg_ids))

    def _get_demand(lg_id: int, day: int) -> float:
        try:
            return float(req_pivot.at[lg_id, day])
        except Exception:
            return 0.0

    def _free_room(stock: dict, lg_id: int) -> float:
        return max(0.0, capacity.get(lg_id, 0.0) - stock.get(lg_id, 0.0))

    def _simulate(pre_days: int, collect_rows: bool = False, include_pre_days: bool = False):
        start_day = 1 - pre_days
        stock = {lg: lg_stock_base.get(lg, 0.0) for lg in lg_ids}
        rows = [] if collect_rows else None

        for day in range(start_day, DAYS + 1):
            trips_left = TOT_V

            # A) Serve today's demand first (only when day >= 1)
            if day >= 1:
                order = sorted(lg_ids, key=lambda lg: -(_get_demand(lg, day) - stock[lg]))
                for lg in order:
                    demand_today = _get_demand(lg, day)
                    need_today = max(0.0, demand_today - stock[lg])

                    while trips_left > 0 and need_today > 1e-9:
                        room = _free_room(stock, lg)
                        if room <= 1e-9:
                            break
                        qty = min(TRUCK_CAP, need_today, room)
                        if qty <= 1e-9:
                            break

                        if collect_rows and (include_pre_days or day >= 1):
                            vid = TOT_V - trips_left + 1
                            row_date = day_to_date.get(int(day), start_date.date())
                            rows.append({
                                "Day": int(day),
                                "Date": row_date,
                                "Vehicle_ID": int(vid),
                                "LG_ID": int(lg),
                                "Quantity_tons": float(qty)
                            })

                        stock[lg] += qty
                        trips_left -= 1
                        need_today -= qty

                    if stock[lg] + 1e-6 < demand_today:
                        return False, (rows or []), start_day, stock

            # B) Pre-stock round-robin with remaining trips
            if trips_left > 0:
                future_unmet = {
                    lg: max(0.0, sum(_get_demand(lg, d) for d in range(max(1, day + 1), DAYS + 1)) - stock[lg])
                    for lg in lg_ids
                }
                candidates = [lg for lg, fu in future_unmet.items() if fu > 1e-6 and _free_room(stock, lg) > 1e-6]
                idx = 0
                while trips_left > 0 and candidates:
                    lg = candidates[idx % len(candidates)]
                    room = _free_room(stock, lg)
                    deliver = min(TRUCK_CAP, future_unmet[lg], room)

                    if deliver > 1e-9:
                        if collect_rows and (include_pre_days or day >= 1):
                            vid = TOT_V - trips_left + 1
                            row_date = day_to_date.get(int(day), start_date.date())
                            rows.append({
                                "Day": int(day),
                                "Date": row_date,
                                "Vehicle_ID": int(vid),
                                "LG_ID": int(lg),
                                "Quantity_tons": float(deliver)
                            })
                        stock[lg] += deliver
                        future_unmet[lg] = max(0.0, future_unmet[lg] - deliver)
                        trips_left -= 1

                    if future_unmet[lg] < 1e-6 or _free_room(stock, lg) < 1e-6:
                        candidates.remove(lg)
                        idx -= 1
                    idx += 1

            # C) End-of-day consumption (only Day >= 1)
            if day >= 1:
                for lg in lg_ids:
                    stock[lg] = max(0.0, stock[lg] - _get_demand(lg, day))

        return True, (rows or []), start_day, stock

    # Find minimal pre_days that makes schedule feasible
    pre_days = None
    for x in range(0, MAX_PRE_DAYS + 1):
        ok, _, start_day, _ = _simulate(pre_days=x, collect_rows=False)
        if ok:
            pre_days = x
            break

    if pre_days is None:
        raise RuntimeError("Unable to meet all demands within MAX_PRE_DAYS.")

    # Re-run with logging, include pre-days
    ok, rows, start_day, _ = _simulate(pre_days=pre_days, collect_rows=True, include_pre_days=True)
    assert ok

    dispatch_cg = pd.DataFrame(rows, columns=["Day", "Date", "Vehicle_ID", "LG_ID", "Quantity_tons"])

    # Build accurate LG stock levels (init + CG_cum - LG_cum)
    lg_ids_sorted = sorted(int(x) for x in lgs["LG_ID"].dropna().astype(int).unique())

    # tolerate multiple initial-stock column names for init_series
    init_col_for_series = _find_col(lgs, ["Initial_LG_Stock", "Initial_LG_stock", "Initial_Allocation_tons", "Initial_Allocation_Tons"])
    if init_col_for_series:
        init_series = (
            lgs.assign(LG_ID=lgs["LG_ID"].astype(int))
               .set_index("LG_ID")[init_col_for_series]
               .reindex(lg_ids_sorted).fillna(0.0)
        )
    else:
        init_series = pd.Series(0.0, index=lg_ids_sorted)

    # CG receipts cumulative (include pre-days start_day..DAYS → then keep 1..DAYS)
    if not dispatch_cg.empty:
        dcg = dispatch_cg.copy()
        dcg["LG_ID"] = dcg["LG_ID"].astype(int)
        dcg["Day"]   = dcg["Day"].astype(int)
        cg_piv = dcg.pivot_table(index="LG_ID", columns="Day",
                                 values="Quantity_tons", aggfunc="sum", fill_value=0.0)
        full_cols = list(range(start_day, DAYS + 1))  # includes negative/zero pre-days
        cg_piv = cg_piv.reindex(index=lg_ids_sorted, columns=full_cols, fill_value=0.0)
        cg_cum = cg_piv.cumsum(axis=1).reindex(columns=list(range(1, DAYS + 1)), fill_value=0.0)
    else:
        cg_cum = pd.DataFrame(0.0, index=lg_ids_sorted, columns=list(range(1, DAYS + 1)))

    # LG→FPS dispatch cumulative (compute directly from dispatch_lg)
    if not dispatch_lg.empty:
        dlg = dispatch_lg.copy()
        if "Quantity_tons" not in dlg.columns:
            dlg["Quantity_tons"] = dlg.get("Quantity_tons", 0.0)
        dlg["LG_ID"] = dlg["LG_ID"].astype(int)
        dlg["Day"]   = dlg["Day"].astype(int)
        lg_piv = dlg.pivot_table(index="LG_ID", columns="Day",
                                 values="Quantity_tons", aggfunc="sum", fill_value=0.0)
        lg_piv = lg_piv.reindex(index=lg_ids_sorted, columns=list(range(1, DAYS + 1)), fill_value=0.0)
        lg_cum = lg_piv.cumsum(axis=1)
    else:
        lg_cum = pd.DataFrame(0.0, index=lg_ids_sorted, columns=list(range(1, DAYS + 1)))

    # Stock = init + CG_cum − LG_cum
    stock_matrix = init_series.to_numpy()[:, None] + cg_cum.to_numpy() - lg_cum.to_numpy()
    stock_matrix = np.where(np.abs(stock_matrix) < 1e-9, 0.0, stock_matrix)

    lg_stock_levels = (
        pd.DataFrame(stock_matrix, index=lg_ids_sorted, columns=list(range(1, DAYS + 1)))
          .stack().rename("Stock_Level_tons")
          .rename_axis(index=["LG_ID", "Day"]).reset_index()
          .rename(columns={"LG_ID": "Entity_ID"})
          .assign(Entity_Type="LG")[["Day", "Entity_Type", "Entity_ID", "Stock_Level_tons"]]
    )

    # Include pre-day LG stocks (start_day..0) if applicable
    pre_cols = list(range(start_day, 1))
    if pre_cols:
        if not dispatch_cg.empty:
            cg_pre = cg_piv.reindex(index=lg_ids_sorted, columns=pre_cols, fill_value=0.0)
            cg_pre_cum = cg_pre.cumsum(axis=1)
        else:
            cg_pre_cum = pd.DataFrame(0.0, index=lg_ids_sorted, columns=pre_cols)

        stock_pre_matrix = init_series.to_numpy()[:, None] + cg_pre_cum.to_numpy()
        stock_pre_matrix = np.where(np.abs(stock_pre_matrix) < 1e-9, 0.0, stock_pre_matrix)

        lg_stock_levels_pre = (
            pd.DataFrame(stock_pre_matrix, index=lg_ids_sorted, columns=pre_cols)
              .stack().rename("Stock_Level_tons")
              .rename_axis(index=["LG_ID", "Day"]).reset_index()
              .rename(columns={"LG_ID": "Entity_ID"})
              .assign(Entity_Type="LG")[["Day", "Entity_Type", "Entity_ID", "Stock_Level_tons"]]
        )
    else:
        lg_stock_levels_pre = pd.DataFrame(columns=["Day", "Entity_Type", "Entity_ID", "Stock_Level_tons"])

    lg_stock_levels = pd.concat([lg_stock_levels_pre, lg_stock_levels], ignore_index=True)

    # Add Date column to LG stock rows using day_to_date mapping (covers pre-days too)
    if not lg_stock_levels.empty:
        lg_stock_levels["Date"] = lg_stock_levels["Day"].map(lambda d: day_to_date.get(int(d), pd.NaT))
    else:
        lg_stock_levels["Date"] = pd.NaT

    # Ensure stock_levels (FPS rows) have Date; if not, compute from Day mapping
    if 'Date' not in stock_levels.columns:
        stock_levels['Date'] = stock_levels['Day'].map(lambda d: day_to_date.get(int(d), pd.NaT))

    # Keep FPS rows (from earlier) and append LG rows; ensure consistent column ordering
    final_stock_levels = pd.concat(
        [stock_levels[["Day", "Date", "Entity_Type", "Entity_ID", "Stock_Level_tons"]].copy(),
         lg_stock_levels[["Day", "Date", "Entity_Type", "Entity_ID", "Stock_Level_tons"]].copy()],
        ignore_index=True
    )

    # Normalize dtypes
    final_stock_levels["Day"] = pd.to_numeric(final_stock_levels["Day"], errors="coerce").astype("Int64")
    final_stock_levels["Entity_ID"] = pd.to_numeric(final_stock_levels["Entity_ID"], errors="coerce").astype("Int64")
    final_stock_levels["Stock_Level_tons"] = pd.to_numeric(final_stock_levels["Stock_Level_tons"], errors="coerce").fillna(0.0)

    # Ensure dispatch_cg and dispatch_lg have stable core columns and 'Date' present
    if "Date" not in dispatch_cg.columns:
        dispatch_cg["Date"] = dispatch_cg["Day"].map(lambda d: day_to_date.get(int(d), pd.NaT))
    if "Date" not in dispatch_lg.columns:
        dispatch_lg["Date"] = dispatch_lg["Day"].map(lambda d: day_to_date.get(int(d), pd.NaT))

    # Re-order columns for predictability
    core_cg_cols = ["Day", "Date", "Vehicle_ID", "LG_ID", "Quantity_tons"]
    for col in core_cg_cols:
        if col not in dispatch_cg.columns:
            dispatch_cg[col] = pd.NA
    dispatch_cg = dispatch_cg[core_cg_cols + [c for c in dispatch_cg.columns if c not in core_cg_cols]]

    core_lg_cols = ["Day", "Date", "Vehicle_ID", "LG_ID", "FPS_ID", "Quantity_tons", "AAY_tons", "PHH_tons", "APL_tons", "NFSA_tons"]
    for col in core_lg_cols:
        if col not in dispatch_lg.columns:
            dispatch_lg[col] = 0.0 if col.endswith("_tons") or col == "Quantity_tons" else pd.NA
    dispatch_lg = dispatch_lg[core_lg_cols + [c for c in dispatch_lg.columns if c not in core_lg_cols]]

    # Final stock_levels column order
    final_stock_levels = final_stock_levels[["Day", "Date", "Entity_Type", "Entity_ID", "Stock_Level_tons"]]

    return dispatch_cg, dispatch_lg, final_stock_levels
