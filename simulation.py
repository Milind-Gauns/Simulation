# simulation.py (patched)
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
    Runs a two-phase simulation:

    1) LG → FPS dispatch (priority-based, with vehicle mapping + per-vehicle trip caps)
       - Produces `dispatch_lg` with columns: Day, Date, Vehicle_ID, LG_ID, FPS_ID, Quantity_tons
       - Produces `stock_levels` (LG & FPS end-of-day stock)

    2) CG → LG pre-dispatch using derived LG daily requirement from phase (1)
       - Produces `dispatch_cg` with columns: Day, Date, Vehicle_ID, LG_ID, Quantity_tons

    Returns:
        (dispatch_cg, dispatch_lg, stock_levels)
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

    # -----------------------------
    # Date mapping (cover pre-days too)
    # -----------------------------
    # Maximum pre-days the CG→LG pre-dispatch may request.
    MAX_PRE_DAYS = 30  # keep same constant used later; adjust if you want.

    # Start date: optional setting "Start_Date" (YYYY-MM-DD). If not provided use today.
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

    # We must **skip Sundays** — ensure the 'start_date' itself is a non-Sunday
    # (if it is Sunday, advance to next non-Sunday).
    while start_date.weekday() == 6:  # Sunday == 6
        start_date += pd.Timedelta(days=1)

    # Build forward (day 1..DAYS) non-Sunday dates
    forward_dates = []
    cur = start_date
    while len(forward_dates) < DAYS:
        if cur.weekday() != 6:
            forward_dates.append(cur.date())
        cur += pd.Timedelta(days=1)

    # Build backward dates for pre-days: day 0, -1, -2, ... (closest previous non-Sunday first)
    backward_dates = []
    cur = start_date - pd.Timedelta(days=1)
    while len(backward_dates) < MAX_PRE_DAYS:
        if cur.weekday() != 6:
            backward_dates.append(cur.date())  # backward_dates[0] -> day 0, [1] -> day -1 ...
        cur -= pd.Timedelta(days=1)

    # Create mapping day_int -> date (covering negative/zero pre-days and positive days)
    # day 1 -> forward_dates[0], day 2 -> forward_dates[1], ...
    # day 0 -> backward_dates[0], day -1 -> backward_dates[1], etc.
    day_to_date = {}
    for i, dt in enumerate(forward_dates, start=1):
        day_to_date[i] = dt
    for idx, dt in enumerate(backward_dates):
        day_to_date[0 - idx] = dt

    # -----------------------------
    # 1) Prepare LG & FPS mappings
    # -----------------------------
    # Normalize LG keys (support either ID or Name references in FPS.Linked_LG_ID)
    lgs = lgs.copy()
    if "LG_ID" not in lgs.columns or "LG_Name" not in lgs.columns:
        raise ValueError("LGs sheet must contain columns: LG_ID, LG_Name")

    # Build bi-directional maps
    lgid_by_name = {str(nm).strip().lower(): int(lg_id) for lg_id, nm in zip(lgs["LG_ID"], lgs["LG_Name"])}
    valid_lg_ids = set(int(x) for x in lgs["LG_ID"])

    def normalize_lg_ref(val):
        """Accepts either an int-like ID or a name; returns int LG_ID or None."""
        if pd.isna(val):
            return None
        s = str(val).strip()
        # Try as int ID
        try:
            i = int(float(s))  # handles "5" or "5.0"
            return i if i in valid_lg_ids else None
        except Exception:
            pass
        # Try as name
        return lgid_by_name.get(s.lower())

    # Make sure FPS has core columns
    req_cols = {"FPS_ID", "Monthly_Demand_tons", "Max_Capacity_tons", "Linked_LG_ID"}
    # Note: Monthly_Demand_tons may be blank in your upload (we compute it from RC counts later),
    # but the column must exist. If it doesn't, throw a helpful error.
    missing = req_cols - set(fps.columns)
    if missing:
        raise ValueError(f"FPS sheet missing required columns: {missing}")

    fps = fps.copy()
    # Ensure Lead_Time_days exists and fill NaN with default
    if "Lead_Time_days" not in fps.columns:
        fps["Lead_Time_days"] = DEFAULT_LEAD
    else:
        fps["Lead_Time_days"] = fps["Lead_Time_days"].fillna(DEFAULT_LEAD)

    # -----------------------------
    # Demand from RC counts (AAY, PHH, APL)
    # -----------------------------
    # We expect the FPS sheet to have new columns:
    #   - AAY_Count
    #   - PHH_Beneficiaries
    #   - APL_Count
    #
    # And the Settings sheet to include:
    #   - AAY_kg_per_card (default 35.0)
    #   - PHH_kg_per_beneficiary (default 5.0)
    #   - APL_kg_per_card (default 0.0)
    #
    # Compute monthly demand in tons (kg -> tons via / 1000).
    AAY_kg = _get_setting("AAY_kg_per_card", default=35.0, cast=float)
    PHH_kg = _get_setting("PHH_kg_per_beneficiary", default=5.0, cast=float)
    APL_kg = _get_setting("APL_kg_per_card", default=0.0, cast=float)

    # Ensure the count columns exist; if missing, create zero columns so downstream code doesn't break.
    if "AAY_Count" not in fps.columns:
        fps["AAY_Count"] = 0
    if "PHH_Beneficiaries" not in fps.columns:
        fps["PHH_Beneficiaries"] = 0
    if "APL_Count" not in fps.columns:
        fps["APL_Count"] = 0

    # Compute monthly_from_counts_kg and Monthly_Demand_tons (kg -> tons)
    fps["Monthly_from_counts_kg"] = (
        pd.to_numeric(fps["AAY_Count"], errors="coerce").fillna(0.0) * AAY_kg
        + pd.to_numeric(fps["PHH_Beneficiaries"], errors="coerce").fillna(0.0) * PHH_kg
        + pd.to_numeric(fps["APL_Count"], errors="coerce").fillna(0.0) * APL_kg
    )
    # convert to tons
    fps["Monthly_Demand_tons"] = fps["Monthly_from_counts_kg"] / 1000.0

    # Compute daily demand & reorder threshold
    fps["Daily_Demand_tons"] = fps["Monthly_Demand_tons"] / 30.0
    fps["Reorder_Threshold_tons"] = fps["Daily_Demand_tons"] * fps["Lead_Time_days"]

    # Attach LG_ID (normalized) to each FPS
    fps["LG_ID"] = fps["Linked_LG_ID"].apply(normalize_lg_ref)
    if fps["LG_ID"].isna().any():
        bad_rows = fps[fps["LG_ID"].isna()][["FPS_ID", "Linked_LG_ID"]]
        raise ValueError(
            "Some FPS rows couldn't map Linked_LG_ID to a valid LG_ID. "
            f"Examples:\n{bad_rows.head(5).to_string(index=False)}\n"
            "Ensure Linked_LG_ID is either a valid LG_ID or a valid LG_Name."
        )

    fps["LG_ID"] = fps["LG_ID"].astype(int)

    # -----------------------------
    # 2) Prepare Vehicles mapping
    # -----------------------------
    vehicles = vehicles.copy()
    if vehicles.empty:
        # Fallback: create a basic pool of vehicles all mapped to all LGs
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
            # If not given, assume each vehicle can serve all LGs
            vehicles["Mapped_LG_IDs"] = ",".join(str(x) for x in sorted(valid_lg_ids))

    # Parse Mapped_LG_IDs into normalized lists for easy filtering
    def parse_lg_list(val):
        if pd.isna(val):
            return []
        out = []
        for token in str(val).split(","):
            token = token.strip()
            if not token:
                continue
            # try ID then name
            try:
                i = int(float(token))
                if i in valid_lg_ids:
                    out.append(i)
                    continue
            except Exception:
                pass
            # maybe it is a name
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
    # 3) LG → FPS SIMULATION
    # -----------------------------
    # Initialize stocks
    if "Initial_Allocation_tons" not in lgs.columns:
        lgs["Initial_Allocation_tons"] = 0.0

    lg_stock = {int(row["LG_ID"]): float(row["Initial_Allocation_tons"]) for _, row in lgs.iterrows()}
    fps_stock = {int(fid): 0.0 for fid in fps["FPS_ID"]}

    dispatch_lg_rows = []
    stock_rows = []

    for day in range(1, DAYS + 1):
        # 3a) FPS consumes daily demand
        for _, r in fps.iterrows():
            fid = int(r["FPS_ID"])
            fps_stock[fid] = max(0.0, fps_stock[fid] - float(r["Daily_Demand_tons"]))

        # 3b) Compute needs
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

        # 3c) Reset vehicle usage counters for the day
        vehicles["Trips_Used"] = 0

        # 3d) Dispatch loop
        for urgency, fid, lgid, need_qty in needs:
            # candidate vehicles that can serve this LG and have trips left
            cand = vehicles[vehicles["Mapped_LGs_List"].apply(lambda lst: lgid in lst)].copy()
            cand = cand[cand["Trips_Used"] < MAX_TRIPS]
            if cand.empty:
                continue

            # Prefer shared vehicles (mapped to >1 LG)
            cand["is_shared"] = cand["Mapped_LGs_List"].apply(lambda lst: len(lst) > 1)
            cand = cand.sort_values(["is_shared"], ascending=False)
            chosen = cand.iloc[0]

            vid = chosen["Vehicle_ID"]
            cap = float(chosen["Capacity_tons"])
            qty = min(cap, need_qty, lg_stock.get(lgid, 0.0))
            if qty <= 0:
                continue

            # compute category split for FPS demand taken (derive from FPS composition)
            fps_row = fps.loc[fps["FPS_ID"] == fid].iloc[0]
            # fraction of FPS demand that is from each category on this dispatch:
            # We take current composition weights from monthly_from_counts_kg to split quantity.
            total_kg = float(fps_row.get("Monthly_from_counts_kg", 0.0))
            if total_kg > 0:
                aay_kg = float(fps_row.get("AAY_Count", 0.0)) * AAY_kg
                phh_kg = float(fps_row.get("PHH_Beneficiaries", 0.0)) * PHH_kg
                apl_kg = float(fps_row.get("APL_Count", 0.0)) * APL_kg
                # fractions
                aay_frac = aay_kg / total_kg if total_kg else 0.0
                phh_frac = phh_kg / total_kg if total_kg else 0.0
                apl_frac = apl_kg / total_kg if total_kg else 0.0
            else:
                aay_frac = phh_frac = apl_frac = 0.0

            aay_tons = qty * aay_frac
            phh_tons = qty * phh_frac
            apl_tons = qty * apl_frac
            nfsa_tons = aay_tons + phh_tons  # NFSA = AAY + PHH

            dispatch_lg_rows.append({
                "Day": int(day),
                "Date": day_to_date[int(day)],
                "Vehicle_ID": vid,
                "LG_ID": int(lgid),
                "FPS_ID": int(fid),
                "Quantity_tons": float(qty),
                "AAY_tons": float(aay_tons),
                "PHH_tons": float(phh_tons),
                "APL_tons": float(apl_tons),
                "NFSA_tons": float(nfsa_tons)
            })

            # update stocks & vehicle usage
            lg_stock[lgid] = lg_stock.get(lgid, 0.0) - qty
            fps_stock[fid] = fps_stock.get(fid, 0.0) + qty
            vehicles.loc[vehicles["Vehicle_ID"] == vid, "Trips_Used"] += 1

        # 3e) Record end-of-day stocks
        for lgid, st in lg_stock.items():
            stock_rows.append({"Day": int(day), "Date": day_to_date[int(day)], "Entity_Type": "LG",  "Entity_ID": int(lgid), "Stock_Level_tons": float(st)})
        for fid, st in fps_stock.items():
            stock_rows.append({"Day": int(day), "Date": day_to_date[int(day)], "Entity_Type": "FPS", "Entity_ID": int(fid),  "Stock_Level_tons": float(st)})

    # Build DataFrames with **expected schema**
    dispatch_lg = pd.DataFrame(dispatch_lg_rows, columns=["Day","Date","Vehicle_ID","LG_ID","FPS_ID","Quantity_tons","AAY_tons","PHH_tons","APL_tons","NFSA_tons"])
    stock_levels = pd.DataFrame(stock_rows, columns=["Day","Date","Entity_Type","Entity_ID","Stock_Level_tons"])

    # Ensure required columns exist even if empty (prevents KeyError later)
    if dispatch_lg.empty:
        dispatch_lg = pd.DataFrame(columns=["Day","Date","Vehicle_ID","LG_ID","FPS_ID","Quantity_tons","AAY_tons","PHH_tons","APL_tons","NFSA_tons"])

    # -----------------------------------------------
    # 4) Derive LG daily requirement from dispatch_lg
    # -----------------------------------------------
    required_cols = {"LG_ID", "Day", "Quantity_tons"}
    missing = required_cols - set(dispatch_lg.columns)
    if missing:
        raise ValueError(f"dispatch_lg is missing required columns: {missing}")

    if dispatch_lg.empty:
        # If nothing was dispatched, there is no derived requirement.
        # To avoid crashing, create an all-zero requirement for the known LGs.
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
    # 5) CG → LG PRE-DISPATCH (same DAYS timeline)
    # -----------------------------------------------
    # Capacity (prefer LG_Capacity sheet; fallback to LGs.Storage_Capacity_tons)
    try:
        cap_df = pd.read_excel(master_workbook, sheet_name="LG_Capacity")
        if {"LG_ID", "Capacity_tons"} <= set(cap_df.columns):
            capacity = {int(r["LG_ID"]): float(r["Capacity_tons"]) for _, r in cap_df.iterrows()}
        else:
            raise ValueError
    except Exception:
        if "Storage_Capacity_tons" not in lgs.columns:
            raise ValueError("Provide LG_Capacity sheet or 'Storage_Capacity_tons' in LGs.")
        capacity = {int(r["LG_ID"]): float(r["Storage_Capacity_tons"]) for _, r in lgs.iterrows()}

    # Initial stock (optional column)
    lg_stock_base = {int(r["LG_ID"]): float(r.get("Initial_LG_stock", 0.0)) for _, r in lgs.iterrows()}

    # Ensure req_pivot uses int LG_ID index and int Day columns
    req_pivot = req_pivot.copy()
    req_pivot.index = [int(x) for x in req_pivot.index]
    req_pivot.columns = [int(c) for c in req_pivot.columns]

    lg_ids = list(req_pivot.index)

    def _get_demand(lg_id: int, day: int) -> float:
        try:
            return float(req_pivot.at[lg_id, day])
        except Exception:
            return 0.0

    def _free_room(stock: dict, lg_id: int) -> float:
        return max(0.0, capacity.get(lg_id, 0.0) - stock.get(lg_id, 0.0))

    def _simulate(pre_days: int, collect_rows: bool = False, include_pre_days: bool = False):
        """
        Runs the CG→LG simulation starting at day = 1 - pre_days.
        - On days < 1: no consumption (pure pre-stocking).
        - On days >= 1: (A) cover today's need, (B) pre-stock with remaining trips, (C) consume.
        If collect_rows=True, records trips for:
            - all days if include_pre_days=True (including 0, -1, -2, ...)
            - only Day >= 1 if include_pre_days=False
        Returns: (feasible: bool, rows: list[dict], start_day: int, final_stock: dict)
        """
        start_day = 1 - pre_days
        stock = {lg: lg_stock_base.get(lg, 0.0) for lg in lg_ids}
        rows = [] if collect_rows else None

        for day in range(start_day, DAYS + 1):
            trips_left = TOT_V

            # --- A) Serve today's demand first (only matters when day >= 1) ---
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

                            # Safely get date for this integer day from day_to_date
                            row_date = day_to_date.get(int(day))
                            if row_date is None:
                                # fallback: use start_date if mapping missing (should not happen with prepared mapping)
                                row_date = start_date.date()

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

            # --- B) Pre-stock round-robin with remaining trips ---
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

                            row_date = day_to_date.get(int(day))
                            if row_date is None:
                                row_date = start_date.date()

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

            # --- C) End-of-day consumption (only Day >= 1) ---
            if day >= 1:
                for lg in lg_ids:
                    stock[lg] = max(0.0, stock[lg] - _get_demand(lg, day))

        return True, (rows or []), start_day, stock

    # --- Find minimal pre_days (0..MAX_PRE_DAYS) that makes schedule feasible ---
    pre_days = None
    for x in range(0, MAX_PRE_DAYS + 1):
        ok, _, start_day, _ = _simulate(pre_days=x, collect_rows=False)
        if ok:
            pre_days = x
            break

    if pre_days is None:
        raise RuntimeError("Unable to meet all demands within MAX_PRE_DAYS.")

    # --- Re-run with logging; include pre-days in output ---
    ok, rows, start_day, _ = _simulate(pre_days=pre_days, collect_rows=True, include_pre_days=True)
    assert ok

    dispatch_cg = pd.DataFrame(rows, columns=["Day", "Date", "Vehicle_ID", "LG_ID", "Quantity_tons"])

    # === Accurate LG stock levels: init + cumulative(CG→LG incl. pre-days) − cumulative(LG→FPS) ===
    # Robust to empty dfs + dtype mismatches; does not rely on req_pivot.

    # LG universe & init (INT-aligned)
    lg_ids_sorted = sorted(int(x) for x in lgs["LG_ID"].dropna().astype(int).unique())

    if "Initial_LG_stock" in lgs.columns:
        init_series = (
            lgs.assign(LG_ID=lgs["LG_ID"].astype(int))
               .set_index("LG_ID")["Initial_LG_stock"]
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

    # LG→FPS dispatch cumulative (compute directly from dispatch_lg; do NOT rely on req_pivot)
    if not dispatch_lg.empty:
        dlg = dispatch_lg.copy()
        dlg["LG_ID"] = dlg["LG_ID"].astype(int)
        dlg["Day"]   = dlg["Day"].astype(int)
        lg_piv = dlg.pivot_table(index="LG_ID", columns="Day",
                                 values="Quantity_tons", aggfunc="sum", fill_value=0.0)
        lg_piv = lg_piv.reindex(index=lg_ids_sorted, columns=list(range(1, DAYS + 1)), fill_value=0.0)
        lg_cum = lg_piv.cumsum(axis=1)
    else:
        lg_cum = pd.DataFrame(0.0, index=lg_ids_sorted, columns=list(range(1, DAYS + 1)))

    # Stock = init + CG_cum − LG_cum  (eps clamp kills float fuzz)
    stock_matrix = init_series.to_numpy()[:, None] + cg_cum.to_numpy() - lg_cum.to_numpy()
    stock_matrix = np.where(np.abs(stock_matrix) < 1e-9, 0.0, stock_matrix)

    # Tidy → LG rows; keep FPS rows intact
    lg_stock_levels = (
        pd.DataFrame(stock_matrix, index=lg_ids_sorted, columns=list(range(1, DAYS + 1)))
          .stack().rename("Stock_Level_tons")
          .rename_axis(index=["LG_ID", "Day"]).reset_index()
          .rename(columns={"LG_ID": "Entity_ID"})
          .assign(Entity_Type="LG")[["Day", "Entity_Type", "Entity_ID", "Stock_Level_tons"]]
    )

    # --- ALSO include LG stock levels for pre-days (start_day..0) with zero consumption/dispatch ---
    pre_cols = list(range(start_day, 1))  # includes negatives and 0; empty if start_day >= 1
    if pre_cols:
        if not dispatch_cg.empty:
            # Reuse cg_piv if available; else build a zero frame sized LG x pre_cols
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

    # Append pre-day LG stocks to the main LG stock rows
    lg_stock_levels = pd.concat([lg_stock_levels_pre, lg_stock_levels], ignore_index=True)
    stock_levels = pd.concat(
        [stock_levels[stock_levels["Entity_Type"] == "FPS"], lg_stock_levels],
        ignore_index=True
    )

    return dispatch_cg, dispatch_lg, stock_levels
