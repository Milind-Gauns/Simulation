# simulation.py
import pandas as pd
import numpy as np

def run_simulation(
    master_workbook,          # str path or file-like buffer (passed through to read optional sheets)
    settings: pd.DataFrame,
    lgs: pd.DataFrame,
    fps: pd.DataFrame,
    vehicles: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Two-phase simulation (LG->FPS then CG->LG)
    Produces dispatch_cg, dispatch_lg, stock_levels.
    Adds Date column (datetime.date) mapped from Day using Start_Date (skipping Sundays).
    """

    # -----------------------------
    # Helpers: read settings safely
    # -----------------------------
    def _get_setting(param_name, default=None, cast=float):
        try:
            val = settings.loc[settings["Parameter"] == param_name, "Value"].iloc[0]
            return cast(val)
        except Exception:
            if default is None:
                raise ValueError(f"Missing required setting: {param_name}")
            return cast(default)

    DAYS       = int(_get_setting("Distribution_Days", default=30, cast=int))
    TRUCK_CAP  = float(_get_setting("Vehicle_Capacity_tons", default=11.5, cast=float))
    TOT_V      = int(_get_setting("Vehicles_Total", default=30, cast=int))
    MAX_TRIPS  = int(_get_setting("Max_Trips_Per_Vehicle_Per_Day", default=3, cast=int))
    DEFAULT_LEAD = float(_get_setting("Default_Lead_Time_days", default=3, cast=float))

    # -----------------------------
    # Build day -> date mapping (exclude Sundays)
    # -----------------------------
    # Optional Start_Date in settings (YYYY-MM-DD). If absent/invalid -> today.
    start_date_val = None
    try:
        start_date_val = settings.loc[settings["Parameter"] == "Start_Date", "Value"].iloc[0]
    except Exception:
        start_date_val = None

    if pd.notna(start_date_val):
        try:
            start_dt = pd.to_datetime(start_date_val).normalize()
        except Exception:
            start_dt = pd.Timestamp.today().normalize()
    else:
        start_dt = pd.Timestamp.today().normalize()

    # If the start date falls on Sunday, shift to next non-Sunday
    while start_dt.weekday() == 6:  # Sunday == 6
        start_dt += pd.Timedelta(days=1)

    # forward_dates: map Day=1..DAYS to calendar date excluding Sundays
    forward_dates = []
    cur = start_dt
    while len(forward_dates) < DAYS:
        if cur.weekday() != 6:  # skip Sunday
            forward_dates.append(cur.date())
        cur += pd.Timedelta(days=1)

    # also prepare pre-days (0, -1, -2, ...) up to a reasonable MAX_PRE_DAYS (30)
    MAX_PRE_DAYS = 30
    backward_dates = []
    cur = start_dt - pd.Timedelta(days=1)
    while len(backward_dates) < MAX_PRE_DAYS:
        if cur.weekday() != 6:
            backward_dates.append(cur.date())  # backward_dates[0] is day 0
        cur -= pd.Timedelta(days=1)

    # assemble day_to_date dict: Day -> date (integers -> datetime.date)
    day_to_date = {}
    for i, dt in enumerate(forward_dates, start=1):
        day_to_date[i] = dt
    for idx, dt in enumerate(backward_dates):
        day_to_date[0 - idx] = dt

    # -----------------------------
    # Prepare LG & FPS as before
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

    fps = fps.copy()
    # ensure Lead_Time_days exists:
    if "Lead_Time_days" not in fps.columns:
        fps["Lead_Time_days"] = DEFAULT_LEAD
    else:
        fps["Lead_Time_days"] = fps["Lead_Time_days"].fillna(DEFAULT_LEAD)

    # If Monthly_Demand_tons present, coerce numeric; if absent we'll derive later in your next change.
    if "Monthly_Demand_tons" in fps.columns:
        fps["Monthly_Demand_tons"] = pd.to_numeric(fps["Monthly_Demand_tons"], errors="coerce").fillna(0.0)
    else:
        fps["Monthly_Demand_tons"] = 0.0

    fps["Daily_Demand_tons"] = fps["Monthly_Demand_tons"] / 30.0
    fps["Reorder_Threshold_tons"] = fps["Daily_Demand_tons"] * fps["Lead_Time_days"]

    fps["LG_ID"] = fps.get("Linked_LG_ID", pd.Series([None]*len(fps))).apply(normalize_lg_ref)
    if fps["LG_ID"].isna().any():
        bad_rows = fps[fps["LG_ID"].isna()][["FPS_ID", "Linked_LG_ID"]] if "Linked_LG_ID" in fps.columns else fps[fps["LG_ID"].isna()][["FPS_ID"]]
        raise ValueError(
            "Some FPS rows couldn't map Linked_LG_ID to a valid LG_ID. "
            f"Examples:\n{bad_rows.head(5).to_string(index=False)}"
        )
    fps["LG_ID"] = fps["LG_ID"].astype(int)

    # -----------------------------
    # Vehicles mapping (unchanged)
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
    # LG -> FPS simulation (unchanged quantities)
    # but ensure we populate a Date column for each dispatch & stock row
    # -----------------------------
    if "Initial_Allocation_tons" not in lgs.columns:
        lgs["Initial_Allocation_tons"] = 0.0

    lg_stock = {int(row["LG_ID"]): float(row["Initial_Allocation_tons"]) for _, row in lgs.iterrows()}
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

            # safe date lookup
            row_date = day_to_date.get(int(day), start_dt.date())

            dispatch_lg_rows.append({
                "Day": int(day),
                "Date": row_date,
                "Vehicle_ID": vid,
                "LG_ID": int(lgid),
                "FPS_ID": int(fid),
                "Quantity_tons": float(qty)
            })

            lg_stock[lgid] = lg_stock.get(lgid, 0.0) - qty
            fps_stock[fid] = fps_stock.get(fid, 0.0) + qty
            vehicles.loc[vehicles["Vehicle_ID"] == vid, "Trips_Used"] += 1

        # record end-of-day stocks (with Date)
        for lgid, st in lg_stock.items():
            stock_rows.append({
                "Day": int(day),
                "Date": day_to_date.get(int(day), start_dt.date()),
                "Entity_Type": "LG",
                "Entity_ID": int(lgid),
                "Stock_Level_tons": float(st)
            })
        for fid, st in fps_stock.items():
            stock_rows.append({
                "Day": int(day),
                "Date": day_to_date.get(int(day), start_dt.date()),
                "Entity_Type": "FPS",
                "Entity_ID": int(fid),
                "Stock_Level_tons": float(st)
            })

    dispatch_lg = pd.DataFrame(dispatch_lg_rows, columns=[
        "Day", "Date", "Vehicle_ID", "LG_ID", "FPS_ID", "Quantity_tons"
    ])
    stock_levels = pd.DataFrame(stock_rows, columns=["Day", "Date", "Entity_Type", "Entity_ID", "Stock_Level_tons"])

    if dispatch_lg.empty:
        dispatch_lg = pd.DataFrame(columns=["Day", "Date", "Vehicle_ID", "LG_ID", "FPS_ID", "Quantity_tons"])

    # -----------------------------------------------
    # Derive LG daily requirement -> CG dispatch (unchanged logic)
    # but ensure dispatch_cg also has Date column
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

    # CG->LG pre-dispatch (the code here is intentionally the same as your previous logic
    # so I won't repeat long blocks; assume it computes 'dispatch_cg_rows' similarly).
    # For brevity keep the same algorithm as before but ensure we attach Date when adding rows.
    # (Below is a faithful compact reimplementation producing dispatch_cg_rows.)
    # --- simple feasibility-based prefill simulation (keeps logic identical) ---

    # Build capacity mapping (prefer LG_Capacity; fallback to Storage_Capacity_tons)
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

    lg_stock_base = {int(r["LG_ID"]): float(r.get("Initial_LG_stock", 0.0)) for _, r in lgs.iterrows()}

    # Align types for req_pivot
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
        start_day = 1 - pre_days
        stock = {lg: lg_stock_base.get(lg, 0.0) for lg in lg_ids}
        rows = [] if collect_rows else None

        for day in range(start_day, DAYS + 1):
            trips_left = TOT_V

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
                            row_date = day_to_date.get(int(day), start_dt.date())
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

            # Pre-stock round-robin
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
                            row_date = day_to_date.get(int(day), start_dt.date())
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

            if day >= 1:
                for lg in lg_ids:
                    stock[lg] = max(0.0, stock[lg] - _get_demand(lg, day))

        return True, (rows or []), start_day, stock

    pre_days = None
    for x in range(0, MAX_PRE_DAYS + 1):
        ok, _, start_day, _ = _simulate(pre_days=x, collect_rows=False)
        if ok:
            pre_days = x
            break

    if pre_days is None:
        raise RuntimeError("Unable to meet all demands within MAX_PRE_DAYS.")

    ok, rows, start_day, _ = _simulate(pre_days=pre_days, collect_rows=True, include_pre_days=True)
    assert ok

    dispatch_cg = pd.DataFrame(rows, columns=["Day", "Date", "Vehicle_ID", "LG_ID", "Quantity_tons"])

    # Build LG stock levels: init + CG_cum - LG_cum
    lg_ids_sorted = sorted(int(x) for x in lgs["LG_ID"].dropna().astype(int).unique())

    if "Initial_LG_stock" in lgs.columns:
        init_series = (
            lgs.assign(LG_ID=lgs["LG_ID"].astype(int))
               .set_index("LG_ID")["Initial_LG_stock"]
               .reindex(lg_ids_sorted).fillna(0.0)
        )
    else:
        init_series = pd.Series(0.0, index=lg_ids_sorted)

    # CG receipts cumulative (include pre-days)
    if not dispatch_cg.empty:
        dcg = dispatch_cg.copy()
        dcg["LG_ID"] = dcg["LG_ID"].astype(int)
        dcg["Day"]   = dcg["Day"].astype(int)
        cg_piv = dcg.pivot_table(index="LG_ID", columns="Day",
                                 values="Quantity_tons", aggfunc="sum", fill_value=0.0)
        full_cols = list(range(start_day, DAYS + 1))
        cg_piv = cg_piv.reindex(index=lg_ids_sorted, columns=full_cols, fill_value=0.0)
        cg_cum = cg_piv.cumsum(axis=1).reindex(columns=list(range(1, DAYS + 1)), fill_value=0.0)
    else:
        cg_cum = pd.DataFrame(0.0, index=lg_ids_sorted, columns=list(range(1, DAYS + 1)))

    # LG->FPS cumulative
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

    stock_matrix = init_series.to_numpy()[:, None] + cg_cum.to_numpy() - lg_cum.to_numpy()
    stock_matrix = np.where(np.abs(stock_matrix) < 1e-9, 0.0, stock_matrix)

    lg_stock_levels = (
        pd.DataFrame(stock_matrix, index=lg_ids_sorted, columns=list(range(1, DAYS + 1)))
          .stack().rename("Stock_Level_tons")
          .rename_axis(index=["LG_ID", "Day"]).reset_index()
          .rename(columns={"LG_ID": "Entity_ID"})
          .assign(Entity_Type="LG")[["Day", "Entity_Type", "Entity_ID", "Stock_Level_tons"]]
    )

    # Add pre-day LG stocks
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

    # Attach Date to LG stock rows using the day_to_date mapping. Keep Day too.
    if not lg_stock_levels.empty:
        lg_stock_levels = lg_stock_levels.copy()
        lg_stock_levels["Date"] = lg_stock_levels["Day"].apply(lambda d: day_to_date.get(int(d), start_dt.date()))

    # Ensure FPS stock rows already had Date; if not, add from Day mapping:
    if 'Date' not in stock_levels.columns:
        stock_levels['Date'] = stock_levels['Day'].apply(lambda d: day_to_date.get(int(d), start_dt.date()))

    # final stock_levels: keep FPS rows (already include Date) + LG rows
    final_stock = pd.concat(
        [stock_levels[stock_levels["Entity_Type"] == "FPS"], lg_stock_levels],
        ignore_index=True
    )

    # Ensure dispatch_lg & dispatch_cg always contain a Date column (they already do, but just in case)
    if "Date" not in dispatch_lg.columns:
        dispatch_lg["Date"] = dispatch_lg["Day"].apply(lambda d: day_to_date.get(int(d), start_dt.date()))
    if "Date" not in dispatch_cg.columns:
        dispatch_cg["Date"] = dispatch_cg["Day"].apply(lambda d: day_to_date.get(int(d), start_dt.date()))

    return dispatch_cg, dispatch_lg, final_stock
