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

                        if collect_r_
