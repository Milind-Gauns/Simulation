# simulation.py

import pandas as pd
import math

def run_simulation(
    master_workbook,          # str path or file-like buffer
    settings: pd.DataFrame,
    lgs: pd.DataFrame,
    fps: pd.DataFrame,
    vehicles: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Two-phase simulation:
      1) LG → FPS dispatch (vehicle mapping + trip caps)
      2) CG → LG pre-dispatch based on derived LG daily requirement from (1)

    Returns: (dispatch_cg, dispatch_lg, stock_levels)
    """

    # 0) Settings helpers ------------------------------------------------------
    def _get_setting(param_name, default=None, cast=float):
        try:
            val = settings.loc[settings["Parameter"] == param_name, "Value"].iloc[0]
            return cast(val)
        except Exception:
            if default is None:
                raise ValueError(f"Missing required setting: {param_name}")
            return cast(default)

    DAYS         = _get_setting("Distribution_Days", cast=int)
    TRUCK_CAP    = _get_setting("Vehicle_Capacity_tons", cast=float)
    TOT_V        = _get_setting("Vehicles_Total", cast=int)
    MAX_TRIPS    = _get_setting("Max_Trips_Per_Vehicle_Per_Day", cast=int)
    DEFAULT_LEAD = _get_setting("Default_Lead_Time_days", cast=float)

    # 1) LG/FPS normalization --------------------------------------------------
    lgs = lgs.copy()
    if {"LG_ID","LG_Name"} - set(lgs.columns):
        raise ValueError("LGs sheet must contain columns: LG_ID, LG_Name")

    # Ensure numeric LG_ID
    lgs["LG_ID"] = lgs["LG_ID"].apply(lambda x: int(float(str(x).strip())) if pd.notna(x) and str(x).strip() != "" else None)
    if lgs["LG_ID"].isna().any():
        raise ValueError("Some LG_ID values are blank/invalid.")

    valid_lg_ids = set(lgs["LG_ID"].astype(int))
    lgid_by_name = {str(nm).strip().lower(): int(lg_id) for lg_id, nm in zip(lgs["LG_ID"], lgs["LG_Name"])}

    def normalize_lg_ref(val):
        """Accepts int-like or name; returns int LG_ID or None."""
        if pd.isna(val):
            return None
        s = str(val).strip()
        # ID?
        try:
            i = int(float(s))
            return i if i in valid_lg_ids else None
        except ValueError:
            pass
        # Name?
        return lgid_by_name.get(s.lower())

    fps = fps.copy()
    req_cols = {"FPS_ID","Monthly_Demand_tons","Max_Capacity_tons","Linked_LG_ID"}
    missing = req_cols - set(fps.columns)
    if missing:
        raise ValueError(f"FPS sheet missing required columns: {missing}")

    # FPS_ID numeric
    fps["FPS_ID"] = fps["FPS_ID"].apply(lambda x: int(float(str(x).strip())) if pd.notna(x) and str(x).strip() else None)
    if fps["FPS_ID"].isna().any():
        raise ValueError("Some FPS_ID values are blank/invalid.")

    # Lead time
    if "Lead_Time_days" not in fps.columns:
        fps["Lead_Time_days"] = DEFAULT_LEAD
    else:
        fps["Lead_Time_days"] = fps["Lead_Time_days"].fillna(DEFAULT_LEAD)

    fps["Daily_Demand_tons"]      = pd.to_numeric(fps["Monthly_Demand_tons"], errors="coerce").fillna(0.0) / 30.0
    fps["Reorder_Threshold_tons"] = fps["Daily_Demand_tons"] * pd.to_numeric(fps["Lead_Time_days"], errors="coerce").fillna(DEFAULT_LEAD)

    # Normalize LG_ID on FPS from Linked_LG_ID (ID or Name)
    fps["LG_ID"] = fps["Linked_LG_ID"].apply(normalize_lg_ref)
    if fps["LG_ID"].isna().any():
        bad = fps.loc[fps["LG_ID"].isna(), ["FPS_ID","Linked_LG_ID"]].head(5)
        raise ValueError(
            "Some FPS rows couldn't map Linked_LG_ID to a valid LG_ID.\n"
            f"Examples:\n{bad.to_string(index=False)}"
        )
    fps["LG_ID"] = fps["LG_ID"].astype(int)

    # 2) Vehicles normalization -----------------------------------------------
    vehicles = vehicles.copy()
    if vehicles.empty:
        # fallback: pool of generic vehicles mapped to all LGs
        vehicles = pd.DataFrame({
            "Vehicle_ID": list(range(1, TOT_V+1)),
            "Capacity_tons": [TRUCK_CAP] * TOT_V,
            "Mapped_LG_IDs": [",".join(map(str, sorted(valid_lg_ids)))] * TOT_V
        })
    else:
        if "Vehicle_ID" not in vehicles.columns:
            raise ValueError("Vehicles sheet must contain 'Vehicle_ID'")
        if "Capacity_tons" not in vehicles.columns:
            vehicles["Capacity_tons"] = TRUCK_CAP
        if "Mapped_LG_IDs" not in vehicles.columns:
            vehicles["Mapped_LG_IDs"] = ",".join(map(str, sorted(valid_lg_ids)))

    vehicles["Vehicle_ID"]   = vehicles["Vehicle_ID"].apply(lambda x: int(float(str(x).strip())))
    vehicles["Capacity_tons"] = pd.to_numeric(vehicles["Capacity_tons"], errors="coerce").fillna(TRUCK_CAP)

    def parse_lg_list(val):
        if pd.isna(val): return []
        out = []
        for tok in str(val).split(","):
            t = tok.strip()
            if not t: continue
            # try id
            try:
                i = int(float(t))
                if i in valid_lg_ids:
                    out.append(i); continue
            except ValueError:
                pass
            # try name
            m = normalize_lg_ref(t)
            if m is not None: out.append(m)
        return sorted(set(out))

    vehicles["Mapped_LGs_List"] = vehicles["Mapped_LG_IDs"].apply(parse_lg_list)
    if vehicles["Mapped_LGs_List"].apply(len).eq(0).any():
        bad = vehicles.loc[vehicles["Mapped_LGs_List"].apply(len).eq(0), ["Vehicle_ID","Mapped_LG_IDs"]].head(5)
        raise ValueError(
            "Some vehicles couldn't map any LGs from 'Mapped_LG_IDs'.\n"
            f"Examples:\n{bad.to_string(index=False)}"
        )

    # 3) LG → FPS simulation ---------------------------------------------------
    if "Initial_Allocation_tons" not in lgs.columns:
        lgs["Initial_Allocation_tons"] = 0.0
    lgs["Initial_Allocation_tons"] = pd.to_numeric(lgs["Initial_Allocation_tons"], errors="coerce").fillna(0.0)

    lg_stock  = {int(r["LG_ID"]): float(r["Initial_Allocation_tons"]) for _, r in lgs.iterrows()}
    fps_stock = {int(fid): 0.0 for fid in fps["FPS_ID"]}

    dispatch_lg_rows = []
    stock_rows       = []

    for day in range(1, DAYS + 1):
        # consume FPS demand
        for _, r in fps.iterrows():
            fid = int(r["FPS_ID"])
            fps_stock[fid] = max(0.0, fps_stock[fid] - float(r["Daily_Demand_tons"]))

        # compute needs
        needs = []
        for _, r in fps.iterrows():
            fid   = int(r["FPS_ID"])
            lgid  = int(r["LG_ID"])
            curr  = fps_stock[fid]
            thr   = float(r["Reorder_Threshold_tons"])
            maxc  = float(r["Max_Capacity_tons"])
            if curr <= thr:
                avail = lg_stock.get(lgid, 0.0)
                need  = min(maxc - curr, avail)
                if need > 0:
                    dd = float(r["Daily_Demand_tons"])
                    urg = (thr - curr) / dd if dd > 0 else 0.0
                    needs.append((urg, fid, lgid, need))
        needs.sort(reverse=True, key=lambda x: x[0])

        # reset trip counters
        vehicles["Trips_Used"] = 0

        # dispatch
        for urg, fid, lgid, need_qty in needs:
            cand = vehicles[vehicles["Mapped_LGs_List"].apply(lambda lst: lgid in lst)]
            cand = cand[cand["Trips_Used"] < MAX_TRIPS]
            if cand.empty: 
                continue
            cand = cand.assign(is_shared=cand["Mapped_LGs_List"].apply(lambda lst: len(lst) > 1)) \
                       .sort_values("is_shared", ascending=False)
            ch = cand.iloc[0]
            vid = int(ch["Vehicle_ID"])
            cap = float(ch["Capacity_tons"])
            qty = min(cap, need_qty, lg_stock.get(lgid, 0.0))
            if qty <= 0: 
                continue

            dispatch_lg_rows.append({
                "Day": int(day), "Vehicle_ID": vid, "LG_ID": int(lgid),
                "FPS_ID": int(fid), "Quantity_tons": float(qty)
            })
            lg_stock[lgid] = lg_stock.get(lgid, 0.0) - qty
            fps_stock[fid] = fps_stock.get(fid, 0.0) + qty
            vehicles.loc[vehicles["Vehicle_ID"] == vid, "Trips_Used"] += 1

        # record stocks
        for lgid, st in lg_stock.items():
            stock_rows.append({"Day": day, "Entity_Type": "LG",  "Entity_ID": int(lgid), "Stock_Level_tons": float(st)})
        for fid, st in fps_stock.items():
            stock_rows.append({"Day": day, "Entity_Type": "FPS", "Entity_ID": int(fid),  "Stock_Level_tons": float(st)})

    dispatch_lg  = pd.DataFrame(dispatch_lg_rows, columns=["Day","Vehicle_ID","LG_ID","FPS_ID","Quantity_tons"])
    stock_levels = pd.DataFrame(stock_rows,      columns=["Day","Entity_Type","Entity_ID","Stock_Level_tons"])

    # 4) Derive LG daily requirement (robust) ---------------------------------
    # Guarantee required columns exist even if empty
    if dispatch_lg.empty:
        # fabricate zero req across DAYS so CG→LG still has a frame to work with
        lg_daily_req = (
            pd.MultiIndex.from_product([sorted(valid_lg_ids), range(1, DAYS+1)], names=["LG_ID","Day"])
            .to_frame(index=False)
            .assign(Daily_Requirement_tons=0.0)
        )
    else:
        need_cols = {"LG_ID","Day","Quantity_tons"} - set(dispatch_lg.columns)
        if need_cols:
            raise ValueError(f"dispatch_lg missing columns: {need_cols}")
        # coerce types
        for c in ["LG_ID","Day"]:
            dispatch_lg[c] = pd.to_numeric(dispatch_lg[c], errors="coerce").astype("Int64")
        dispatch_lg["Quantity_tons"] = pd.to_numeric(dispatch_lg["Quantity_tons"], errors="coerce").fillna(0.0)
        # drop bad rows safely
        dispatch_lg = dispatch_lg.dropna(subset=["LG_ID","Day"]).copy()
        dispatch_lg["LG_ID"] = dispatch_lg["LG_ID"].astype(int)
        dispatch_lg["Day"]   = dispatch_lg["Day"].astype(int)

        lg_daily_req = (
            dispatch_lg
            .groupby(["LG_ID","Day"], as_index=False)["Quantity_tons"]
            .sum()
            .rename(columns={"Quantity_tons":"Daily_Requirement_tons"})
        )

    req_pivot = lg_daily_req.pivot_table(index="LG_ID", columns="Day",
                                         values="Daily_Requirement_tons",
                                         aggfunc="sum", fill_value=0.0)

    # 5) CG → LG PRE-DISPATCH --------------------------------------------------
    # Capacity source
    try:
        cap_df = pd.read_excel(master_workbook, sheet_name="LG_Capacity")
        if {"LG_ID","Capacity_tons"} <= set(cap_df.columns):
            capacity = {int(r["LG_ID"]): float(r["Capacity_tons"]) for _, r in cap_df.iterrows()}
        else:
            raise ValueError
    except Exception:
        if "Storage_Capacity_tons" not in lgs.columns:
            raise ValueError("Provide LG_Capacity sheet or 'Storage_Capacity_tons' in LGs.")
        capacity = {int(r["LG_ID"]): float(r["Storage_Capacity_tons"]) for _, r in lgs.iterrows()}

    # Initial LG stock for CG stage (optional column)
    init_col = "Initial_LG_Stock" if "Initial_LG_Stock" in lgs.columns else "Initial_Allocation_tons"
    lg_stock_cg = {int(r["LG_ID"]): float(pd.to_numeric(r.get(init_col, 0.0), errors="coerce")) for _, r in lgs.iterrows()}

    dispatch_cg_rows = []

    def free_room(lg_id: int) -> float:
        return max(0.0, capacity.get(lg_id, 0.0) - lg_stock_cg.get(lg_id, 0.0))

    for day in range(1, DAYS + 1):
        trips_left = TOT_V
        if day not in req_pivot.columns:
            # if missing a day column, treat as zero requirement that day
            continue

        # Serve today’s requirement for each LG
        for lgid in req_pivot.index:
            need_today = max(0.0, float(req_pivot.at[lgid, day]) - lg_stock_cg.get(lgid, 0.0))

            # Ship in trips as long as need and trips remain
            while trips_left > 0 and need_today > 1e-9:
                vid = TOT_V - trips_left + 1  # simple rotating id
                qty = min(TRUCK_CAP, need_today, free_room(lgid))
                if qty <= 1e-9:
                    break
                dispatch_cg_rows.append({
                    "Day": int(day),
                    "Vehicle_ID": int(vid),
                    "LG_ID": int(lgid),
                    "Quantity_tons": float(qty)
                })
                lg_stock_cg[lgid] = lg_stock_cg.get(lgid, 0.0) + qty
                trips_left -= 1
                need_today -= qty

        # (Optional) pre-stock future days if trips_left>0 — omitted for clarity.

    dispatch_cg = pd.DataFrame(dispatch_cg_rows, columns=["Day","Vehicle_ID","LG_ID","Quantity_tons"])

    return dispatch_cg, dispatch_lg, stock_levels
