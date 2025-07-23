# simulation.py

import pandas as pd
import math

DEFAULT_FILE      = "distribution_dashboard_template.xlsx"

# CG → LG pre‑dispatch parameters
NUM_CG_VEHICLES   = 30
CG_VEHICLE_CAP    = 11.5
CG_TOTAL_DAYS     = 30
CG_MAX_PRE_DAYS   = 30

def run_simulation(settings: pd.DataFrame,
                   lgs: pd.DataFrame,
                   fps: pd.DataFrame,
                   vehicles: pd.DataFrame):
    """
    Runs:
      1) CG → LG pre‑dispatch based on LG_Daily_Req & LG_Capacity
      2) LG → FPS dynamic dispatch based on FPS, settings, and vehicle mappings

    Returns:
      dispatch_cg_df    : DataFrame(Day, Vehicle_ID, LG_ID, Quantity_tons)
      dispatch_lg_df    : DataFrame(Day, Vehicle_ID, LG_ID, FPS_ID, Quantity_tons)
      stock_levels_df   : DataFrame(Day, Entity_Type, Entity_ID, Stock_Level_tons)
    """

    # --- PHASE 1: CG → LG pre‑dispatch ---

    # 1a) Load daily LG requirements (fallback to LG_to_FPS_Dispatch if missing)
    try:
        req = pd.read_excel(DEFAULT_FILE, sheet_name="LG_Daily_Req").fillna(0)
    except ValueError:
        tmp = pd.read_excel(DEFAULT_FILE, sheet_name="LG_to_FPS_Dispatch")
        req = (
            tmp.groupby(["LG_ID","Day"])["Quantity_tons"]
            .sum().reset_index()
            .rename(columns={"Quantity_tons":"Daily_Requirement_tons"})
        )

    # 1b) Load LG capacities (fallback to lgs["Storage_Capacity_tons"])
    try:
        cap_df = pd.read_excel(DEFAULT_FILE, sheet_name="LG_Capacity")
        capacity = dict(zip(cap_df["LG_ID"], cap_df["Capacity_tons"]))
    except ValueError:
        capacity = dict(zip(lgs["LG_ID"], lgs["Storage_Capacity_tons"]))

    # Pivot for quick lookup
    req_pivot = req.pivot_table(
        index="LG_ID", columns="Day",
        values="Daily_Requirement_tons",
        aggfunc="sum", fill_value=0
    )

    def free_room(stock, lg_id):
        return max(0.0, capacity[lg_id] - stock[lg_id])

    # Check feasibility & find minimal pre‑days
    def can_meet_all(pre_days):
        stock = {lg: 0.0 for lg in req_pivot.index}
        start = 1 - pre_days

        for day in range(start, CG_TOTAL_DAYS + 1):
            trips = NUM_CG_VEHICLES

            # Serve today's unmet need
            if day >= 1:
                for lg in stock:
                    need = max(0.0, req_pivot.at[lg, day] - stock[lg])
                    room = free_room(stock, lg)
                    deliver = min(need, room)
                    used = min(trips, math.ceil(deliver / CG_VEHICLE_CAP))
                    stock[lg] += used * CG_VEHICLE_CAP
                    trips -= used
                    if stock[lg] + 1e-6 < req_pivot.at[lg, day]:
                        return False

            # Pre‑stock round‑robin
            if trips > 0:
                future_unmet = {
                    lg: max(
                        0.0,
                        sum(req_pivot.at[lg, d]
                            for d in range(max(1, day+1), CG_TOTAL_DAYS+1))
                        - stock[lg]
                    )
                    for lg in stock
                }
                candidates = [
                    lg for lg, fu in future_unmet.items()
                    if fu > 1e-6 and free_room(stock, lg) > 1e-6
                ]
                idx = 0
                while trips > 0 and candidates:
                    lg = candidates[idx % len(candidates)]
                    room = free_room(stock, lg)
                    deliver = min(CG_VEHICLE_CAP, future_unmet[lg], room)
                    if deliver > 1e-6:
                        stock[lg] += CG_VEHICLE_CAP
                        future_unmet[lg] = max(0.0, future_unmet[lg] - CG_VEHICLE_CAP)
                        trips -= 1
                    if future_unmet[lg] < 1e-6 or free_room(stock, lg) < 1e-6:
                        candidates.remove(lg)
                        idx -= 1
                    idx += 1

            # Consume today's demand
            if day >= 1:
                for lg in stock:
                    stock[lg] = max(0.0, stock[lg] - req_pivot.at[lg, day])

        return True

    # Find the minimal pre_days
    for x in range(CG_MAX_PRE_DAYS + 1):
        if can_meet_all(x):
            pre_days = x
            break
    else:
        raise RuntimeError("Cannot meet LG demand within allowed pre-days")

    # Build the CG→LG dispatch schedule
    start_day = 1 - pre_days
    stock     = {lg: 0.0 for lg in req_pivot.index}
    cg_records = []

    for day in range(start_day, CG_TOTAL_DAYS + 1):
        trips = NUM_CG_VEHICLES
        vids  = list(range(1, NUM_CG_VEHICLES + 1))

        # A) Serve Day ≥ 1
        if day >= 1:
            for lg in sorted(
                req_pivot.index,
                key=lambda l: -(req_pivot.at[l, day] - stock[l])
            ):
                need = max(0.0, req_pivot.at[lg, day] - stock[lg])
                room = free_room(stock, lg)
                deliver = min(need, room)
                while trips > 0 and deliver > 1e-6:
                    vid = vids.pop(0)
                    qty = min(deliver, CG_VEHICLE_CAP)
                    cg_records.append({
                        "Day": day,
                        "Vehicle_ID": vid,
                        "LG_ID": lg,
                        "Quantity_tons": qty
                    })
                    stock[lg] += qty
                    trips -= 1
                    deliver -= qty
                if trips == 0:
                    break

        # B) Pre‑stock round‑robin
        if trips > 0:
            future_unmet = {
                lg: max(
                    0.0,
                    sum(req_pivot.at[lg, d]
                        for d in range(max(1, day+1), CG_TOTAL_DAYS+1))
                    - stock[lg]
                )
                for lg in stock
            }
            candidates = [
                lg for lg, fu in future_unmet.items()
                if fu > 1e-6 and free_room(stock, lg) > 1e-6
            ]
            idx = 0
            while trips > 0 and candidates:
                lg = candidates[idx % len(candidates)]
                room = free_room(stock, lg)
                deliver = min(CG_VEHICLE_CAP, future_unmet[lg], room)
                if deliver > 1e-6:
                    vid = vids.pop(0)
                    qty = min(deliver, CG_VEHICLE_CAP)
                    cg_records.append({
                        "Day": day,
                        "Vehicle_ID": vid,
                        "LG_ID": lg,
                        "Quantity_tons": qty
                    })
                    stock[lg] += qty
                    trips -= 1
                    future_unmet[lg] -= qty
                if future_unmet[lg] < 1e-6 or free_room(stock, lg) < 1e-6:
                    candidates.remove(lg)
                    idx -= 1
                idx += 1

        # C) Consume Day’s demand
        if day >= 1:
            for lg in stock:
                stock[lg] = max(0.0, stock[lg] - req_pivot.at[lg, day])

    dispatch_cg_df = pd.DataFrame(cg_records)

    # --- PHASE 2: LG → FPS dynamic dispatch ---

    fps2 = fps.copy()
    fps2["Daily_Demand_tons"]      = fps2["Monthly_Demand_tons"] / 30.0
    default_lead = float(
        settings.query("Parameter=='Default_Lead_Time_days'")["Value"].iloc[0]
    )
    fps2["Lead_Time_days"]         = fps2["Lead_Time_days"].fillna(default_lead)
    fps2["Reorder_Threshold_tons"] = fps2["Daily_Demand_tons"] * fps2["Lead_Time_days"]

    # Map FPS → numeric LG_ID if needed
    if "Linked_LG_ID" in fps2.columns:
        mp = {n.strip().lower(): i for n, i in zip(lgs["LG_Name"], lgs["LG_ID"])}
        fps2["LG_ID"] = fps2["Linked_LG_ID"].str.lower().map(mp)

    # Initialize LG & FPS stocks
    lg_stock2  = dict(zip(lgs["LG_ID"], lgs["Initial_Allocation_tons"]))
    fps_stock2 = {fid: 0.0 for fid in fps2["FPS_ID"]}

    trips_per = int(
        settings.query("Parameter=='Max_Trips_Per_Vehicle_Per_Day'")["Value"].iloc[0]
    )

    # Prepare vehicle mappings
    veh = vehicles.copy()
    if "Mapped_LG_IDs" in veh.columns:
        veh["Mapped_LGs_List"] = veh["Mapped_LG_IDs"].apply(
            lambda s: [int(x) for x in str(s).split(",") if x.strip().isdigit()]
        )
    else:
        veh["Mapped_LGs_List"] = [list(lgs["LG_ID"]) for _ in veh.index]
    veh["Capacity"] = veh.get("Capacity_tons", CG_VEHICLE_CAP).fillna(CG_VEHICLE_CAP)

    lgp_records = []
    stock_records = []

    for day in range(1, CG_TOTAL_DAYS + 1):
        # 1) Consume FPS daily demand
        for _, r in fps2.iterrows():
            fid = r["FPS_ID"]
            fps_stock2[fid] = max(0.0, fps_stock2[fid] - r["Daily_Demand_tons"])

        # 2) Compute reorder needs
        needs = []
        for _, r in fps2.iterrows():
            fid, lgid = r["FPS_ID"], int(r["LG_ID"])
            cur, thr  = fps_stock2[fid], r["Reorder_Threshold_tons"]
            if cur <= thr:
                avail = lg_stock2.get(lgid, 0.0)
                space = r["Max_Capacity_tons"] - cur
                qty   = min(avail, space)
                if qty > 0:
                    urg = (thr - cur) / r["Daily_Demand_tons"]
                    needs.append((urg, fid, lgid, qty))

        needs.sort(reverse=True, key=lambda x: x[0])
        veh["Trips_Used"] = 0

        # 3) Dispatch to FPS
        for _, fid, lgid, need in needs:
            cand = veh[veh["Mapped_LGs_List"].apply(lambda lst: lgid in lst)]
            cand = cand[cand["Trips_Used"] < trips_per]
            if cand.empty:
                continue
            shared = cand[cand["Mapped_LGs_List"].apply(len) > 1]
            truck  = shared.iloc[0] if not shared.empty else cand.iloc[0]
            vid, capv = truck["Vehicle_ID"], truck["Capacity"]
            send = min(capv, need, lg_stock2[lgid])
            if send <= 0:
                continue

            lgp_records.append({
                "Day": day,
                "Vehicle_ID": vid,
                "LG_ID": lgid,
                "FPS_ID": fid,
                "Quantity_tons": send
            })
            lg_stock2[lgid]  -= send
            fps_stock2[fid]  += send
            veh.loc[veh.Vehicle_ID == vid, "Trips_Used"] += 1

        # 4) Record end‑of‑day stocks
        for lgid, st in lg_stock2.items():
            stock_records.append({
                "Day": day,
                "Entity_Type": "LG",
                "Entity_ID": lgid,
                "Stock_Level_tons": st
            })
        for fid, st in fps_stock2.items():
            stock_records.append({
                "Day": day,
                "Entity_Type": "FPS",
                "Entity_ID": fid,
                "Stock_Level_tons": st
            })

    dispatch_lg_df  = pd.DataFrame(lgp_records)
    stock_levels_df = pd.DataFrame(stock_records)

    return dispatch_cg_df, dispatch_lg_df, stock_levels_df
