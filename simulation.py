# simulation.py

import pandas as pd
import math

def run_simulation(
    master_workbook,          # str filename or file-like buffer
    settings: pd.DataFrame,
    lgs: pd.DataFrame,
    fps: pd.DataFrame,
    vehicles: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    1) LG→FPS dispatch
    2) derive LG daily req
    3) CG→LG pre‑dispatch
    4) return (dispatch_cg, dispatch_lg, stock_levels)
    """

    # ——————————————————————————————————————————————
    # 1) LG→FPS dispatch simulation
    # ——————————————————————————————————————————————
    DAYS      = int(settings.loc[settings.Parameter=="Distribution_Days","Value"].iloc[0])
    TRUCK_CAP = float(settings.loc[settings.Parameter=="Vehicle_Capacity_tons","Value"].iloc[0])
    TOT_V     = int(settings.loc[settings.Parameter=="Vehicles_Total","Value"].iloc[0])
    MAX_TRIPS = int(settings.loc[settings.Parameter=="Max_Trips_Per_Vehicle_Per_Day","Value"].iloc[0])

    # Fill defaults
    default_lead = float(settings.loc[settings.Parameter=="Default_Lead_Time_days","Value"].iloc[0])

    # Initialize stocks
    lg_stock = dict(zip(lgs["LG_ID"], lgs["Initial_Allocation_tons"]))
    fps_stock = {fid: 0.0 for fid in fps["FPS_ID"]}

    dispatch_lg_rows = []
    stock_levels_rows = []

    for day in range(1, DAYS+1):
        # 1a) FPS daily consumption
        for _, r in fps.iterrows():
            fid = r["FPS_ID"]
            daily_dem = r["Monthly_Demand_tons"] / 30.0
            fps_stock[fid] = max(0.0, fps_stock[fid] - daily_dem)

        # 1b) compute reorder thresholds & needs
        needs = []
        for _, r in fps.iterrows():
            fid, lgid = r["FPS_ID"], r["Linked_LG_ID"]
            curr = fps_stock[fid]
            lead = r["Lead_Time_days"] if not pd.isna(r["Lead_Time_days"]) else default_lead
            thresh = (r["Monthly_Demand_tons"]/30.0) * lead
            if curr <= thresh:
                maxcap = r["Max_Capacity_tons"]
                avail = lg_stock.get(lgid, 0.0)
                qty_need = min(maxcap-curr, avail)
                if qty_need > 0:
                    urgency = (thresh-curr) / (r["Monthly_Demand_tons"]/30.0)
                    needs.append((urgency, fid, lgid, qty_need))
        needs.sort(reverse=True, key=lambda x: x[0])

        # 1c) reset vehicle usage
        vehicles["Trips_Used"] = 0

        # 1d) dispatch loop
        for urg, fid, lgid, need_qty in needs:
            # select candidate vehicles
            cand = vehicles[vehicles["Mapped_LG_IDs"]
                            .str.split(",")
                            .apply(lambda lst: str(lgid) in lst)]
            cand = cand[cand["Trips_Used"] < MAX_TRIPS]
            if cand.empty:
                continue
            shared = cand[cand["Mapped_LG_IDs"].str.contains(",")]
            chosen = shared.iloc[0] if not shared.empty else cand.iloc[0]

            vid  = chosen["Vehicle_ID"]
            cap  = chosen["Capacity_tons"]
            qty  = min(cap, need_qty, lg_stock.get(lgid,0.0))
            if qty <= 0:
                continue

            dispatch_lg_rows.append({
                "Day": day,
                "Vehicle_ID": vid,
                "LG_ID": lgid,
                "FPS_ID": fid,
                "Quantity_tons": qty
            })
            lg_stock[lgid]  -= qty
            fps_stock[fid]  += qty
            vehicles.loc[vehicles.Vehicle_ID==vid, "Trips_Used"] += 1

        # 1e) record end‑of‑day stock levels for LG & FPS
        for lgid, st in lg_stock.items():
            stock_levels_rows.append({
                "Day": day,
                "Entity_Type": "LG",
                "Entity_ID": lgid,
                "Stock_Level_tons": st
            })
        for fid, st in fps_stock.items():
            stock_levels_rows.append({
                "Day": day,
                "Entity_Type": "FPS",
                "Entity_ID": fid,
                "Stock_Level_tons": st
            })

    dispatch_lg   = pd.DataFrame(dispatch_lg_rows)
    stock_levels  = pd.DataFrame(stock_levels_rows)

    # ——————————————————————————————————————————————
    # 2) Derive LG daily requirement from LG→FPS results
    # ——————————————————————————————————————————————
    lg_req = (
        dispatch_lg
        .groupby(["LG_ID","Day"])["Quantity_tons"]
        .sum()
        .reset_index()
        .rename(columns={"Quantity_tons":"Daily_Requirement_tons"})
    )
    req_pivot = lg_req.pivot_table(
        index="LG_ID", columns="Day",
        values="Daily_Requirement_tons",
        aggfunc="sum", fill_value=0
    )

    # ——————————————————————————————————————————————
    # 3) CG→LG pre‑dispatch simulation
    # ——————————————————————————————————————————————
    # read capacity sheet (fallback to lgs if missing)
    try:
        cap_df = pd.read_excel(master_workbook, sheet_name="LG_Capacity")
        capacity = dict(zip(cap_df["LG_ID"], cap_df["Capacity_tons"]))
    except ValueError:
        capacity = dict(zip(lgs["LG_ID"], lgs["Storage_Capacity_tons"]))

    dispatch_cg_rows = []
    lg_stock_cg = dict(zip(lgs["LG_ID"], lgs["Initial_Allocation_tons"]))

    def free_room(lg_id):
        return max(0.0, capacity.get(lg_id,0.0) - lg_stock_cg.get(lg_id,0.0))

    for day in range(1, DAYS+1):
        trips_left = TOT_V

        # 3a) serve today's requirement first
        for lgid in req_pivot.index:
            need = max(0.0, req_pivot.at[lgid, day] - lg_stock_cg.get(lgid,0.0))
            while need > 0 and trips_left > 0:
                vid = (TOT_V - trips_left) % TOT_V + 1
                qty = min(TRUCK_CAP, need, free_room(lgid))
                dispatch_cg_rows.append({
                    "Day": day,
                    "Vehicle_ID": vid,
                    "LG_ID": lgid,
                    "Quantity_tons": qty
                })
                lg_stock_cg[lgid] += qty
                need       -= qty
                trips_left -= 1

        # 3b) optional: pre‑stock for future days if trips_left>0
        #    (implement your round‑robin here)

    dispatch_cg = pd.DataFrame(dispatch_cg_rows)

    # ——————————————————————————————————————————————
    # 4) Return results
    # ——————————————————————————————————————————————
    return dispatch_cg, dispatch_lg, stock_levels
