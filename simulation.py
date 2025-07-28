import pandas as pd
import math

def run_simulation(
    master_workbook,          # path string or file buffer
    settings: pd.DataFrame,
    lgs: pd.DataFrame,
    fps: pd.DataFrame,
    vehicles: pd.DataFrame
):
    """
    Reads all required sheets from `master_workbook`:
      • LG_Daily_Req  (if present)
      • LG_Capacity   (if present)
    Falls back to dispatch data or lgs dataframe if missing.
    Returns three DataFrames: dispatch_cg, dispatch_lg, stock_levels.
    """

    # 1) Read LG daily requirement
    try:
        lg_req = pd.read_excel(master_workbook, sheet_name="LG_Daily_Req").fillna(0)
    except ValueError:
        # fallback: derive from LG→FPS dispatch if that sheet exists
        tmp = pd.read_excel(master_workbook, sheet_name="LG_to_FPS_Dispatch")
        lg_req = (
            tmp.groupby(["LG_ID","Day"])["Quantity_tons"]
               .sum().reset_index()
               .rename(columns={"Quantity_tons":"Daily_Requirement_tons"})
        )

    # pivot for quick lookup
    req_pivot = lg_req.pivot_table(
        index="LG_ID", columns="Day",
        values="Daily_Requirement_tons", aggfunc="sum", fill_value=0
    )

    # 2) Read LG capacity
    try:
        cap_df = pd.read_excel(master_workbook, sheet_name="LG_Capacity")
        capacity = dict(zip(cap_df["LG_ID"], cap_df["Capacity_tons"]))
    except ValueError:
        capacity = dict(zip(
            lgs["LG_ID"], lgs["Storage_Capacity_tons"]
        ))

    # 3) Simulation config from settings DF
    DAYS       = int(settings.query("Parameter=='Distribution_Days'")["Value"].iloc[0])
    TRUCK_CAP  = float(settings.query("Parameter=='Vehicle_Capacity_tons'")["Value"].iloc[0])
    TOTAL_V    = int(settings.query("Parameter=='Vehicles_Total'")["Value"].iloc[0])
    MAX_TRIP   = int(settings.query("Parameter=='Max_Trips_Per_Vehicle_Per_Day'")["Value"].iloc[0])
    NUM_VEH    = TOTAL_V
    VEH_CAP    = TRUCK_CAP

    # 4) Initialize stocks and records
    lg_stock  = {lg_id: alloc for lg_id, alloc in zip(
                    lgs["LG_ID"], lgs["Initial_Allocation_tons"])}
    fps_stock = {fid: 0.0 for fid in fps["FPS_ID"]}

    dispatch_records = []
    stock_records    = []

    # 5) Helper
    def free_room(lg_id):
        return max(0.0, capacity.get(lg_id, 0.0) - lg_stock.get(lg_id, 0.0))

    # 6) Loop days 1…DAYS
    for day in range(1, DAYS+1):
        # FPS consumption
        for _, r in fps.iterrows():
            fid = r["FPS_ID"]
            daily = (r["Monthly_Demand_tons"]/30.0)
            fps_stock[fid] = max(0.0, fps_stock[fid] - daily)

        # build needs list
        needs = []
        for _, r in fps.iterrows():
            fid = r["FPS_ID"]; lgid = r["Linked_LG_ID"]
            curr = fps_stock[fid]
            thresh = (r["Monthly_Demand_tons"]/30.0) * r.get("Lead_Time_days",
                       settings.query("Parameter=='Default_Lead_Time_days'")["Value"].iloc[0])
            maxcap = r["Max_Capacity_tons"]
            if curr <= thresh:
                avail = lg_stock.get(lgid, 0.0)
                qty_need = min(maxcap-curr, avail)
                if qty_need>0:
                    urgency = (thresh-curr)/(r["Monthly_Demand_tons"]/30.0)
                    needs.append((urgency, fid, lgid, qty_need))
        needs.sort(reverse=True, key=lambda x: x[0])

        # reset vehicle usage
        vehicles["Trips_Used"] = 0

        # dispatch loop
        for urg, fid, lgid, need_qty in needs:
            # find vehicles that map to this LG and haven't maxed trips
            cand = vehicles[vehicles["Mapped_LG_IDs"]
                          .str.split(",")
                          .apply(lambda lst: str(lgid) in lst)]
            cand = cand[cand["Trips_Used"] < MAX_TRIP]
            if cand.empty:
                continue
            # prefer shared
            shared = cand[cand["Mapped_LG_IDs"].str.contains(",")]
            chosen = shared.iloc[0] if not shared.empty else cand.iloc[0]
            vid = chosen["Vehicle_ID"]
            cap = chosen["Capacity_tons"]
            qty = min(cap, need_qty, lg_stock.get(lgid,0.0))
            if qty<=0: 
                continue

            dispatch_records.append({
                "Day": day,
                "Vehicle_ID": vid,
                "LG_ID": lgid,
                "FPS_ID": fid,
                "Quantity_tons": qty
            })
            lg_stock[lgid] -= qty
            fps_stock[fid] += qty
            vehicles.loc[vehicles["Vehicle_ID"]==vid,"Trips_Used"] += 1

        # end-of-day stock record
        for lgid, st in lg_stock.items():
            stock_records.append({
                "Day": day,
                "Entity_Type":"LG",
                "Entity_ID": lgid,
                "Stock_Level_tons": st
            })
        for fid, st in fps_stock.items():
            stock_records.append({
                "Day": day,
                "Entity_Type":"FPS",
                "Entity_ID": fid,
                "Stock_Level_tons": st
            })

    # build DataFrames
    dispatch_cg = pd.DataFrame(dispatch_records)
    dispatch_lg = dispatch_cg.copy()  # if your merged algo separates, adjust here
    stock_levels= pd.DataFrame(stock_records)

    return dispatch_cg, dispatch_lg, stock_levels
