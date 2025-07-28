import pandas as pd
import math

def run_simulation(
    master_workbook,          # path str or file‐like
    settings: pd.DataFrame,
    lgs: pd.DataFrame,
    fps: pd.DataFrame,
    vehicles: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Reads LG_Daily_Req & LG_Capacity (or falls back),
    runs the CG→LG and LG→FPS simulation,
    returns (dispatch_cg, dispatch_lg, stock_levels).
    """

    # 1) LG_Daily_Req
    try:
        lg_req = pd.read_excel(master_workbook, sheet_name="LG_Daily_Req").fillna(0)
    except ValueError:
        tmp = pd.read_excel(master_workbook, sheet_name="LG_to_FPS_Dispatch")
        lg_req = (
            tmp.groupby(["LG_ID","Day"])["Quantity_tons"]
               .sum().reset_index()
               .rename(columns={"Quantity_tons":"Daily_Requirement_tons"})
        )

    req_pivot = lg_req.pivot_table(
        index="LG_ID", columns="Day",
        values="Daily_Requirement_tons", aggfunc="sum", fill_value=0
    )

    # 2) LG_Capacity
    try:
        cap_df = pd.read_excel(master_workbook, sheet_name="LG_Capacity")
        capacity = dict(zip(cap_df["LG_ID"], cap_df["Capacity_tons"]))
    except ValueError:
        capacity = dict(zip(lgs["LG_ID"], lgs["Storage_Capacity_tons"]))

    # 3) Settings
    DAYS      = int(settings.loc[settings.Parameter=="Distribution_Days","Value"].iloc[0])
    TRUCK_CAP = float(settings.loc[settings.Parameter=="Vehicle_Capacity_tons","Value"].iloc[0])
    TOTAL_V   = int(settings.loc[settings.Parameter=="Vehicles_Total","Value"].iloc[0])
    MAX_TRIPS = int(settings.loc[settings.Parameter=="Max_Trips_Per_Vehicle_Per_Day","Value"].iloc[0])

    # 4) Init stocks & records
    lg_stock      = dict(zip(lgs["LG_ID"], lgs["Initial_Allocation_tons"]))
    fps_stock     = {fid: 0.0 for fid in fps["FPS_ID"]}
    dispatch_rows = []
    stock_rows    = []

    def free_room(lg_id):
        return max(0.0, capacity.get(lg_id,0.0) - lg_stock.get(lg_id,0.0))

    # 5) Simulate days
    for day in range(1, DAYS+1):
        # consumption
        for _, r in fps.iterrows():
            fid = r["FPS_ID"]
            daily = r["Monthly_Demand_tons"] / 30.0
            fps_stock[fid] = max(0.0, fps_stock[fid] - daily)

        # build needs
        needs = []
        for _, r in fps.iterrows():
            fid, lgid = r["FPS_ID"], r["Linked_LG_ID"]
            curr = fps_stock[fid]
            lead = r.get("Lead_Time_days", None)
            if pd.isna(lead):
                lead = float(settings.loc[settings.Parameter=="Default_Lead_Time_days","Value"].iloc[0])
            threshold = (r["Monthly_Demand_tons"]/30.0) * lead
            maxcap = r["Max_Capacity_tons"]
            if curr <= threshold:
                avail = lg_stock.get(lgid,0.0)
                need_qty = min(maxcap-curr, avail)
                if need_qty>0:
                    urgency = (threshold-curr)/(r["Monthly_Demand_tons"]/30.0)
                    needs.append((urgency, fid, lgid, need_qty))
        needs.sort(reverse=True, key=lambda x: x[0])

        # reset usage
        vehicles["Trips_Used"] = 0

        # dispatch
        for urgency, fid, lgid, need_qty in needs:
            cand = vehicles[
                vehicles["Mapped_LG_IDs"]
                        .str.split(",")
                        .apply(lambda lst: str(lgid) in lst)
            ]
            cand = cand[cand["Trips_Used"] < MAX_TRIPS]
            if cand.empty:
                continue
            shared = cand[cand["Mapped_LG_IDs"].str.contains(",")]
            chosen = shared.iloc[0] if not shared.empty else cand.iloc[0]
            vid, cap = chosen["Vehicle_ID"], chosen["Capacity_tons"]
            qty = min(cap, need_qty, lg_stock.get(lgid,0.0))
            if qty <= 0:
                continue

            dispatch_rows.append({
                "Day": day,
                "Vehicle_ID": vid,
                "LG_ID": lgid,
                "FPS_ID": fid,
                "Quantity_tons": qty
            })
            lg_stock[lgid]  -= qty
            fps_stock[fid]  += qty
            vehicles.loc[vehicles.Vehicle_ID==vid,"Trips_Used"] += 1

        # record stocks
        for lgid, st in lg_stock.items():
            stock_rows.append({
                "Day": day,
                "Entity_Type": "LG",
                "Entity_ID": lgid,
                "Stock_Level_tons": st
            })
        for fid, st in fps_stock.items():
            stock_rows.append({
                "Day": day,
                "Entity_Type": "FPS",
                "Entity_ID": fid,
                "Stock_Level_tons": st
            })

    dispatch_cg   = pd.DataFrame(dispatch_rows)
    dispatch_lg   = dispatch_cg.copy()  # split if needed
    stock_levels  = pd.DataFrame(stock_rows)
    return dispatch_cg, dispatch_lg, stock_levels
