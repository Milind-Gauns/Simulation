# app1.py
import os
from io import BytesIO
import streamlit as st
import pandas as pd

from simulation import run_simulation  # ensure simulation.py sits alongside this file

st.set_page_config(page_title="Grain Distribution Simulator", layout="wide")
st.title("🚛 Grain Distribution Simulator")

# ---------------------------
# Helpers
# ---------------------------
def to_excel(sheets: dict[str, pd.DataFrame]) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        for name, df in sheets.items():
            (df if df is not None else pd.DataFrame()).to_excel(w, sheet_name=name, index=False)
    buf.seek(0)
    return buf.getvalue()

def template_workbook() -> bytes:
    settings = pd.DataFrame({
        "Parameter": [
            "Distribution_Days",
            "Vehicle_Capacity_tons",
            "Vehicles_Total",
            "Max_Trips_Per_Vehicle_Per_Day",
            "Default_Lead_Time_days",
            "AAY_kg_per_card",
            "PHH_kg_per_beneficiary",
            "APL_kg_per_card",
            "Start_Date"
        ],
        "Value": [30, 11.5, 30, 3, 3, 35, 5, 0, pd.Timestamp.today().strftime("%Y-%m-%d")],
    })

    lgs = pd.DataFrame({
        "LG_ID": [1, 2],
        "LG_Name": ["LG_A", "LG_B"],
        "Storage_Capacity_tons": [500.0, 400.0],
        "Initial_Allocation_tons": [0.0, 0.0],
        "Initial_LG_stock": [0.0, 0.0],
    })

    fps = pd.DataFrame({
        "FPS_ID": [101, 102, 201],
        "FPS_Name": ["Shop_101", "Shop_102", "Shop_201"],
        "Monthly_Demand_tons": [150.0, 90.0, 120.0],
        "Max_Capacity_tons": [40.0, 30.0, 35.0],
        "Linked_LG_ID": ["LG_A", "LG_B", "LG_A"],
        "Lead_Time_days": [3, None, 2],
        "AAY_Count": [100, 50, 80],
        "PHH_Beneficiaries": [200, 100, 150],
        "APL_Count": [0, 0, 10]
    })

    vehicles = pd.DataFrame({
        "Vehicle_ID": [1, 2, 3, 4, 5],
        "Capacity_tons": [11.5, 11.5, 11.5, 11.5, 11.5],
        "Mapped_LG_IDs": ["LG_A,LG_B", "LG_A", "LG_B", "1,2", "LG_A"],
    })

    return to_excel({
        "Settings": settings,
        "LGs": lgs,
        "FPS": fps,
        "Vehicles": vehicles,
        "LG_Capacity": pd.DataFrame({"LG_ID": [1, 2], "Capacity_tons": [500.0, 400.0]}),
    })

def read_sheet(xls_obj, sheet, required_cols=None) -> pd.DataFrame:
    try:
        df = pd.read_excel(xls_obj, sheet_name=sheet)
    except ValueError as e:
        raise ValueError(f"Worksheet named '{sheet}' not found.") from e
    if required_cols:
        missing = set(required_cols) - set(df.columns)
        if missing:
            raise ValueError(f"Sheet '{sheet}' is missing required columns: {sorted(missing)}")
    return df

@st.cache_data
def load_inputs(src):
    data = src.read() if hasattr(src, "read") else open(src, "rb").read()
    xls = BytesIO(data)

    settings = read_sheet(xls, "Settings", required_cols={"Parameter", "Value"})
    xls.seek(0)
    lgs = read_sheet(xls, "LGs", required_cols={"LG_ID", "LG_Name"})
    xls.seek(0)
    # FPS: require key linking & capacity but allow counts instead of Monthly_Demand_tons
    fps = read_sheet(xls, "FPS", required_cols={"FPS_ID", "Max_Capacity_tons", "Linked_LG_ID"})
    xls.seek(0)
    try:
        vehicles = read_sheet(xls, "Vehicles", required_cols={"Vehicle_ID"})
    except ValueError:
        vehicles = pd.DataFrame(columns=["Vehicle_ID", "Capacity_tons", "Mapped_LG_IDs"])

    return settings, lgs, fps, vehicles

# ---------------------------
# Sidebar template & upload
# ---------------------------
with st.sidebar:
    st.subheader("📄 Template")
    st.download_button(
        "Download input template (Excel)",
        data=template_workbook(),
        file_name="grain_simulator_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )

uploaded = st.file_uploader("Upload master workbook (.xlsx)", type="xlsx")
if uploaded is not None:
    master = uploaded
elif os.path.exists("grain_simulator_template.xlsx"):
    master = "grain_simulator_template.xlsx"
else:
    st.warning("Please upload an Excel file using the button above, or place 'grain_simulator_template.xlsx' in the working directory.")
    st.stop()

# ---------------------------
# Load & preview inputs
# ---------------------------
try:
    settings, lgs, fps, vehicles = load_inputs(master)
except Exception as e:
    st.error(f"❌ Could not load inputs: {e}")
    st.stop()

with st.expander("🔍 Preview Inputs", expanded=False):
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Settings")
        st.dataframe(settings, use_container_width=True)
        st.subheader("LGs")
        st.dataframe(lgs, use_container_width=True)
    with c2:
        st.subheader("FPS")
        st.dataframe(fps, use_container_width=True)
        st.subheader("Vehicles")
        st.dataframe(vehicles, use_container_width=True)

# ---------------------------
# Run simulation
# ---------------------------
if st.button("▶️ Run Simulation", use_container_width=True):
    try:
        with st.spinner("Running simulation…"):
            dispatch_cg, dispatch_lg, stock_levels = run_simulation(
                master, settings, lgs, fps, vehicles
            )
        st.success("✅ Simulation complete")

        with st.expander("👀 Preview Results", expanded=False):
            st.subheader("LG → FPS (dispatch_lg)")
            st.dataframe(dispatch_lg, use_container_width=True, height=240)
            st.subheader("CG → LG (dispatch_cg)")
            st.dataframe(dispatch_cg, use_container_width=True, height=240)
            st.subheader("Stock Levels")
            st.dataframe(stock_levels, use_container_width=True, height=240)

        output_sheets = {
            "Settings": settings,
            "LGs": lgs,
            "FPS": fps,
            "Vehicles": vehicles,
            "LG_to_FPS": dispatch_lg,
            "CG_to_LG": dispatch_cg,
            "Stock_Levels": stock_levels,
        }
        excel_bytes = to_excel(output_sheets)

        st.download_button(
            label="📥 Download simulation_output.xlsx",
            data=excel_bytes,
            file_name="simulation_output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    except Exception as e:
        st.error("❌ Simulation failed.")
        st.exception(e)
else:
    st.info("Upload your workbook above, review inputs, then click ▶️ Run Simulation.")
