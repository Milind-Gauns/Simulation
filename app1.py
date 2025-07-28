import os
import streamlit as st
import pandas as pd
from io import BytesIO
from simulation import run_simulation

# 1) Page config
st.set_page_config(page_title="Grain Distribution Simulator", layout="wide")

# 2) Download helper
def to_excel(sheets: dict[str,pd.DataFrame]) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        for name, df in sheets.items():
            df.to_excel(w, sheet_name=name, index=False)
    return buf.getvalue()

# 3) UI title
st.title("🚛 Grain Distribution Simulator")

# 4) File uploader + fallback
uploaded = st.file_uploader("Upload master workbook (.xlsx)", type="xlsx")
if uploaded is not None:
    master = uploaded
elif os.path.exists("grain_simulator_template.xlsx"):
    master = "grain_simulator_template.xlsx"
else:
    st.error("❌ Please upload an Excel template first.")
    st.stop()

# 5) Load input sheets
@st.cache_data
def load_inputs(path):
    s = pd.read_excel(path, sheet_name="Settings")
    lg= pd.read_excel(path, sheet_name="LGs")
    fp= pd.read_excel(path, sheet_name="FPS")
    try:
        vh= pd.read_excel(path, sheet_name="Vehicles")
    except ValueError:
        vh = pd.DataFrame(columns=["Vehicle_ID","Capacity_tons","Mapped_LG_IDs"])
    return s, lg, fp, vh

settings, lgs, fps, vehicles = load_inputs(master)

# 6) Preview
with st.expander("🔍 Preview inputs"):
    st.subheader("Settings");      st.dataframe(settings)
    st.subheader("Local Godowns"); st.dataframe(lgs)
    st.subheader("Shops (FPS)");   st.dataframe(fps)
    st.subheader("Vehicles");      st.dataframe(vehicles)

# 7) Run button
if st.button("▶️ Run Simulation"):
    with st.spinner("Running…"):
        dispatch_cg, dispatch_lg, stock_levels = run_simulation(
            master, settings, lgs, fps, vehicles
        )
    st.success("✅ Done")

    # package
    sheets = {
        "Settings": settings,
        "LGs":      lgs,
        "FPS":      fps,
        "Vehicles": vehicles,
        "CG_to_LG": dispatch_cg,
        "LG_to_FPS":dispatch_lg,
        "Stock_Levels": stock_levels,
    }
    data = to_excel(sheets)
    st.download_button(
        "📥 Download simulation_output.xlsx",
        data, "simulation_output.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.info("Upload your workbook and click **Run Simulation**")
