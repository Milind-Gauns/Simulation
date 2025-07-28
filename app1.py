import streamlit as st
import pandas as pd
from io import BytesIO
from simulation import run_simulation  # your unified CG→LG & LG→FPS algorithm

# ————————————————————————————————
# 1. Page config
# ————————————————————————————————
st.set_page_config(
    page_title="Grain Distribution Simulator",
    layout="wide",
)

# ————————————————————————————————
# 2. Helper to build downloadable Excel
# ————————————————————————————————
def to_excel(sheets: dict[str, pd.DataFrame]) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)
    return buffer.getvalue()

# ————————————————————————————————
# 3. UI: Title & instructions
# ————————————————————————————————
st.title("🚛 Grain Distribution Simulator")
st.markdown("""
1. Upload your **master workbook** (must include at least `Settings`, `LGs`, `FPS`; optional: `Vehicles`, `LG_Daily_Req`, `LG_Capacity`).  
2. Click **Run Simulation**.  
3. Download the **simulation_output.xlsx** and feed it into your dashboard app.
""")

# ————————————————————————————————
# 4. File uploader
# ————————————————————————————————
uploaded = st.file_uploader(
    "Upload Master Workbook (.xlsx)",
    type="xlsx",
    help="Should contain sheets: Settings, LGs, FPS (plus optional Vehicles, LG_Daily_Req, LG_Capacity)."
)
if uploaded is None:
    st.info("Using default template from repo: `grain_simulator_template.xlsx`.")
    master_path = "grain_simulator_template.xlsx"
else:
    master_path = uploaded

# ————————————————————————————————
# 5. Load inputs (cached)
# ————————————————————————————————
@st.cache_data(show_spinner=False)
def load_inputs(path):
    settings = pd.read_excel(path, sheet_name="Settings")
    lgs      = pd.read_excel(path, sheet_name="LGs")
    fps      = pd.read_excel(path, sheet_name="FPS")
    try:
        vehicles = pd.read_excel(path, sheet_name="Vehicles")
    except ValueError:
        vehicles = pd.DataFrame(columns=["Vehicle_ID","Capacity_tons","Mapped_LG_IDs"])
    return settings, lgs, fps, vehicles

settings, lgs, fps, vehicles = load_inputs(master_path)

# ————————————————————————————————
# 6. Preview input data
# ————————————————————————————————
with st.expander("🔍 Preview Uploaded Data"):
    st.subheader("Settings")
    st.dataframe(settings, height=150)
    st.subheader("Local Godowns (LGs)")
    st.dataframe(lgs, height=200)
    st.subheader("Fair Price Shops (FPS)")
    st.dataframe(fps, height=200)
    st.subheader("Vehicles")
    st.dataframe(vehicles, height=150)

# ————————————————————————————————
# 7. Run simulation button
# ————————————————————————————————
if st.button("▶️ Run Simulation"):
    with st.spinner("Computing optimal dispatch…"):
        dispatch_cg, dispatch_lg, stock_levels = run_simulation(
            settings, lgs, fps, vehicles
        )
    st.success("Simulation complete! ✅")

    # Package inputs + outputs
    all_sheets = {
        "Settings":       settings,
        "LGs":            lgs,
        "FPS":            fps,
        "Vehicles":       vehicles,
        "CG_to_LG":       dispatch_cg,
        "LG_to_FPS":      dispatch_lg,
        "Stock_Levels":   stock_levels,
    }
    excel_data = to_excel(all_sheets)

    st.download_button(
        label="📥 Download simulation_output.xlsx",
        data=excel_data,
        file_name="simulation_output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("Click **Run Simulation** to execute the distribution algorithm.")
