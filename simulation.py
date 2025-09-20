# app1.py
import os
from io import BytesIO
import streamlit as st
import pandas as pd

from simulation import run_simulation  # make sure simulation.py is alongside this file

st.set_page_config(page_title="Grain Distribution Simulator", layout="wide")
st.title("🚛 Grain Distribution Simulator")

# ---------------------------
# Helpers
# ---------------------------
REQUIRED_SHEETS = ["Settings", "LGs", "FPS"]  # Vehicles is optional

def to_excel(sheets: dict[str, pd.DataFrame]) -> bytes:
    """Write multiple DataFrames (sheet_name -> df) into one Excel bytes object."""
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        for name, df in sheets.items():
            # Ensure DataFrame even if None
            (df if df is not None else pd.DataFrame()).to_excel(w, sheet_name=name, index=False)
    buf.seek(0)
    return buf.getvalue()

def template_workbook() -> bytes:
    """Return a minimal template workbook with correct headers."""
    settings = pd.DataFrame({
        "Parameter": [
            "Distribution_Days",
            "Vehicle_Capacity_tons",
            "Vehicles_Total",
            "Max_Trips_Per_Vehicle_Per_Day",
            "Default_Lead_Time_days",
            "Start_Date",  # YYYY-MM-DD start date for timeline (optional)
            "AAY_kg_per_card",
            "PHH_kg_per_beneficiary",
            "APL_kg_per_card",
        ],
        "Value": [30, 11.5, 30, 3, 3, pd.Timestamp.today().strftime("%Y-%m-%d"), 35.0, 5.0, 2.5],
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
        # we'll compute Monthly_Demand_tons from the counts below
        "AAY_Count": [1000, 500, 800],
        "PHH_Beneficiaries": [200, 100, 50],
        "APL_Count": [50, 30, 20],
        "Max_Capacity_tons": [40.0, 30.0, 35.0],
        # Can be LG_ID (1/2) or LG_Name ("LG_A"/"LG_B")
        "Linked_LG_ID": ["LG_A", "LG_B", "LG_A"],
        # Optional; if omitted, defaults to settings["Default_Lead_Time_days"]
        "Lead_Time_days": [3, None, 2],
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
    """Read a sheet and optionally validate columns; raise ValueError with a friendly message."""
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
    """Load required inputs from uploaded file or path; Vehicles is optional."""
    # We re-open a BytesIO because ExcelFile keeps a read pointer.
    data = src.read() if hasattr(src, "read") else open(src, "rb").read()
    xls = BytesIO(data)

    settings = read_sheet(
        xls, "Settings",
        required_cols={"Parameter", "Value"}
    )
    xls.seek(0)
    lgs = read_sheet(
        xls, "LGs",
        required_cols={"LG_ID", "LG_Name"}
    )
    xls.seek(0)
    fps = read_sheet(
        xls, "FPS",
        required_cols={"FPS_ID", "Max_Capacity_tons", "Linked_LG_ID"}
    )
    xls.seek(0)
    try:
        vehicles = read_sheet(
            xls, "Vehicles",
            required_cols={"Vehicle_ID"}  # Capacity_tons & Mapped_LG_IDs are optional
        )
    except ValueError:
        vehicles = pd.DataFrame(columns=["Vehicle_ID", "Capacity_tons", "Mapped_LG_IDs"])

    # --- derive Monthly_Demand_tons from RC counts automatically ---
    # Determine settings for eligibilities
    def _get_setting_param(s_df, name, default):
        try:
            return float(s_df.loc[s_df["Parameter"] == name, "Value"].iloc[0])
        except Exception:
            return float(default)

    AAY_kg = _get_setting_param(settings, "AAY_kg_per_card", 35.0)
    PHH_kg = _get_setting_param(settings, "PHH_kg_per_beneficiary", 5.0)
    APL_kg = _get_setting_param(settings, "APL_kg_per_card", 0.0)

    # normalize/ensure count columns exist
    fps = fps.copy()
    fps["AAY_Count"] = fps.get("AAY_Count", 0).fillna(0).astype(float)
    fps["PHH_Beneficiaries"] = fps.get("PHH_Beneficiaries", 0).fillna(0).astype(float)
    fps["APL_Count"] = fps.get("APL_Count", 0).fillna(0).astype(float)

    # compute monthly demand (kg -> tons). User requested monthly demand = counts * eligibility (kgs) converted to tons.
    fps["Monthly_from_counts_kg"] = fps["AAY_Count"] * AAY_kg + fps["PHH_Beneficiaries"] * PHH_kg + fps["APL_Count"] * APL_kg
    fps["Monthly_Demand_tons"] = fps.get("Monthly_Demand_tons")
    fps["Monthly_Demand_tons"] = pd.to_numeric(fps["Monthly_Demand_tons"], errors="coerce")
    # override or fill with computed value — user said they will not enter demand, so prefer derived value
    fps["Monthly_Demand_tons"] = (fps["Monthly_from_counts_kg"] / 1000.0).fillna(0.0)

    return settings, lgs, fps, vehicles

# ---------------------------
# Sidebar: template download
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

# ---------------------------
# Upload or fallback to local
# ---------------------------
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
        st.subheader("FPS (counts-derived demand)")
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

        # Ensure Date column exists on outputs (in case simulation wasn't updated)
        # Derive from Settings.Start_Date if missing (same exclusion of Sundays)
        def ensure_dates_present(df, settings_df):
            if "Date" in df.columns and not df["Date"].isnull().all():
                return df
            # build mapping from Start_Date
            try:
                start_date_val = settings_df.loc[settings_df["Parameter"] == "Start_Date", "Value"].iloc[0]
            except Exception:
                start_date_val = None
            if pd.notna(start_date_val):
                try:
                    start_dt = pd.to_datetime(start_date_val).normalize()
                except Exception:
                    start_dt = pd.Timestamp.today().normalize()
            else:
                start_dt = pd.Timestamp.today().normalize()
            while start_dt.weekday() == 6:
                start_dt += pd.Timedelta(days=1)
            day_to_date = {}
            cur = start_dt
            for d in range(1, int(settings_df.loc[settings_df["Parameter"] == "Distribution_Days", "Value"].iloc[0]) + 1):
                if cur.weekday() != 6:
                    day_to_date[d] = cur.date()
                else:
                    # skip Sundays by advancing until non-Sunday and account in mapping
                    while cur.weekday() == 6:
                        cur += pd.Timedelta(days=1)
                    day_to_date[d] = cur.date()
                cur += pd.Timedelta(days=1)
            if "Day" in df.columns:
                df = df.copy()
                df["Date"] = df["Day"].apply(lambda d: day_to_date.get(int(d)))
            return df

        dispatch_lg = ensure_dates_present(dispatch_lg, settings)
        dispatch_cg = ensure_dates_present(dispatch_cg, settings)
        stock_levels = ensure_dates_present(stock_levels, settings)

        # Previews
        with st.expander("👀 Preview Results", expanded=False):
            st.subheader("LG → FPS (dispatch_lg)")
            st.dataframe(dispatch_lg, use_container_width=True, height=240)
            st.subheader("CG → LG (dispatch_cg)")
            st.dataframe(dispatch_cg, use_container_width=True, height=240)
            st.subheader("Stock Levels")
            st.dataframe(stock_levels, use_container_width=True, height=240)

        # Package for download
        output_sheets = {
            "Settings":     settings,
            "LGs":          lgs,
            "FPS":          fps,
            "Vehicles":     vehicles,
            "LG_to_FPS":    dispatch_lg,     # keep clear names
            "CG_to_LG":     dispatch_cg,
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
