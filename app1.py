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
        # Optional: if you prefer counts, use AAY_Count/PHH_Beneficiaries/APL_Count
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
    # FPS: relax requirement to allow count-based inputs instead of Monthly_Demand_tons
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

    return settings, lgs, fps, vehicles

# rest of the file is unchanged except that run_simulation will now produce category splits & Date columns
# (the remainder of your app1.py logic remains identical)
# (I did not change the UI flow; the simulation call stays the same)
# ... (file continues exactly as before for UI run button logic) ...
