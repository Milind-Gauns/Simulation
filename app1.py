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
# 2. Helper to
