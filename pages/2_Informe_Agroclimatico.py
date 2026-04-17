"""Página Streamlit: Informe Agroclimático Santibáñez v4 (backend on-demand)."""
import streamlit as st

import informes_api

st.set_page_config(
    page_title="Informe Agroclimático",
    page_icon="🌾",
    layout="wide",
)

informes_api.render_tab_informes()
