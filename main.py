import streamlit as st

st.set_page_config(page_title="DAP Trucks", layout="wide", page_icon="🚛")

pg = st.navigation([
    st.Page("app.py", title="Delivery Map", icon="🗺️"),
    st.Page("calculator.py", title="Price Calculator", icon="💰"),
])
pg.run()
