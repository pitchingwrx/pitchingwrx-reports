"""
PitchingWRX — Streamlit Web UI
"""

import streamlit as st
import requests
import io

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PitchingWRX Reports",
    page_icon="⚾",
    layout="centered"
)

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown("""
    <style>
        .main { background-color: #1A1F2E; }
        h1 { color: #F47920; }
        h3 { color: #E8EAF0; }
        .stButton>button {
            background-color: #F47920;
            color: white;
            border: none;
            padding: 0.5em 2em;
            font-size: 1em;
            border-radius: 6px;
        }
    </style>
""", unsafe_allow_html=True)

st.title("⚾ PitchingWRX")
st.subheader("Game Outing Report Generator")
st.markdown("---")

# ── API URL ────────────────────────────────────────────────────────────────────
API​​​​​​​​​​​​​​​​
