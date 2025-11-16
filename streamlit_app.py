# streamlit_app.py
import streamlit as st
import requests, json, os

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.title("ETL Auraverse — Upload & Schema")

st.markdown("Upload a file (.txt, .md, .pdf) and give a `source_id` (optional).")

source_id = st.text_input("source_id (optional)")
uploaded = st.file_uploader("Choose a file", type=["txt","md","json","pdf"])
version = st.text_input("version (optional)")

if st.button("Upload"):
    if not uploaded:
        st.error("Please choose a file")
    else:
        files = {"file": (uploaded.name, uploaded.getvalue(), uploaded.type)}
        data = {}
        if source_id:
            data["source_id"] = source_id
        if version:
            data["version"] = version
        resp = requests.post(f"{API_URL}/upload", files=files, data=data)
        st.write("Response:")
        try:
            st.json(resp.json())
        except:
            st.text(resp.text)

st.markdown("You can then view schema: `/schema?source_id=...`")
st.markdown("Or query using `/query` (NL -> filter demo).")
