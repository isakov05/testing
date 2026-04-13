import streamlit as st
from auth.db_authenticator import protect_page

st.set_page_config(page_title="Financing", page_icon="💳", layout="wide")

protect_page()


st.title("💳 Financing")
st.info("Placeholder page. Add financing products and scenarios.")
