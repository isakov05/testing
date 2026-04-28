import streamlit as st
from auth.db_authenticator import show_login_form, check_authentication, get_current_user, show_logout_button
from utils.cookie_manager import mount_cookie_manager


st.set_page_config(
    page_title="FLOTT Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)


def main() -> None:
    mount_cookie_manager()

    if not check_authentication():
        st.markdown("""
            <style>
                section[data-testid="stSidebar"] {
                    display: none !important;
                }
            </style>
        """, unsafe_allow_html=True)
        show_login_form()
        st.stop()

    # Sidebar: user info and logout
    with st.sidebar:
        user_info = get_current_user()
        st.markdown(f"**{user_info['username']}**")
        if user_info.get('email'):
            st.markdown(f"{user_info['email']}")
        show_logout_button()
        st.markdown("---")

    # Single page app — run the analytics page directly
    pages = [
        st.Page("pages/dashboard.py", title="Dashboard", icon="📊"),
        st.Page("pages/invoice_analytics.py", title="Invoice Analytics", icon="📈"),
        st.Page("pages/file_upload.py", title="File Upload", icon="📂"),
    ]

    nav = st.navigation(pages)
    nav.run()


if __name__ == "__main__":
    main()
