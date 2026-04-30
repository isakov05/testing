import streamlit as st
from auth.db_authenticator import show_login_form, check_authentication, get_current_user, show_logout_button
from utils.cookie_manager import mount_cookie_manager
from utils.session_loader import get_all_companies, get_user_company_tin


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

    # Sidebar: user info, company selector, and logout
    with st.sidebar:
        user_info = get_current_user()
        st.markdown(f"**{user_info['username']}**")
        if user_info.get('email'):
            st.markdown(f"{user_info['email']}")
        show_logout_button()
        st.markdown("---")

        # Company derived from uploaded data
        companies_df = get_all_companies(min_invoices=1)
        if not companies_df.empty:
            companies_df['label'] = companies_df.apply(
                lambda r: f"{r['name'][:40]} ({r['tin']})" if r['name'] else r['tin'], axis=1
            )
            tin_to_label = dict(zip(companies_df['tin'], companies_df['label']))
            label_to_tin = dict(zip(companies_df['label'], companies_df['tin']))
            options = companies_df['label'].tolist()

            current_tin = st.session_state.get('selected_company_tin')
            default_idx = 0
            if current_tin and current_tin in tin_to_label:
                default_idx = options.index(tin_to_label[current_tin])

            st.markdown("**Company**")
            selected_label = st.selectbox(
                "Company", options, index=default_idx, label_visibility="collapsed"
            )
            new_tin = label_to_tin[selected_label]
            if new_tin != st.session_state.get('selected_company_tin'):
                st.session_state['selected_company_tin'] = new_tin
                st.rerun()
        else:
            st.caption("Upload invoices to see company")

        st.markdown("---")

    # Single page app — run the analytics page directly
    pages = [
        st.Page("pages/dashboard.py", title="Dashboard", icon="📊"),
        st.Page("pages/invoice_analytics.py", title="Invoice Analytics", icon="📈"),
        st.Page("pages/11_Business_Overview.py", title="Business Overview", icon="🗂️"),
        st.Page("pages/file_upload.py", title="File Upload", icon="📂"),
    ]

    nav = st.navigation(pages)
    nav.run()


if __name__ == "__main__":
    main()
