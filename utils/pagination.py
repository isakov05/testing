"""
Pagination utilities for displaying large dataframes
"""
import streamlit as st
import pandas as pd
from typing import Tuple


def paginate_dataframe(
    df: pd.DataFrame,
    page_size: int = 100,
    key_prefix: str = "page"
) -> Tuple[pd.DataFrame, dict]:
    """
    Paginate a dataframe and return the current page data with pagination controls.

    Args:
        df: DataFrame to paginate
        page_size: Number of records per page (default: 100)
        key_prefix: Unique prefix for session state keys (to support multiple tables on same page)

    Returns:
        Tuple of (paginated_df, pagination_info)
        - paginated_df: DataFrame slice for current page
        - pagination_info: Dict with 'current_page', 'total_pages', 'start_idx', 'end_idx', 'total_records'
    """
    if df is None or df.empty:
        return df, {
            'current_page': 0,
            'total_pages': 0,
            'start_idx': 0,
            'end_idx': 0,
            'total_records': 0
        }

    # Initialize session state for this table
    page_key = f"{key_prefix}_current_page"
    if page_key not in st.session_state:
        st.session_state[page_key] = 1

    total_records = len(df)
    total_pages = (total_records + page_size - 1) // page_size  # Ceiling division

    # Ensure current page is within bounds
    if st.session_state[page_key] > total_pages:
        st.session_state[page_key] = total_pages if total_pages > 0 else 1
    if st.session_state[page_key] < 1:
        st.session_state[page_key] = 1

    current_page = st.session_state[page_key]
    start_idx = (current_page - 1) * page_size
    end_idx = min(start_idx + page_size, total_records)

    # Get the slice for current page
    paginated_df = df.iloc[start_idx:end_idx]

    pagination_info = {
        'current_page': current_page,
        'total_pages': total_pages,
        'start_idx': start_idx,
        'end_idx': end_idx,
        'total_records': total_records
    }

    return paginated_df, pagination_info


def render_pagination_controls(
    pagination_info: dict,
    key_prefix: str = "page"
) -> None:
    """
    Render pagination controls at the bottom of a table.

    Args:
        pagination_info: Dictionary returned by paginate_dataframe
        key_prefix: Same prefix used in paginate_dataframe
    """
    if pagination_info['total_pages'] <= 1:
        return  # No pagination needed

    page_key = f"{key_prefix}_current_page"
    current_page = pagination_info['current_page']
    total_pages = pagination_info['total_pages']
    start_idx = pagination_info['start_idx']
    end_idx = pagination_info['end_idx']
    total_records = pagination_info['total_records']

    # Display pagination info and controls
    st.markdown("---")

    col1, col2, col3, col4, col5 = st.columns([2, 1, 2, 1, 2])

    with col1:
        st.markdown(f"**Showing {start_idx + 1}-{end_idx} of {total_records} records**")

    with col2:
        if st.button("⏮️ First", key=f"{key_prefix}_first", disabled=(current_page == 1)):
            st.session_state[page_key] = 1
            st.rerun()

    with col3:
        if st.button("◀️ Previous", key=f"{key_prefix}_prev", disabled=(current_page == 1)):
            st.session_state[page_key] = current_page - 1
            st.rerun()

    with col4:
        st.markdown(f"<div style='text-align: center; padding-top: 5px;'><b>Page {current_page} of {total_pages}</b></div>", unsafe_allow_html=True)

    with col5:
        col5a, col5b = st.columns(2)
        with col5a:
            if st.button("Next ▶️", key=f"{key_prefix}_next", disabled=(current_page >= total_pages)):
                st.session_state[page_key] = current_page + 1
                st.rerun()
        with col5b:
            if st.button("Last ⏭️", key=f"{key_prefix}_last", disabled=(current_page >= total_pages)):
                st.session_state[page_key] = total_pages
                st.rerun()

    # Optional: Jump to page
    with st.expander("🔢 Jump to page"):
        jump_page = st.number_input(
            "Page number",
            min_value=1,
            max_value=total_pages,
            value=current_page,
            key=f"{key_prefix}_jump_input"
        )
        if st.button("Go", key=f"{key_prefix}_jump_btn"):
            st.session_state[page_key] = jump_page
            st.rerun()
