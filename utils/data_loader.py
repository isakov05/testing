"""
Centralized Data Loader with Caching
Provides a single source of truth for loading user data with smart caching
"""
import streamlit as st
import pandas as pd
from typing import Dict, Optional
from datetime import datetime, timedelta
from utils.db_operations import (
    load_user_invoices,
    load_user_bank_transactions,
    load_user_reconciliation
)


# Cache configuration
CACHE_TTL_MINUTES = 5  # Cache data for 5 minutes


@st.cache_data(ttl=timedelta(minutes=CACHE_TTL_MINUTES))
def _load_invoices_cached(user_id: int, invoice_type: str) -> pd.DataFrame:
    """Load invoices with caching. Cache key includes user_id and invoice_type."""
    print(f"DEBUG _load_invoices_cached: Loading {invoice_type} invoices for user {user_id} from database")
    df = load_user_invoices(user_id, invoice_type)
    print(f"DEBUG _load_invoices_cached: Loaded {len(df)} {invoice_type} invoices")
    return df


@st.cache_data(ttl=timedelta(minutes=CACHE_TTL_MINUTES))
def _load_bank_transactions_cached(user_id: int) -> pd.DataFrame:
    """Load bank transactions with caching."""
    print(f"DEBUG _load_bank_transactions_cached: Loading bank transactions for user {user_id} from database")
    df = load_user_bank_transactions(user_id)
    print(f"DEBUG _load_bank_transactions_cached: Loaded {len(df)} bank transactions")
    return df


@st.cache_data(ttl=timedelta(minutes=CACHE_TTL_MINUTES))
def _load_reconciliation_cached(user_id: int, record_type: str) -> pd.DataFrame:
    """Load reconciliation data with caching."""
    print(f"DEBUG _load_reconciliation_cached: Loading {record_type} reconciliation for user {user_id} from database")
    df = load_user_reconciliation(user_id, record_type)
    print(f"DEBUG _load_reconciliation_cached: Loaded {len(df)} {record_type} reconciliation records")
    return df


def get_user_data(user_id: Optional[int] = None) -> Dict[str, Optional[pd.DataFrame]]:
    """
    Get all user data with intelligent caching.

    Args:
        user_id: User ID to load data for. If None, gets from session_state.

    Returns:
        Dictionary with keys:
        - 'invoices_in': DataFrame of incoming invoices
        - 'invoices_out': DataFrame of outgoing invoices
        - 'bank_statements': DataFrame of bank transactions
        - 'reconciliation_ar': DataFrame of AR reconciliation
        - 'reconciliation_ap': DataFrame of AP reconciliation
        - 'loaded_at': Timestamp when data was loaded
    """
    # Get user_id from session state if not provided
    if user_id is None:
        user_id = st.session_state.get('user_id')

    if not user_id:
        print("WARN get_user_data: No user_id provided or in session_state")
        return {
            'invoices_in': None,
            'invoices_out': None,
            'bank_statements': None,
            'reconciliation_ar': None,
            'reconciliation_ap': None,
            'loaded_at': None
        }

    print(f"DEBUG get_user_data: Loading data for user_id={user_id}")

    # Load all data using cached functions
    invoices_in = _load_invoices_cached(user_id, 'IN')
    invoices_out = _load_invoices_cached(user_id, 'OUT')
    bank_statements = _load_bank_transactions_cached(user_id)
    reconciliation_ar = _load_reconciliation_cached(user_id, 'IN')
    reconciliation_ap = _load_reconciliation_cached(user_id, 'OUT')

    # Store in session state for fast access across pages
    st.session_state.invoices_in_processed = invoices_in if not invoices_in.empty else None
    st.session_state.invoices_out_processed = invoices_out if not invoices_out.empty else None
    st.session_state.bank_statements_processed = bank_statements if not bank_statements.empty else None
    st.session_state.reconciliation_ar_processed = reconciliation_ar if not reconciliation_ar.empty else None
    st.session_state.reconciliation_ap_processed = reconciliation_ap if not reconciliation_ap.empty else None
    st.session_state.data_loaded_at = datetime.now()
    st.session_state.data_loaded_from_db = True
    st.session_state.data_loaded_for_user_id = user_id  # Track which user this data belongs to

    print(f"DEBUG get_user_data: Data loaded and cached in session_state for user_id={user_id}")

    return {
        'invoices_in': invoices_in if not invoices_in.empty else None,
        'invoices_out': invoices_out if not invoices_out.empty else None,
        'bank_statements': bank_statements if not bank_statements.empty else None,
        'reconciliation_ar': reconciliation_ar if not reconciliation_ar.empty else None,
        'reconciliation_ap': reconciliation_ap if not reconciliation_ap.empty else None,
        'loaded_at': datetime.now()
    }


def get_user_data_from_session() -> Dict[str, Optional[pd.DataFrame]]:
    """
    Get user data from session state if available, otherwise load from database.

    This is faster than get_user_data() because it tries session_state first.
    Validates that cached data belongs to the current user.

    Returns:
        Dictionary with data, same structure as get_user_data()
    """
    current_user_id = st.session_state.get('user_id')

    if not current_user_id:
        print("WARN get_user_data_from_session: No user_id in session_state")
        return {
            'invoices_in': None,
            'invoices_out': None,
            'bank_statements': None,
            'reconciliation_ar': None,
            'reconciliation_ap': None,
            'loaded_at': None
        }

    # Check if data exists in session state AND belongs to current user
    cached_user_id = st.session_state.get('data_loaded_for_user_id')

    if st.session_state.get('data_loaded_from_db') and cached_user_id == current_user_id:
        print(f"DEBUG get_user_data_from_session: Using data from session_state for user {current_user_id} (fast path)")
        return {
            'invoices_in': st.session_state.get('invoices_in_processed'),
            'invoices_out': st.session_state.get('invoices_out_processed'),
            'bank_statements': st.session_state.get('bank_statements_processed'),
            'reconciliation_ar': st.session_state.get('reconciliation_ar_processed'),
            'reconciliation_ap': st.session_state.get('reconciliation_ap_processed'),
            'loaded_at': st.session_state.get('data_loaded_at')
        }

    # Data not in session state or belongs to different user, load from database
    if cached_user_id and cached_user_id != current_user_id:
        print(f"DEBUG get_user_data_from_session: Cached data belongs to user {cached_user_id}, but current user is {current_user_id}. Reloading...")
    else:
        print("DEBUG get_user_data_from_session: No data in session_state, loading from database")

    return get_user_data(current_user_id)


def refresh_user_data(user_id: Optional[int] = None) -> Dict[str, Optional[pd.DataFrame]]:
    """
    Force refresh of user data by clearing cache and reloading.

    Args:
        user_id: User ID to refresh data for. If None, gets from session_state.

    Returns:
        Dictionary with refreshed data
    """
    if user_id is None:
        user_id = st.session_state.get('user_id')

    if not user_id:
        print("WARN refresh_user_data: No user_id provided")
        return get_user_data()

    print(f"DEBUG refresh_user_data: Clearing cache and reloading data for user_id={user_id}")

    # Clear the cache for this user
    _load_invoices_cached.clear()
    _load_bank_transactions_cached.clear()
    _load_reconciliation_cached.clear()

    # Clear session state data - IMPORTANT: Delete actual data, not just flags
    session_keys_to_clear = [
        'data_loaded_from_db',
        'data_loaded_at',
        'data_loaded_for_user_id',
        'invoices_in_processed',
        'invoices_out_processed',
        'bank_statements_processed',
        'reconciliation_ar_processed',
        'reconciliation_ap_processed'
    ]

    for key in session_keys_to_clear:
        if key in st.session_state:
            print(f"DEBUG refresh_user_data: Deleting session_state['{key}']")
            del st.session_state[key]

    print(f"DEBUG refresh_user_data: Session state cleared, now loading fresh data from database")

    # Reload data
    return get_user_data(user_id)


def clear_all_data_cache():
    """
    Clear all cached data for all users.
    Use this sparingly - typically only for debugging or admin actions.
    """
    print("DEBUG clear_all_data_cache: Clearing all cached data")
    _load_invoices_cached.clear()
    _load_bank_transactions_cached.clear()
    _load_reconciliation_cached.clear()

    # Clear session state
    for key in ['data_loaded_from_db', 'data_loaded_at',
                'invoices_in_processed', 'invoices_out_processed',
                'bank_statements_processed', 'reconciliation_ar_processed',
                'reconciliation_ap_processed']:
        if key in st.session_state:
            del st.session_state[key]


def get_data_stats() -> Dict[str, any]:
    """
    Get statistics about loaded data.

    Returns:
        Dictionary with:
        - 'is_loaded': bool
        - 'loaded_at': datetime or None
        - 'invoices_in_count': int
        - 'invoices_out_count': int
        - 'bank_statements_count': int
        - 'total_records': int
    """
    is_loaded = st.session_state.get('data_loaded_from_db', False)
    loaded_at = st.session_state.get('data_loaded_at')

    invoices_in = st.session_state.get('invoices_in_processed')
    invoices_out = st.session_state.get('invoices_out_processed')
    bank_statements = st.session_state.get('bank_statements_processed')

    invoices_in_count = len(invoices_in) if invoices_in is not None else 0
    invoices_out_count = len(invoices_out) if invoices_out is not None else 0
    bank_statements_count = len(bank_statements) if bank_statements is not None else 0

    return {
        'is_loaded': is_loaded,
        'loaded_at': loaded_at,
        'invoices_in_count': invoices_in_count,
        'invoices_out_count': invoices_out_count,
        'bank_statements_count': bank_statements_count,
        'total_records': invoices_in_count + invoices_out_count + bank_statements_count
    }
