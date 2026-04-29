"""
Session-based data loader for no-DB mode.
Drop-in replacement for utils/integration_loader.py and invoice-loading
functions from utils/db_operations.py.

All uploaded data lives in st.session_state for the duration of the session.
Keys used:
  - invoices_in_processed   : IN invoices (purchases)
  - invoices_out_processed  : OUT invoices (sales)
  - bank_statements_processed : bank transactions
  - reconciliation_ar_processed
  - reconciliation_ap_processed
  - invoice_items_processed  : line-item detail (optional)
"""
import pandas as pd
from typing import Optional, Tuple
import streamlit as st
from datetime import date, timedelta

# ─── Column normalisation maps ────────────────────────────────────────────────

# Every known Russian/English variant → canonical English name
_TO_ENGLISH: dict = {
    'Номер документ': 'Document Number',
    'Номер документа': 'Document Number',
    'номер документа': 'Document Number',
    'Дата документ': 'Document Date',
    'Дата документа': 'Document Date',
    'дата документа': 'Document Date',
    'Продавец (ИНН или ПИНФЛ)': 'Seller (Tax ID or PINFL)',
    'Продавец (ИНН/ПИНФЛ)': 'Seller (Tax ID or PINFL)',
    'Продавец(ИНН/ПИНФЛ)': 'Seller (Tax ID or PINFL)',
    'Продавец (ИНН)': 'Seller (Tax ID or PINFL)',
    'Продавец (наименование)': 'Seller (Name)',
    'Продавец(наименование)': 'Seller (Name)',
    'Покупатель (ИНН или ПИНФЛ)': 'Buyer (Tax ID or PINFL)',
    'Покупатель (ИНН/ПИНФЛ)': 'Buyer (Tax ID or PINFL)',
    'Покупатель(ИНН/ПИНФЛ)': 'Buyer (Tax ID or PINFL)',
    'Покупатель (ИНН)': 'Buyer (Tax ID or PINFL)',
    'Покупатель (наименование)': 'Buyer (Name)',
    'Покупатель(наименование)': 'Buyer (Name)',
    'Стоимость поставки': 'Supply Value',
    'НДС сумма': 'VAT Amount',
    'НДС': 'VAT Amount',
    'Стоимость поставки с учётом НДС': 'Supply Value (incl. VAT)',
    'Сумма с НДС': 'Supply Value (incl. VAT)',
    'Сумма к оплате': 'Supply Value (incl. VAT)',
    'СТАТУС': 'Status',
    'Статус': 'Status',
    'Договор номер': 'Contract Number',
    'Номер договора': 'Contract Number',
    'Договор дата': 'Contract Date',
    'Дата договора': 'Contract Date',
    'Примечание к товару (работе, услуге)': 'Примечание к товару (работе, услуге)',
}

# Canonical English name → raw column name used by dashboard.py / insights_engine.py
_TO_RAW: dict = {
    'Document Number': 'factura_no',
    'Document Date': 'factura_date',
    'Seller (Tax ID or PINFL)': 'seller_tin',
    'Seller (Name)': 'seller_name',
    'Buyer (Tax ID or PINFL)': 'buyer_tin',
    'Buyer (Name)': 'buyer_name',
    'Supply Value': 'delivery_sum',
    'VAT Amount': 'vat_sum',
    'Supply Value (incl. VAT)': 'delivery_sum_with_vat',
    'Status': 'status',
    'Contract Number': 'contract_no',
    'Contract Date': 'contract_date',
}


def normalize_to_english(df: pd.DataFrame) -> pd.DataFrame:
    """Rename all known Russian column variants to standard English names.
    Each target name is only used once — first matching source wins."""
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    rename = {}
    claimed = set(df.columns)
    for src, tgt in _TO_ENGLISH.items():
        if src in df.columns and tgt not in claimed:
            rename[src] = tgt
            claimed.add(tgt)
    df = df.rename(columns=rename) if rename else df.copy()
    # Drop any duplicate column names that may have slipped through
    df = df.loc[:, ~df.columns.duplicated(keep='first')]
    return df


def store_invoices(df: pd.DataFrame, invoice_type: str) -> None:
    """Normalise and store uploaded invoices in session_state."""
    df = normalize_to_english(df.copy())
    if 'invoice_type' not in df.columns:
        df['invoice_type'] = invoice_type
    key = 'invoices_in_processed' if invoice_type == 'IN' else 'invoices_out_processed'
    existing = st.session_state.get(key)
    if existing is not None and not existing.empty:
        df = pd.concat([existing, df], ignore_index=True)
        dup_cols = [c for c in ['Document Number', 'Document Date'] if c in df.columns]
        if dup_cols:
            df = df.drop_duplicates(subset=dup_cols, keep='last')
    st.session_state[key] = df


def store_bank_transactions(df: pd.DataFrame) -> None:
    """Store uploaded bank transactions in session_state."""
    existing = st.session_state.get('bank_statements_processed')
    if existing is not None and not existing.empty:
        df = pd.concat([existing, df], ignore_index=True)
        dup_cols = [c for c in ['date', 'amount', 'Payment Purpose'] if c in df.columns]
        if dup_cols:
            df = df.drop_duplicates(subset=dup_cols, keep='last')
    st.session_state['bank_statements_processed'] = df


def store_reconciliation(df: pd.DataFrame, rec_type: str) -> None:
    """Store reconciliation data in session_state."""
    key = 'reconciliation_ar_processed' if rec_type in ('AR', 'IN') else 'reconciliation_ap_processed'
    st.session_state[key] = df


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _get_session_invoices(invoice_type: Optional[str] = None) -> pd.DataFrame:
    """Return all uploaded invoices, combining IN and/or OUT based on invoice_type."""
    frames = []
    if invoice_type in ('IN', None):
        df = st.session_state.get('invoices_in_processed')
        if df is not None and not df.empty:
            f = normalize_to_english(df.copy())
            if 'invoice_type' not in f.columns:
                f['invoice_type'] = 'IN'
            frames.append(f)
    if invoice_type in ('OUT', None):
        df = st.session_state.get('invoices_out_processed')
        if df is not None and not df.empty:
            f = normalize_to_english(df.copy())
            if 'invoice_type' not in f.columns:
                f['invoice_type'] = 'OUT'
            frames.append(f)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _filter_by_tin(df: pd.DataFrame, tin: str, invoice_type: Optional[str]) -> pd.DataFrame:
    """
    Filter rows relevant to a specific company TIN.
    - OUT: company is the seller  → keep rows where Seller TIN == tin
    - IN:  company is the buyer   → keep rows where Buyer TIN == tin
    - None: either role           → union of both
    """
    if df.empty or not tin or tin == 'SESSION':
        return df
    s_col = 'Seller (Tax ID or PINFL)'
    b_col = 'Buyer (Tax ID or PINFL)'
    tin_str = str(tin)
    if invoice_type == 'OUT':
        if s_col in df.columns:
            return df[df[s_col].astype(str) == tin_str]
    elif invoice_type == 'IN':
        if b_col in df.columns:
            return df[df[b_col].astype(str) == tin_str]
    else:
        mask = pd.Series(False, index=df.index)
        if s_col in df.columns:
            mask |= df[s_col].astype(str) == tin_str
        if b_col in df.columns:
            mask |= df[b_col].astype(str) == tin_str
        return df[mask]
    return df


# ─── Public API: invoice loaders ─────────────────────────────────────────────

def load_user_invoices(user_id, invoice_type: Optional[str] = None) -> pd.DataFrame:
    """Return invoices in standard English format from session_state."""
    return _get_session_invoices(invoice_type)


def load_integration_invoices(user_id, invoice_type: Optional[str] = None) -> pd.DataFrame:
    tin = get_user_company_tin(user_id)
    df = _get_session_invoices(invoice_type)
    return _filter_by_tin(df, tin, invoice_type)


def load_integration_invoices_by_tin(tin: str, invoice_type: Optional[str] = None) -> pd.DataFrame:
    df = _get_session_invoices(invoice_type)
    return _filter_by_tin(df, tin, invoice_type)


def load_raw_invoices(tin: str, invoice_type: Optional[str] = None) -> pd.DataFrame:
    """Return invoices in raw format (factura_no, factura_date, …) for dashboard.py."""
    df = _get_session_invoices(invoice_type)
    df = _filter_by_tin(df, tin, invoice_type)
    if df.empty:
        return df
    rename = {k: v for k, v in _TO_RAW.items() if k in df.columns}
    df = df.rename(columns=rename)
    for col in ['id', 'factoring_status', 'factoring_request_id', 'summa']:
        if col not in df.columns:
            df[col] = None
    return df


def load_integration_items_by_tin(tin: str, invoice_type: str = 'OUT',
                                   start_date=None, end_date=None) -> pd.DataFrame:
    """Return invoice line items from session_state."""
    df = st.session_state.get('invoice_items_processed')
    if df is None or df.empty:
        return pd.DataFrame()
    if invoice_type and 'invoice_type' in df.columns:
        df = df[df['invoice_type'] == invoice_type].copy()
    date_col = next((c for c in ['Document Date', 'factura_date'] if c in df.columns), None)
    if date_col:
        dates = pd.to_datetime(df[date_col], errors='coerce')
        if start_date is not None:
            df = df[dates >= pd.Timestamp(start_date)]
        if end_date is not None:
            df = df[dates <= pd.Timestamp(end_date)]
    return df


# ─── Bank transactions ────────────────────────────────────────────────────────

def load_user_bank_transactions(user_id) -> pd.DataFrame:
    df = st.session_state.get('bank_statements_processed')
    return df if df is not None else pd.DataFrame()


# ─── Company helpers ──────────────────────────────────────────────────────────

def get_user_company_tin(user_id) -> Optional[str]:
    """Derive company TIN from session state or uploaded data."""
    selected = st.session_state.get('selected_company_tin')
    if selected:
        return selected
    # Derive from OUT invoices (we are the seller)
    df_out = st.session_state.get('invoices_out_processed')
    if df_out is not None and not df_out.empty:
        df_out = normalize_to_english(df_out)
        col = 'Seller (Tax ID or PINFL)'
        if col in df_out.columns:
            vals = df_out[col].dropna().unique()
            if len(vals) >= 1:
                return str(vals[0])
    # Derive from IN invoices (we are the buyer)
    df_in = st.session_state.get('invoices_in_processed')
    if df_in is not None and not df_in.empty:
        df_in = normalize_to_english(df_in)
        col = 'Buyer (Tax ID or PINFL)'
        if col in df_in.columns:
            vals = df_in[col].dropna().unique()
            if len(vals) >= 1:
                return str(vals[0])
    return "SESSION"


def get_all_companies(min_invoices: int = 1) -> pd.DataFrame:
    """
    Return only companies we have data FOR (own-company perspective).
    - Unique seller TINs from OUT invoices  (we are the seller)
    - Unique buyer TINs from IN invoices    (we are the buyer)
    These are the only TINs where switching the selector makes sense.
    """
    rows = []
    df_out = st.session_state.get('invoices_out_processed')
    if df_out is not None and not df_out.empty:
        df_out = normalize_to_english(df_out)
        s_col, n_col = 'Seller (Tax ID or PINFL)', 'Seller (Name)'
        if s_col in df_out.columns:
            grp = (df_out.groupby(s_col)
                   .agg(name=(n_col, 'first') if n_col in df_out.columns else (s_col, 'first'),
                        total_invoices=(s_col, 'count'))
                   .reset_index()
                   .rename(columns={s_col: 'tin'}))
            rows.append(grp)

    df_in = st.session_state.get('invoices_in_processed')
    if df_in is not None and not df_in.empty:
        df_in = normalize_to_english(df_in)
        b_col, n_col = 'Buyer (Tax ID or PINFL)', 'Buyer (Name)'
        if b_col in df_in.columns:
            grp = (df_in.groupby(b_col)
                   .agg(name=(n_col, 'first') if n_col in df_in.columns else (b_col, 'first'),
                        total_invoices=(b_col, 'count'))
                   .reset_index()
                   .rename(columns={b_col: 'tin'}))
            rows.append(grp)

    if not rows:
        return pd.DataFrame(columns=['tin', 'name', 'total_invoices', 'total_volume'])
    result = (pd.concat(rows)
              .groupby('tin')
              .agg(name=('name', 'first'), total_invoices=('total_invoices', 'sum'))
              .reset_index())
    result['total_volume'] = 0
    return result[result['total_invoices'] >= min_invoices].sort_values('total_invoices', ascending=False)


def get_company_name(tin: str) -> str:
    """Derive company name from uploaded data."""
    df = _get_session_invoices()
    if df.empty or not tin:
        return tin or 'Company'
    for tin_col, name_col in [
        ('Seller (Tax ID or PINFL)', 'Seller (Name)'),
        ('Buyer (Tax ID or PINFL)', 'Buyer (Name)'),
    ]:
        if tin_col in df.columns and name_col in df.columns:
            match = df[df[tin_col].astype(str) == str(tin)][name_col].dropna()
            if not match.empty:
                return str(match.iloc[0])
    return tin


# ─── Risk analysis helpers ────────────────────────────────────────────────────

def calculate_counterparty_lookback_period(user_id: str, counterparty_inn: str,
                                            invoice_type: str = 'OUT',
                                            as_of_date=None) -> int:
    """Return lookback period in months based on available session data."""
    df = _get_session_invoices(invoice_type)
    if df.empty:
        return 12
    date_col = 'Document Date'
    if date_col not in df.columns:
        return 12
    dates = pd.to_datetime(df[date_col], errors='coerce').dropna()
    if dates.empty:
        return 12
    months = max(1, int((dates.max() - dates.min()).days / 30))
    return min(months, 36)


def get_all_invoices_and_payments(
    user_id: str,
    invoice_type: str = 'OUT',
    months_back: int = 24,
    as_of_date=None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Return (inv_df, pay_df) from session data.
    inv_df columns mirror what risk_queries.get_all_invoices_and_payments returns.
    pay_df is empty if no bank data uploaded.
    """
    df = _get_session_invoices(invoice_type)

    # Convert to the snake_case format expected by risk_engine
    col_map = {
        'Document Number': 'document_number',
        'Document Date': 'document_date',
        'Seller (Tax ID or PINFL)': 'seller_inn',
        'Seller (Name)': 'seller_name',
        'Buyer (Tax ID or PINFL)': 'buyer_inn',
        'Buyer (Name)': 'buyer_name',
        'Supply Value': 'supply_value',
        'VAT Amount': 'vat_amount',
        'Supply Value (incl. VAT)': 'total_amount',
        'Status': 'status',
        'Contract Number': 'contract_number',
        'Contract Date': 'contract_date',
    }
    inv_df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    if 'invoice_id' not in inv_df.columns:
        inv_df['invoice_id'] = range(len(inv_df))

    # Bank payments
    pay_df = st.session_state.get('bank_statements_processed')
    if pay_df is None:
        pay_df = pd.DataFrame()

    return inv_df, pay_df
