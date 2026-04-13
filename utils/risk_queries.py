"""
Risk Engine Database Query Layer

Optimized SQL queries for fetching data required by the risk engine.
Uses existing database schema (invoices, bank_transactions, invoice_items).
"""

import pandas as pd
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Tuple
from utils.db_operations import get_db_engine


def get_counterparty_invoices(
    user_id: str,
    counterparty_inn: str,
    invoice_type: str = 'OUT',
    months_back: int = 12,
    as_of_date: Optional[date] = None
) -> pd.DataFrame:
    """
    Get all invoices for a specific counterparty.
    
    Args:
        user_id: User/company identifier
        counterparty_inn: Counterparty INN
        invoice_type: 'OUT' for AR (receivables), 'IN' for AP (payables)
        months_back: Number of months to look back
        as_of_date: Cutoff date (defaults to today)
        
    Returns:
        DataFrame with invoice data
    """
    if as_of_date is None:
        as_of_date = date.today()

    # Calculate lookback date: 1 month ≈ 30.44 days (365.25/12)
    lookback_date = as_of_date - timedelta(days=int(months_back * 30.44))

    engine = get_db_engine()

    # Determine which INN column to filter on
    inn_column = 'buyer_inn' if invoice_type == 'OUT' else 'seller_inn'
    
    query = f"""
        SELECT 
            id as invoice_id,
            document_number,
            document_date,
            invoice_type,
            seller_inn,
            seller_name,
            buyer_inn,
            buyer_name,
            total_amount,
            supply_value,
            vat_amount,
            status,
            contract_number,
            contract_date,
            created_at,
            updated_at
        FROM invoices
        WHERE user_id = %(user_id)s
          AND invoice_type = %(invoice_type)s
          AND {inn_column} = %(counterparty_inn)s
          AND document_date >= %(lookback_date)s
          AND document_date <= %(as_of_date)s
        ORDER BY document_date ASC
    """
    
    params = {
        'user_id': str(user_id),
        'invoice_type': invoice_type,
        'counterparty_inn': str(counterparty_inn),
        'lookback_date': lookback_date,
        'as_of_date': as_of_date
    }
    
    try:
        df = pd.read_sql_query(query, engine, params=params)
        return df
    except Exception as e:
        print(f"Error fetching counterparty invoices: {e}")
        return pd.DataFrame()


def get_counterparty_payments(
    user_id: str,
    counterparty_inn: str,
    transaction_type: str = 'Incoming',
    months_back: int = 12,
    as_of_date: Optional[date] = None
) -> pd.DataFrame:
    """
    Get all bank transactions (payments) for a specific counterparty.
    
    Args:
        user_id: User/company identifier
        counterparty_inn: Counterparty INN
        transaction_type: 'Incoming' for receipts, 'Outgoing' for payments
        months_back: Number of months to look back
        as_of_date: Cutoff date (defaults to today)
        
    Returns:
        DataFrame with payment data
    """
    if as_of_date is None:
        as_of_date = date.today()

    # Calculate lookback date: 1 month ≈ 30.44 days (365.25/12)
    lookback_date = as_of_date - timedelta(days=int(months_back * 30.44))

    engine = get_db_engine()

    query = """
        SELECT
            id as payment_id,
            transaction_date,
            document_number,
            document_date,
            counterparty_inn,
            counterparty_name,
            amount,
            debit_amount,
            credit_amount,
            transaction_type,
            payment_purpose,
            contract_number,
            created_at
        FROM bank_transactions
        WHERE user_id = %(user_id)s
          AND counterparty_inn = %(counterparty_inn)s
          AND transaction_type = %(transaction_type)s
          AND transaction_date >= %(lookback_date)s
          AND transaction_date <= %(as_of_date)s
        ORDER BY transaction_date ASC
    """
    
    params = {
        'user_id': str(user_id),
        'counterparty_inn': str(counterparty_inn),
        'transaction_type': transaction_type,
        'lookback_date': lookback_date,
        'as_of_date': as_of_date
    }
    
    try:
        df = pd.read_sql_query(query, engine, params=params)
        return df
    except Exception as e:
        print(f"Error fetching counterparty payments: {e}")
        return pd.DataFrame()


def get_all_counterparties(
    user_id: str,
    invoice_type: str = 'OUT',
    months_back: int = 12,
    as_of_date: Optional[date] = None
) -> pd.DataFrame:
    """
    Get list of all unique counterparties with basic statistics.
    
    Args:
        user_id: User/company identifier
        invoice_type: 'OUT' for customers, 'IN' for suppliers
        months_back: Number of months to look back
        as_of_date: Cutoff date (defaults to today)
        
    Returns:
        DataFrame with counterparty summary data
    """
    if as_of_date is None:
        as_of_date = date.today()

    # Calculate lookback date: 1 month ≈ 30.44 days (365.25/12)
    lookback_date = as_of_date - timedelta(days=int(months_back * 30.44))

    engine = get_db_engine()

    # Determine which columns to use
    if invoice_type == 'OUT':
        inn_column = 'buyer_inn'
        name_column = 'buyer_name'
    else:
        inn_column = 'seller_inn'
        name_column = 'seller_name'
    
    query = f"""
        SELECT 
            {inn_column} as counterparty_inn,
            MAX({name_column}) as counterparty_name,
            COUNT(*) as invoice_count,
            SUM(total_amount) as total_invoiced,
            MIN(document_date) as first_invoice_date,
            MAX(document_date) as last_invoice_date,
            AVG(total_amount) as avg_invoice_amount
        FROM invoices
        WHERE user_id = %(user_id)s
          AND invoice_type = %(invoice_type)s
          AND (
              status = 'Подписан'
              OR document_number ILIKE '%%/возврат%%'
              OR document_number ILIKE '%%/return%%'
          )
          AND document_date >= %(lookback_date)s
          AND document_date <= %(as_of_date)s
          AND {inn_column} IS NOT NULL
          AND {inn_column} != ''
        GROUP BY {inn_column}
        HAVING COUNT(*) > 0
        ORDER BY total_invoiced DESC
    """
    
    params = {
        'user_id': str(user_id),
        'invoice_type': invoice_type,
        'lookback_date': lookback_date,
        'as_of_date': as_of_date
    }
    
    try:
        df = pd.read_sql_query(query, engine, params=params)
        return df
    except Exception as e:
        print(f"Error fetching counterparties: {e}")
        return pd.DataFrame()


def get_invoice_details(
    user_id: str,
    invoice_ids: List[int]
) -> pd.DataFrame:
    """
    Get detailed invoice information including line items.
    
    Args:
        user_id: User/company identifier
        invoice_ids: List of invoice IDs to fetch
        
    Returns:
        DataFrame with detailed invoice and item data
    """
    if not invoice_ids:
        return pd.DataFrame()
    
    engine = get_db_engine()
    
    # Convert list to SQL-compatible format
    invoice_ids_str = ','.join(str(id) for id in invoice_ids)
    
    query = f"""
        SELECT 
            i.id as invoice_id,
            i.document_number,
            i.document_date,
            i.invoice_type,
            i.seller_inn,
            i.seller_name,
            i.buyer_inn,
            i.buyer_name,
            i.total_amount,
            i.status,
            i.contract_number,
            ii.item_number,
            ii.item_note,
            ii.catalog_code,
            ii.quantity,
            ii.unit_of_measure,
            ii.unit_price,
            ii.supply_value as item_supply_value,
            ii.vat_amount as item_vat_amount,
            ii.total_amount as item_total_amount
        FROM invoices i
        LEFT JOIN invoice_items ii ON i.id = ii.invoice_id
        WHERE i.user_id = %(user_id)s
          AND i.id IN ({invoice_ids_str})
        ORDER BY i.document_date, ii.item_number
    """
    
    params = {
        'user_id': str(user_id)
    }
    
    try:
        df = pd.read_sql_query(query, engine, params=params)
        return df
    except Exception as e:
        print(f"Error fetching invoice details: {e}")
        return pd.DataFrame()


def get_invoices_with_payments(
    user_id: str,
    invoice_ids: Optional[List[int]] = None,
    document_numbers: Optional[List[str]] = None,
    document_numbers_no_status: Optional[List[str]] = None,
    contract_number: Optional[str] = None,
    invoice_type: str = 'OUT'
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch a specific set of invoices together with all related payments.

    Args:
        user_id: User/company identifier
        invoice_ids: Optional list of invoice primary keys
        document_numbers: Optional list of human-readable document numbers
        invoice_type: Invoice polarity ('OUT' receivables, 'IN' payables)

    Returns:
        Tuple of (invoices_df, payments_df) ordered chronologically
    """
    invoice_ids = invoice_ids or []
    document_numbers = document_numbers or []
    document_numbers_no_status = document_numbers_no_status or []

    engine = get_db_engine()
    conditions = []

    if invoice_ids:
        id_values = ','.join(str(i) for i in invoice_ids if str(i).strip().isdigit())
        if id_values:
            conditions.append(f"id IN ({id_values})")

    if document_numbers:
        doc_numbers_clean = [str(num).strip() for num in document_numbers if str(num).strip()]
        if doc_numbers_clean:
            doc_numbers_str = ','.join(f"'{num}'" for num in doc_numbers_clean)
            conditions.append(f"document_number IN ({doc_numbers_str})")

    if not conditions and document_numbers_no_status:
        doc_numbers_clean = [str(num).strip() for num in document_numbers_no_status if str(num).strip()]
        if doc_numbers_clean:
            doc_numbers_str = ','.join(f"'{num}'" for num in doc_numbers_clean)
            conditions.append(f"document_number IN ({doc_numbers_str})")

    filter_clause = ""
    if conditions:
        filter_clause = f" AND ({' OR '.join(conditions)})"
    status_clause = "status = 'Подписан'"
    if document_numbers_no_status:
        doc_no_status_clean = [str(num).strip() for num in document_numbers_no_status if str(num).strip()]
        if doc_no_status_clean:
            doc_no_status_str = ','.join(f"'{num}'" for num in doc_no_status_clean)
            status_clause = f"(status = 'Подписан' OR document_number IN ({doc_no_status_str}))"
    contract_clause = ""
    contract_params = {}
    if contract_number:
        contract_clause = " AND contract_number = %(contract_number)s"
        contract_params['contract_number'] = contract_number

    invoice_query = f"""
        SELECT 
            id as invoice_id,
            document_number,
            document_date,
            invoice_type,
            seller_inn,
            seller_name,
            buyer_inn,
            buyer_name,
            total_amount,
            supply_value,
            vat_amount,
            status,
            contract_number,
            contract_date
        FROM invoices
        WHERE user_id = %(user_id)s
          AND invoice_type = %(invoice_type)s
          AND {status_clause}
          {filter_clause}
          {contract_clause}
        ORDER BY document_date ASC
    """

    invoice_params = {
        'user_id': str(user_id),
        'invoice_type': invoice_type,
        **contract_params
    }

    try:
        invoices_df = pd.read_sql_query(invoice_query, engine, params=invoice_params)
    except Exception as e:
        print(f"Error fetching invoices {invoice_ids}: {e}")
        return pd.DataFrame(), pd.DataFrame()

    if invoices_df.empty:
        return invoices_df, pd.DataFrame()

    counterparty_column = 'buyer_inn' if invoice_type == 'OUT' else 'seller_inn'
    raw_inns = (
        invoices_df[counterparty_column]
        .dropna()
        .astype(str)
        .str.replace('.0', '', regex=False)
        .str.strip()
        .tolist()
    )

    unique_inns = sorted({inn for inn in raw_inns if inn})
    if not unique_inns:
        return invoices_df, pd.DataFrame()

    counterparty_inns_str = ','.join(f"'{inn}'" for inn in unique_inns)
    payment_type = 'Incoming' if invoice_type == 'OUT' else 'Outgoing'
    start_date = invoices_df['document_date'].min()

    payment_query = f"""
        SELECT 
            id as payment_id,
            transaction_date,
            document_number,
            counterparty_inn,
            counterparty_name,
            amount,
            debit_amount,
            credit_amount,
            transaction_type,
            payment_purpose,
            contract_number
        FROM bank_transactions
        WHERE user_id = %(user_id)s
          AND transaction_type = %(payment_type)s
          AND REPLACE(counterparty_inn, '.0', '') IN ({counterparty_inns_str})
          AND transaction_date >= %(start_date)s
        ORDER BY transaction_date ASC
    """

    payment_params = {
        'user_id': str(user_id),
        'payment_type': payment_type,
        'start_date': start_date
    }

    try:
        payments_df = pd.read_sql_query(payment_query, engine, params=payment_params)
    except Exception as e:
        print(f"Error fetching payments for invoices {invoice_ids}: {e}")
        payments_df = pd.DataFrame()

    return invoices_df, payments_df

def get_all_invoices_and_payments(
    user_id: str,
    invoice_type: str = 'OUT',
    months_back: int = 24,
    as_of_date: Optional[date] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Get all invoices and corresponding payments for risk analysis.

    Args:
        user_id: User/company identifier
        invoice_type: 'OUT' for AR, 'IN' for AP
        months_back: Number of months to look back
        as_of_date: Cutoff date (defaults to today)

    Returns:
        Tuple of (invoices_df, payments_df)
    """
    if as_of_date is None:
        as_of_date = date.today()

    # Calculate lookback date using proper month arithmetic
    # Approximate: 1 month = 30.44 days (365.25/12) for more accurate calculation
    lookback_date = as_of_date - timedelta(days=int(months_back * 30.44))

    engine = get_db_engine()
    
    # Fetch all invoices - signed invoices + returns (returns may have different status)
    invoice_query = """
        SELECT 
            id as invoice_id,
            document_number,
            document_date,
            invoice_type,
            seller_inn,
            seller_name,
            buyer_inn,
            buyer_name,
            total_amount,
            supply_value,
            vat_amount,
            status,
            contract_number
        FROM invoices
        WHERE user_id = %(user_id)s
          AND invoice_type = %(invoice_type)s
          AND (
              status = 'Подписан'
              OR document_number ILIKE '%%/возврат%%'
              OR document_number ILIKE '%%/return%%'
              OR document_number ILIKE '%%возврат%%'
              OR document_number ILIKE '%%return%%'
          )
          AND document_date >= %(lookback_date)s
          AND document_date <= %(as_of_date)s
        ORDER BY document_date ASC
    """
    
    payment_type = 'Incoming' if invoice_type == 'OUT' else 'Outgoing'
    
    payment_query = """
        SELECT 
            id as payment_id,
            transaction_date,
            document_number,
            counterparty_inn,
            counterparty_name,
            amount,
            transaction_type,
            payment_purpose,
            contract_number
        FROM bank_transactions
        WHERE user_id = %(user_id)s
          AND transaction_type = %(payment_type)s
          AND transaction_date >= %(lookback_date)s
          AND transaction_date <= %(as_of_date)s
        ORDER BY transaction_date ASC
    """
    
    params = {
        'user_id': str(user_id),
        'invoice_type': invoice_type,
        'lookback_date': lookback_date,
        'as_of_date': as_of_date
    }
    
    payment_params = {
        'user_id': str(user_id),
        'payment_type': payment_type,
        'lookback_date': lookback_date,
        'as_of_date': as_of_date
    }
    
    try:
        invoices_df = pd.read_sql_query(invoice_query, engine, params=params)
        payments_df = pd.read_sql_query(payment_query, engine, params=payment_params)
        return invoices_df, payments_df
    except Exception as e:
        print(f"Error fetching invoices and payments: {e}")
        return pd.DataFrame(), pd.DataFrame()


def get_portfolio_summary(
    user_id: str,
    invoice_type: str = 'OUT',
    as_of_date: Optional[date] = None
) -> Dict[str, Any]:
    """
    Get portfolio-level summary statistics.
    
    Args:
        user_id: User/company identifier
        invoice_type: 'OUT' for AR, 'IN' for AP
        as_of_date: Cutoff date (defaults to today)
        
    Returns:
        Dictionary with portfolio metrics
    """
    if as_of_date is None:
        as_of_date = date.today()
    
    engine = get_db_engine()
    
    # Determine column names
    if invoice_type == 'OUT':
        counterparty_column = 'buyer_inn'
    else:
        counterparty_column = 'seller_inn'
    
    query = f"""
        SELECT 
            COUNT(DISTINCT {counterparty_column}) as unique_counterparties,
            COUNT(*) as total_invoices,
            SUM(total_amount) as total_exposure,
            AVG(total_amount) as avg_invoice_amount,
            MIN(document_date) as earliest_invoice,
            MAX(document_date) as latest_invoice
        FROM invoices
        WHERE user_id = %(user_id)s
          AND invoice_type = %(invoice_type)s
          AND (
              status = 'Подписан'
              OR document_number ILIKE '%%/возврат%%'
              OR document_number ILIKE '%%/return%%'
          )
          AND document_date <= %(as_of_date)s
    """
    
    params = {
        'user_id': str(user_id),
        'invoice_type': invoice_type,
        'as_of_date': as_of_date
    }
    
    try:
        df = pd.read_sql_query(query, engine, params=params)
        if not df.empty:
            return df.iloc[0].to_dict()
        return {}
    except Exception as e:
        print(f"Error fetching portfolio summary: {e}")
        return {}


def get_aging_analysis(
    user_id: str,
    invoice_type: str = 'OUT',
    as_of_date: Optional[date] = None
) -> pd.DataFrame:
    """
    Get aging analysis of unpaid invoices.
    Note: This is a simplified version - actual payment matching would require
    the risk engine's FIFO allocation logic.
    
    Args:
        user_id: User/company identifier
        invoice_type: 'OUT' for AR, 'IN' for AP
        as_of_date: Cutoff date (defaults to today)
        
    Returns:
        DataFrame with aging buckets
    """
    if as_of_date is None:
        as_of_date = date.today()
    
    engine = get_db_engine()
    
    # Determine column names
    if invoice_type == 'OUT':
        counterparty_column = 'buyer_inn'
        counterparty_name_column = 'buyer_name'
    else:
        counterparty_column = 'seller_inn'
        counterparty_name_column = 'seller_name'
    
    query = f"""
        SELECT 
            {counterparty_column} as counterparty_inn,
            MAX({counterparty_name_column}) as counterparty_name,
            COUNT(*) as invoice_count,
            SUM(total_amount) as total_amount,
            SUM(CASE WHEN %(as_of_date)s::date - document_date <= 30 THEN total_amount ELSE 0 END) as amount_0_30,
            SUM(CASE WHEN %(as_of_date)s::date - document_date BETWEEN 31 AND 60 THEN total_amount ELSE 0 END) as amount_31_60,
            SUM(CASE WHEN %(as_of_date)s::date - document_date BETWEEN 61 AND 90 THEN total_amount ELSE 0 END) as amount_61_90,
            SUM(CASE WHEN %(as_of_date)s::date - document_date BETWEEN 91 AND 180 THEN total_amount ELSE 0 END) as amount_91_180,
            SUM(CASE WHEN %(as_of_date)s::date - document_date > 180 THEN total_amount ELSE 0 END) as amount_180_plus
        FROM invoices
        WHERE user_id = %(user_id)s
          AND invoice_type = %(invoice_type)s
          AND (
              status = 'Подписан'
              OR document_number ILIKE '%%/возврат%%'
              OR document_number ILIKE '%%/return%%'
          )
          AND document_date <= %(as_of_date)s
          AND {counterparty_column} IS NOT NULL
        GROUP BY {counterparty_column}
        ORDER BY total_amount DESC
    """
    
    params = {
        'user_id': str(user_id),
        'invoice_type': invoice_type,
        'as_of_date': as_of_date
    }
    
    try:
        df = pd.read_sql_query(query, engine, params=params)
        return df
    except Exception as e:
        print(f"Error fetching aging analysis: {e}")
        return pd.DataFrame()


def search_counterparties(
    user_id: str,
    search_term: str,
    invoice_type: str = 'OUT'
) -> pd.DataFrame:
    """
    Search for counterparties by INN or name.
    
    Args:
        user_id: User/company identifier
        search_term: Search string (INN or name fragment)
        invoice_type: 'OUT' for customers, 'IN' for suppliers
        
    Returns:
        DataFrame with matching counterparties
    """
    engine = get_db_engine()
    
    # Determine columns
    if invoice_type == 'OUT':
        inn_column = 'buyer_inn'
        name_column = 'buyer_name'
    else:
        inn_column = 'seller_inn'
        name_column = 'seller_name'
    
    query = f"""
        SELECT DISTINCT
            {inn_column} as counterparty_inn,
            MAX({name_column}) as counterparty_name,
            COUNT(*) as invoice_count,
            SUM(total_amount) as total_amount
        FROM invoices
        WHERE user_id = %(user_id)s
          AND invoice_type = %(invoice_type)s
          AND (
              status = 'Подписан'
              OR document_number ILIKE '%%/возврат%%'
              OR document_number ILIKE '%%/return%%'
          )
          AND (
              {inn_column} ILIKE %(search_pattern)s
              OR {name_column} ILIKE %(search_pattern)s
          )
          AND {inn_column} IS NOT NULL
          AND {inn_column} != ''
        GROUP BY {inn_column}
        ORDER BY total_amount DESC
        LIMIT 50
    """
    
    params = {
        'user_id': str(user_id),
        'invoice_type': invoice_type,
        'search_pattern': f'%{search_term}%'
    }
    
    try:
        df = pd.read_sql_query(query, engine, params=params)
        # Clean up INN values - remove .0 suffix if present
        if not df.empty and 'counterparty_inn' in df.columns:
            df['counterparty_inn'] = df['counterparty_inn'].astype(str).str.replace('.0', '', regex=False).str.strip()
        return df
    except Exception as e:
        print(f"Error searching counterparties: {e}")
        return pd.DataFrame()


def calculate_counterparty_lookback_period(
    user_id: str,
    counterparty_inn: str,
    invoice_type: str = 'OUT',
    as_of_date: Optional[date] = None
) -> int:
    """
    Calculate lookback period for a counterparty based on:
    - First payment in bank_transactions
    - Latest invoice issued (document_date)
    - Months from today to latest invoice date
    
    Args:
        user_id: User/company identifier
        counterparty_inn: Counterparty INN
        invoice_type: 'OUT' for AR, 'IN' for AP
        as_of_date: Analysis date (defaults to today)
        
    Returns:
        Number of months as lookback period (minimum 3, maximum 36)
    """
    if as_of_date is None:
        as_of_date = date.today()
    
    engine = get_db_engine()
    
    # Determine which INN column to use for invoices
    inn_column = 'buyer_inn' if invoice_type == 'OUT' else 'seller_inn'
    
    # Determine payment type
    payment_type = 'Incoming' if invoice_type == 'OUT' else 'Outgoing'

    # Ensure counterparty INN matches DB formatting (with .0 suffix when needed)
    counterparty_value = str(counterparty_inn).strip() if counterparty_inn is not None else ''
    if counterparty_value and '.' not in counterparty_value:
        counterparty_value = f"{counterparty_value}.0"
    
    try:
        # Query 1: Get first payment date
        payment_query = """
            SELECT 
                MIN(transaction_date) as first_payment_date
            FROM bank_transactions
            WHERE user_id = %(user_id)s
              AND counterparty_inn = %(counterparty_inn)s
              AND transaction_type = %(payment_type)s
        """
        payment_params = {
            'user_id': str(user_id),
            'counterparty_inn': counterparty_inn,
            'payment_type': payment_type
        }
        print("Payment Params:", payment_params)
        payment_df = pd.read_sql_query(payment_query, engine, params=payment_params)
        first_payment_date = payment_df.iloc[0]['first_payment_date'] if not payment_df.empty and payment_df.iloc[0]['first_payment_date'] is not None else None
        
        # Query 2: Get latest invoice date (no returns)
        # Ensure first_payment_date is a string in 'YYYY-MM-DD' format for SQL compatibility
        if first_payment_date is not None and isinstance(first_payment_date, (pd.Timestamp, datetime)):
            first_payment_date_str = first_payment_date.strftime('%Y-%m-%d')
        elif first_payment_date is not None and isinstance(first_payment_date, date):
            first_payment_date_str = first_payment_date.isoformat()
        else:
            first_payment_date_str = first_payment_date  # None or already str

        invoice_query = f"""
            SELECT 
                MAX(document_date) as latest_invoice_date
            FROM invoices
            WHERE user_id = %(user_id)s
              AND invoice_type = %(invoice_type)s
              AND {inn_column} = %(counterparty_inn)s
              AND status = 'Подписан'
              AND document_date <= %(first_payment_date)s
        """

        invoice_params = {
            'user_id': str(user_id),
            'counterparty_inn': counterparty_value,
            'invoice_type': invoice_type,
            'first_payment_date': first_payment_date_str
        }
        print("Invoice Params:", invoice_params)
        
        invoice_df = pd.read_sql_query(invoice_query, engine, params=invoice_params)
        if invoice_df.empty or invoice_df.iloc[0]['latest_invoice_date'] is None:
            # Default to 12 months if no data found
            return 12
        
        latest_invoice_date = invoice_df.iloc[0]['latest_invoice_date']
        print("Latest Invoice Date:", latest_invoice_date)
        print("As of Date:", as_of_date)
        
        # Convert to date if needed
        if isinstance(latest_invoice_date, pd.Timestamp):
            latest_invoice_date = latest_invoice_date.date()
        elif isinstance(latest_invoice_date, datetime):
            latest_invoice_date = latest_invoice_date.date()
        
        # Calculate months difference: (year_diff * 12) + month_diff
        # Add 1 month buffer to ensure we capture the invoice
        year_diff = as_of_date.year - latest_invoice_date.year
        month_diff = as_of_date.month - latest_invoice_date.month
        total_months = (year_diff * 12) + month_diff + 1
        
        # Clamp between 3 and 36 months
        lookback_period = max(3, min(36, total_months))
        print("Lookback Period:", lookback_period)
        
        return lookback_period
        
    except Exception as e:
        print(f"Error calculating lookback period for {counterparty_inn}: {e}")
        # Default to 12 months on error
        return 12

