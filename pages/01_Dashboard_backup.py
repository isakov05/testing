import streamlit as st
import pandas as pd
import datetime
from datetime import datetime as dt
import plotly.express as px  # pyright: ignore[reportMissingImports]
import plotly.graph_objects as go  # pyright: ignore[reportMissingImports]

from auth.db_authenticator import protect_page
import re
import os
import json
from translations import get_text

st.set_page_config(
    page_title="FLOTT Dashboard",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Protect this page - require authentication
protect_page()


def find_amount_column(df):
    """Find the appropriate amount column from invoice data"""
    # Priority order for amount columns
    amount_columns = [
        'Цена',  # Prefer net price column (Russian)
        'Supply Value (incl. VAT)',  # Database column - total with VAT
        'Supply Value',  # Database column - net value
        'Total Amount',  # Alternative database column name
        'amount', 'Amount', 'Сумма', 'Стоимость', 'Cost', 'Total',
        'Стоимость поставки с учётом НДС'  # VAT-included (fallback if nothing else)
    ]

    for col in amount_columns:
        if col in df.columns:
            return col

    return None


def filter_signed_invoices(df: pd.DataFrame) -> pd.DataFrame:
    """Return only rows that look like signed invoices.

    Tries common status column names and values across languages.
    Falls back to substring match if exact labels are unknown.
    """
    if df is None or df.empty:
        return df

    status_columns = [
        'СТАТУС', 'Статус', 'Status', 'status', 'Document Status', 'doc_status', 'invoice_status'
    ]
    signed_labels = {
        'подписан',  # ru
        'signed',    # en
    }

    # Highest priority: exact match for Cyrillic 'СТАТУС' == 'Подписан'
    if 'СТАТУС' in df.columns:
        col_series = df['СТАТУС'].astype(str).str.strip().str.lower()
        return df[col_series == 'подписан']

    # Exact/substring filter if we find other status columns
    for col in status_columns:
        if col in df.columns:
            col_series = df[col].astype(str).str.strip().str.lower()
            return df[
                col_series.isin(signed_labels)
                | col_series.str.contains('подпис', na=False)
                | col_series.str.contains('sign', na=False)
            ]

    # Fallback: look for any column that appears to be a status field
    possible_status_cols = [c for c in df.columns if 'status' in str(c).lower() or 'статус' in str(c).lower()]
    for col in possible_status_cols:
        col_series = df[col].astype(str).str.strip().str.lower()
        return df[col_series.str.contains('подпис', na=False) | col_series.str.contains('sign', na=False)]

    # If nothing is detectable, return original df (do not drop data silently)
    return df

def categorize_bank_revenue(row):
    """Categorize bank revenue transactions based on transaction type and payment purpose"""
    # Try to use Transaction Type first (from bank processor)
    if 'Transaction Type' in row and pd.notna(row['Transaction Type']):
        trans_type = str(row['Transaction Type']).lower()
        if 'клиент' in trans_type or 'customer' in trans_type or 'payment' in trans_type:
            return 'Customer Payments'
        elif 'поступления' in trans_type or 'receipt' in trans_type:
            return 'Other Receipts'
        elif 'возврат' in trans_type or 'refund' in trans_type:
            return 'Refunds Received'
        elif 'процент' in trans_type or 'interest' in trans_type:
            return 'Interest Income'

    # Fallback to Payment Purpose analysis
    if 'Payment Purpose' in row and pd.notna(row['Payment Purpose']):
        purpose = str(row['Payment Purpose']).lower()
        if any(word in purpose for word in ['оплата', 'payment', 'pay']):
            return 'Customer Payments'
        elif any(word in purpose for word in ['косметик', 'медикамент', 'дори', 'cosmetic', 'medicine']):
            return 'Product Sales'
        elif any(word in purpose for word in ['возврат', 'refund', 'return']):
            return 'Refunds Received'
        elif any(word in purpose for word in ['процент', 'interest']):
            return 'Interest Income'

    # Default fallback
    return 'Other Bank Receipts'


def categorize_bank_expense(row):
    """Categorize bank expense transactions based on transaction type and payment purpose"""
    # Try to use Transaction Type first (from bank processor)
    if 'Transaction Type' in row and pd.notna(row['Transaction Type']):
        trans_type = str(row['Transaction Type']).lower()
        if 'поставщик' in trans_type or 'supplier' in trans_type:
            return 'Supplier Payments'
        elif 'зарплата' in trans_type or 'salary' in trans_type or 'wage' in trans_type:
            return 'Salary & Wages'
        elif 'налог' in trans_type or 'tax' in trans_type:
            return 'Tax Payments'
        elif 'аренда' in trans_type or 'rent' in trans_type:
            return 'Rent & Utilities'
        elif 'банк' in trans_type or 'bank' in trans_type or 'комиссия' in trans_type:
            return 'Bank Fees'

    # Fallback to Payment Purpose analysis
    if 'Payment Purpose' in row and pd.notna(row['Payment Purpose']):
        purpose = str(row['Payment Purpose']).lower()
        if any(word in purpose for word in ['поставщик', 'supplier', 'vendor']):
            return 'Supplier Payments'
        elif any(word in purpose for word in ['зарплата', 'salary', 'wage', 'оклад']):
            return 'Salary & Wages'
        elif any(word in purpose for word in ['налог', 'tax', 'ндс']):
            return 'Tax Payments'
        elif any(word in purpose for word in ['аренда', 'rent', 'арендная']):
            return 'Rent & Utilities'
        elif any(word in purpose for word in ['комиссия', 'fee', 'банк', 'bank']):
            return 'Bank Fees'
        elif any(word in purpose for word in ['реклама', 'маркетинг', 'marketing', 'advertising']):
            return 'Marketing & Advertising'
        elif any(word in purpose for word in ['офис', 'office', 'хозяйственные', 'supplies']):
            return 'Office & Supplies'

    # Default fallback
    return 'Other Bank Expenses'


@st.cache_data
def load_payment_codes():
    """Load and cache payment codes mapping"""
    try:
        # dict_data is in root directory, not pages directory
        codes_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dict_data', 'bank_payment_codes.xlsx')
        df = pd.read_excel(codes_path)

        # Create mapping dictionary
        code_mapping = {}
        for _, row in df.iterrows():
            code = str(row['Код назначения платежа']).zfill(5)  # Pad to 5 digits
            description = row.get('Unnamed: 2', '') or row.get('Наименование транша или назначения платежа', '')
            if pd.notna(description):
                code_mapping[code] = str(description).strip()

        return code_mapping
    except Exception as e:
        st.error(f"Could not load payment codes: {e}")
        return {}


@st.cache_data
def load_payment_codes_categorized():
    """Load and cache categorized payment codes mapping"""
    try:
        # dict_data is in root directory, not pages directory
        codes_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dict_data', 'bank_payment_codes_categorized.json')
        with open(codes_path, 'r', encoding='utf-8') as f:
            codes = json.load(f)

        # Create dictionaries for quick lookup
        code_to_category = {}
        code_to_name = {}

        for item in codes:
            code = item['code']
            code_to_category[code] = item['category']
            code_to_name[code] = item.get('name_ru', item.get('name_uz', ''))

        return code_to_category, code_to_name
    except Exception as e:
        st.error(f"Could not load categorized payment codes: {e}")
        return {}, {}


def extract_payment_code(payment_purpose):
    """Extract 5-digit payment code from payment purpose"""
    if pd.isna(payment_purpose) or not isinstance(payment_purpose, str):
        return None

    # Look for 5-digit codes at the beginning
    match = re.match(r'^(\d{5})\s', payment_purpose)
    if match:
        return match.group(1)

    # Look for 3-4 digit codes that we'll pad to 5 digits
    match = re.match(r'^(\d{3,4})\s', payment_purpose)
    if match:
        return match.group(1).zfill(5)

    return None


def categorize_by_payment_code(row, code_mapping):
    """Categorize transaction based on payment code"""
    if 'Payment Purpose' not in row or pd.isna(row['Payment Purpose']):
        return 'No Payment Code'

    code = extract_payment_code(row['Payment Purpose'])
    if code and code in code_mapping:
        return f"Code {code}: {code_mapping[code]}"
    elif code:
        return f"Code {code}: Unknown"
    else:
        return 'No Payment Code'


def get_available_data():
    """Get available data from database"""
    from utils.db_operations import load_user_invoices, load_user_bank_transactions

    # Get user_id from session state
    user_id = st.session_state.get('user_id')
    if not user_id:
        return {
            'invoices_in': None,
            'invoices_out': None,
            'bank_statements': None
        }

    # Load data from database
    invoices_in = load_user_invoices(user_id, invoice_type='IN')
    invoices_out = load_user_invoices(user_id, invoice_type='OUT')
    bank_statements = load_user_bank_transactions(user_id)

    # Convert date columns to datetime for consistency (handle both date and datetime objects from DB)
    if not invoices_in.empty and 'Document Date' in invoices_in.columns:
        invoices_in['date'] = pd.to_datetime(invoices_in['Document Date'], errors='coerce')
    if not invoices_out.empty and 'Document Date' in invoices_out.columns:
        invoices_out['date'] = pd.to_datetime(invoices_out['Document Date'], errors='coerce')
    if not bank_statements.empty and 'date' in bank_statements.columns:
        bank_statements['date'] = pd.to_datetime(bank_statements['date'], errors='coerce')

    return {
        'invoices_in': invoices_in if not invoices_in.empty else None,
        'invoices_out': invoices_out if not invoices_out.empty else None,
        'bank_statements': bank_statements if not bank_statements.empty else None
    }


def prepare_revenue_data(invoices_out_df, bank_statements_df):
    """
    Prepare revenue data:
    - Invoiced Revenue: invoices_out (signed)
    - Collected Revenue: bank statements Credit Turnover filtered by payment codes category='Revenue'
    """
    revenue_data = []
    code_to_category, code_to_name = load_payment_codes_categorized()

    # 1. Invoiced Revenue (from invoices_out)
    if invoices_out_df is not None and not invoices_out_df.empty:
        invoiced_revenue = invoices_out_df.copy()
        # Keep only signed invoices if status column is available
        invoiced_revenue = filter_signed_invoices(invoiced_revenue)

        # Find amount column from invoice data
        amount_col = find_amount_column(invoiced_revenue)
        if amount_col and 'date' in invoiced_revenue.columns:
            invoiced_revenue['amount'] = pd.to_numeric(invoiced_revenue[amount_col], errors='coerce').fillna(0)
            invoiced_revenue['source'] = 'Invoiced Revenue'
            invoiced_revenue['type'] = 'Revenue'
            # Add INN column if not present
            if 'inn' not in invoiced_revenue.columns:
                invoiced_revenue['inn'] = invoiced_revenue.get('Продавец (ИНН или ПИНФЛ)', None)
            revenue_data.append(invoiced_revenue[['date', 'amount', 'source', 'type', 'inn'] +
                                              [col for col in invoiced_revenue.columns if col not in ['date', 'amount', 'source', 'type', 'inn']]])

    # 2. Collected Revenue (from bank statements - Credit Turnover with category='Revenue')
    if bank_statements_df is not None and not bank_statements_df.empty:
        bank_df = bank_statements_df.copy()

        # Check if we have Credit Turnover and Operation Code columns
        credit_col = None
        for col in bank_df.columns:
            if 'credit' in str(col).lower() and 'turnover' in str(col).lower():
                credit_col = col
                break

        operation_code_col = None
        for col in bank_df.columns:
            if 'operation' in str(col).lower() and 'code' in str(col).lower():
                operation_code_col = col
                break

        if credit_col and 'date' in bank_df.columns:
            # Convert credit column to numeric
            bank_df[credit_col] = pd.to_numeric(bank_df[credit_col], errors='coerce').fillna(0)

            # Filter only rows with positive credit amounts
            collected = bank_df[bank_df[credit_col] > 0].copy()

            if not collected.empty:
                # Categorize by payment code if available
                if operation_code_col:
                    collected['payment_code'] = collected[operation_code_col].astype(str).str.zfill(5)
                    collected['payment_category'] = collected['payment_code'].map(code_to_category)
                    collected['payment_name'] = collected['payment_code'].map(code_to_name)

                    # Filter only Revenue category
                    collected = collected[collected['payment_category'] == 'Revenue']

                    # Create source from payment code name
                    collected['source'] = collected.apply(
                        lambda row: f"Collected Revenue: {row['payment_name']}" if pd.notna(row.get('payment_name')) else 'Collected Revenue (Uncategorized)',
                        axis=1
                    )
                else:
                    collected['source'] = 'Collected Revenue (No Payment Code)'

                collected['amount'] = collected[credit_col]
                collected['type'] = 'Revenue'

                if not collected.empty:
                    revenue_data.append(collected[['date', 'amount', 'source', 'type', 'inn'] +
                                                  [col for col in collected.columns if col not in ['date', 'amount', 'source', 'type', 'inn']]])

    if revenue_data:
        return pd.concat(revenue_data, ignore_index=True)
    return pd.DataFrame()


def prepare_expense_data(invoices_in_df, bank_statements_df):
    """
    Prepare expense data:
    - Invoiced Expenses: invoices_in (all supplier bills)
    - Paid Expenses: bank statements Debit Turnover filtered by payment codes category='Expense'
    """
    expense_data = []
    code_to_category, code_to_name = load_payment_codes_categorized()

    # 1. Invoiced Expenses (from invoices_in)
    if invoices_in_df is not None and not invoices_in_df.empty:
        invoiced_expenses = invoices_in_df.copy()
        # Keep only signed invoices if status column is available
        invoiced_expenses = filter_signed_invoices(invoiced_expenses)

        # Find amount column from invoice data
        amount_col = find_amount_column(invoiced_expenses)
        if amount_col and 'date' in invoiced_expenses.columns:
            invoiced_expenses['amount'] = pd.to_numeric(invoiced_expenses[amount_col], errors='coerce').fillna(0)
            invoiced_expenses['source'] = 'Invoiced Expenses'
            invoiced_expenses['type'] = 'Expense'
            # Make amounts positive for consistency
            invoiced_expenses['amount'] = abs(invoiced_expenses['amount'])
            # Add INN column if not present
            if 'inn' not in invoiced_expenses.columns:
                invoiced_expenses['inn'] = invoiced_expenses.get('Покупатель (ИНН или ПИНФЛ)', None)
            expense_data.append(invoiced_expenses[['date', 'amount', 'source', 'type', 'inn'] +
                                               [col for col in invoiced_expenses.columns if col not in ['date', 'amount', 'source', 'type', 'inn']]])

    # 2. Paid Expenses (from bank statements - Debit Turnover with category='Expense')
    if bank_statements_df is not None and not bank_statements_df.empty:
        bank_df = bank_statements_df.copy()

        # Check if we have Debit Turnover and Operation Code columns
        debit_col = None
        for col in bank_df.columns:
            if 'debit' in str(col).lower() and 'turnover' in str(col).lower():
                debit_col = col
                break

        operation_code_col = None
        for col in bank_df.columns:
            if 'operation' in str(col).lower() and 'code' in str(col).lower():
                operation_code_col = col
                break

        if debit_col and 'date' in bank_df.columns:
            # Convert debit column to numeric
            bank_df[debit_col] = pd.to_numeric(bank_df[debit_col], errors='coerce').fillna(0)

            # Filter only rows with positive debit amounts
            paid = bank_df[bank_df[debit_col] > 0].copy()

            if not paid.empty:
                # Categorize by payment code if available
                if operation_code_col:
                    paid['payment_code'] = paid[operation_code_col].astype(str).str.zfill(5)
                    paid['payment_category'] = paid['payment_code'].map(code_to_category)
                    paid['payment_name'] = paid['payment_code'].map(code_to_name)

                    # Filter only Expense category
                    paid = paid[paid['payment_category'] == 'Expense']

                    # Create source from payment code name
                    paid['source'] = paid.apply(
                        lambda row: f"Paid Expense: {row['payment_name']}" if pd.notna(row.get('payment_name')) else 'Paid Expense (Uncategorized)',
                        axis=1
                    )
                else:
                    paid['source'] = 'Paid Expense (No Payment Code)'

                paid['amount'] = paid[debit_col]
                paid['type'] = 'Expense'

                if not paid.empty:
                    expense_data.append(paid[['date', 'amount', 'source', 'type', 'inn'] +
                                             [col for col in paid.columns if col not in ['date', 'amount', 'source', 'type', 'inn']]])

    if expense_data:
        return pd.concat(expense_data, ignore_index=True)
    return pd.DataFrame()


def create_company_options(all_data):
    """Create company options with INN and names"""
    company_options = {}

    if 'inn' not in all_data.columns:
        return company_options

    # Get unique INNs
    unique_inns = all_data['inn'].dropna().unique()

    for inn in unique_inns:
        # Format INN (remove .0)
        try:
            if pd.notna(inn) and str(inn).lower() != 'nan':
                inn_float = float(inn)
                if inn_float == int(inn_float):
                    inn_formatted = str(int(inn_float))
                else:
                    inn_formatted = str(inn)
            else:
                continue  # Skip invalid INNs
        except (ValueError, TypeError):
            continue  # Skip invalid INNs

        # Get company name for this INN
        company_data = all_data[all_data['inn'] == inn]
        company_name = get_company_name(company_data)

        # Create display text
        if company_name:
            display_text = f"{inn_formatted} - {company_name}"
        else:
            display_text = inn_formatted

        company_options[inn] = display_text

    return company_options


def get_company_name(company_data):
    """Extract company name from available fields"""
    # Priority order for company name fields
    name_fields = [
        'Account Name', 'Наименование плательщика', 'Наименование получателя',
        'Продавец (наименование)', 'Покупатель (наименование)',
        'Seller (Name)', 'Buyer (Name)', 'name', 'Name'
    ]

    for field in name_fields:
        if field in company_data.columns:
            names = company_data[field].dropna().unique()
            if len(names) > 0:
                # Return the first non-empty name
                return str(names[0]).strip()

    return None


def render_filters(revenue_df, expense_df):
    """Render filter controls"""
    lang = st.session_state.get('language', 'en')
    st.subheader(get_text('filters', lang))

    # Combine all data for filter options
    all_data = pd.concat([revenue_df, expense_df], ignore_index=True) if not revenue_df.empty or not expense_df.empty else pd.DataFrame()

    if all_data.empty:
        st.warning(get_text('no_processed_data', lang))
        return None, None, None

    filter_col1, filter_col2, filter_col3 = st.columns(3)

    # Date range filter
    with filter_col1:
        if 'date' in all_data.columns:
            # Handle both datetime and date objects
            min_val = all_data['date'].min()
            max_val = all_data['date'].max()

            if pd.notna(min_val):
                # Convert to date object, handling Timestamp specifically
                if isinstance(min_val, pd.Timestamp):
                    min_date = min_val.date()
                elif hasattr(min_val, 'date') and not isinstance(min_val, datetime.date):
                    min_date = min_val.date()
                else:
                    min_date = min_val
            else:
                min_date = dt.now().date()

            if pd.notna(max_val):
                # Convert to date object, handling Timestamp specifically
                if isinstance(max_val, pd.Timestamp):
                    max_date = max_val.date()
                elif hasattr(max_val, 'date') and not isinstance(max_val, datetime.date):
                    max_date = max_val.date()
                else:
                    max_date = max_val
            else:
                max_date = dt.now().date()

            date_range = st.date_input(
                get_text('date_range', lang),
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date
            )
        else:
            date_range = None

    # Period filter (daily, weekly, monthly)
    with filter_col2:
        period_filter = st.selectbox(
            get_text('period_grouping', lang),
            [get_text('period_daily', lang), get_text('period_weekly', lang), get_text('period_monthly', lang)],
            index=2
        )

    # Company filter (by INN)
    with filter_col3:
        if 'inn' in all_data.columns:
            # Create company options with names
            company_options = create_company_options(all_data)
            if len(company_options) > 0:
                selected_companies = st.multiselect(
                    get_text('filter_by_company_inn', lang),
                    options=list(company_options.keys()),
                    format_func=lambda x: company_options[x],
                    default=list(company_options.keys())[:10] if len(company_options) <= 10 else []
                )
            else:
                selected_companies = []
                st.info(get_text('no_company_inn', lang))
        else:
            selected_companies = []
            st.info(get_text('no_company_inn', lang))

    return date_range, period_filter, selected_companies


def apply_filters(df, date_range, selected_companies):
    """Apply filters to dataframe"""
    if df.empty:
        return df

    filtered_df = df.copy()

    # Apply date filter
    if date_range and len(date_range) == 2 and 'date' in filtered_df.columns:
        start_date, end_date = date_range
        filtered_df = filtered_df[
            (filtered_df['date'].dt.date >= start_date) &
            (filtered_df['date'].dt.date <= end_date)
        ]

    # Apply company filter
    if selected_companies and 'inn' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['inn'].isin(selected_companies)]

    return filtered_df


def group_by_period(df, period_filter):
    """Group data by selected period"""
    if df.empty or 'date' not in df.columns:
        return df

    df_grouped = df.copy()

    lang = st.session_state.get('language', 'en')
    daily = get_text('period_daily', lang)
    weekly = get_text('period_weekly', lang)
    monthly = get_text('period_monthly', lang)

    if period_filter == daily:
        df_grouped['period'] = df_grouped['date'].dt.date
    elif period_filter == weekly:
        df_grouped['period'] = df_grouped['date'].dt.to_period('W').dt.start_time
    elif period_filter == monthly:
        df_grouped['period'] = df_grouped['date'].dt.to_period('M').dt.start_time

    return df_grouped


def render_revenue_expense_overview(revenue_df, expense_df, period_filter):
    """Render main revenue/expense overview charts - separated into Invoiced and Cash-basis"""
    lang = st.session_state.get('language', 'en')

    if revenue_df.empty and expense_df.empty:
        st.info(get_text('no_financial_data', lang))
        return

    # === 1. INVOICED REVENUE vs INVOICED EXPENSES (Accrual Basis) ===
    st.subheader(get_text('inv_acc_basis_header', lang))
    st.caption(get_text('inv_acc_basis_caption', lang))

    # Filter only invoiced data
    invoiced_revenue = revenue_df[revenue_df['source'] == 'Invoiced Revenue'].copy() if not revenue_df.empty else pd.DataFrame()
    invoiced_expenses = expense_df[expense_df['source'] == 'Invoiced Expenses'].copy() if not expense_df.empty else pd.DataFrame()

    if not invoiced_revenue.empty or not invoiced_expenses.empty:
        # Group by period
        invoiced_rev_grouped = group_by_period(invoiced_revenue, period_filter) if not invoiced_revenue.empty else pd.DataFrame()
        invoiced_exp_grouped = group_by_period(invoiced_expenses, period_filter) if not invoiced_expenses.empty else pd.DataFrame()

        invoiced_summary_data = []
        if not invoiced_rev_grouped.empty:
            rev_summary = invoiced_rev_grouped.groupby('period')['amount'].sum().reset_index()
            rev_summary['type'] = 'Invoiced Revenue'
            invoiced_summary_data.append(rev_summary)

        if not invoiced_exp_grouped.empty:
            exp_summary = invoiced_exp_grouped.groupby('period')['amount'].sum().reset_index()
            exp_summary['type'] = 'Invoiced Expenses'
            invoiced_summary_data.append(exp_summary)

        if invoiced_summary_data:
            invoiced_combined = pd.concat(invoiced_summary_data, ignore_index=True)

            # Net profit calculation
            invoiced_pivot = invoiced_combined.pivot_table(
                index='period',
                columns='type',
                values='amount',
                fill_value=0
            ).reset_index()

            if 'Invoiced Revenue' in invoiced_pivot.columns and 'Invoiced Expenses' in invoiced_pivot.columns:
                invoiced_pivot['Net Profit (Invoiced)'] = invoiced_pivot['Invoiced Revenue'] - invoiced_pivot['Invoiced Expenses']

            # Localize type labels for legend
            local_label_invoiced_revenue = get_text('label_invoiced_revenue', lang)
            local_label_invoiced_expenses = get_text('label_invoiced_expenses', lang)
            invoiced_combined['type_label'] = invoiced_combined['type'].map({
                'Invoiced Revenue': local_label_invoiced_revenue,
                'Invoiced Expenses': local_label_invoiced_expenses,
            })

            # Chart
            fig_invoiced = px.bar(
                invoiced_combined,
                x='period',
                y='amount',
                color='type_label',
                title=f"{local_label_invoiced_revenue} vs {local_label_invoiced_expenses} ({period_filter})",
                color_discrete_map={local_label_invoiced_revenue: '#00D4AA', local_label_invoiced_expenses: '#FF6B6B'},
                barmode='group'
            )
            # Show value labels on bars for readability
            fig_invoiced.update_traces(
                selector=dict(type='bar'),
                texttemplate='%{y:,.0f}',
                textposition='outside'
            )

            # Overlay net profit line if available
            if 'Net Profit (Invoiced)' in invoiced_pivot.columns:
                invoiced_pivot_sorted = invoiced_pivot.sort_values('period')
                fig_invoiced.add_trace(
                    go.Scatter(
                        x=invoiced_pivot_sorted['period'],
                        y=invoiced_pivot_sorted['Net Profit (Invoiced)'],
                        mode='lines+markers',
                        name=get_text('label_net_profit_invoiced', lang),
                        line=dict(color='#4CAF50', width=3)
                    )
                )

                # Brief textual insights below the chart
                try:
                    max_row = invoiced_pivot_sorted.loc[invoiced_pivot_sorted['Net Profit (Invoiced)'].idxmax()]
                    min_row = invoiced_pivot_sorted.loc[invoiced_pivot_sorted['Net Profit (Invoiced)'].idxmin()]
                    total_rev = invoiced_pivot_sorted['Invoiced Revenue'].sum() if 'Invoiced Revenue' in invoiced_pivot_sorted.columns else 0
                    total_exp = invoiced_pivot_sorted['Invoiced Expenses'].sum() if 'Invoiced Expenses' in invoiced_pivot_sorted.columns else 0
                    total_net = total_rev - total_exp
                    margin = (total_net / total_rev * 100) if total_rev else 0
                    st.caption(
                        f"Best month (invoiced): {pd.to_datetime(max_row['period']).strftime('%b %Y')} — Net {max_row['Net Profit (Invoiced)']:,.0f}; "
                        f"Worst month: {pd.to_datetime(min_row['period']).strftime('%b %Y')} — Net {min_row['Net Profit (Invoiced)']:,.0f}; "
                        f"Total margin: {margin:,.1f}%"
                    )
                except Exception:
                    pass

            fig_invoiced.update_layout(height=420)
            st.plotly_chart(fig_invoiced, use_container_width=True)

            # Totals summary (after chart)
            try:
                totals_by_type = invoiced_combined.groupby('type')['amount'].sum()
                total_rev_sum = float(totals_by_type.get('Invoiced Revenue', 0))
                total_exp_sum = float(totals_by_type.get('Invoiced Expenses', 0))
                total_net_sum = total_rev_sum - total_exp_sum
                total_margin = (total_net_sum / total_rev_sum * 100) if total_rev_sum else 0
                st.caption(
                    f"Totals (Invoiced): Revenue {total_rev_sum:,.0f} | Expenses {total_exp_sum:,.0f} | Net {total_net_sum:,.0f} | Margin {total_margin:,.1f}%"
                )
            except Exception:
                pass

            # # Net profit trend
            # if 'Net Profit (Invoiced)' in invoiced_pivot.columns:
            #     fig_profit_invoiced = px.line(
            #         invoiced_pivot,
            #         x='period',
            #         y='Net Profit (Invoiced)',
            #         title=f"Net Profit Trend - Invoiced ({period_filter})",
            #         line_shape='spline',
            #         markers=True
            #     )
            #     fig_profit_invoiced.update_traces(line_color='#4CAF50', line_width=3)
            #     fig_profit_invoiced.update_layout(height=300)
            #     st.plotly_chart(fig_profit_invoiced, use_container_width=True)
    else:
        st.info(get_text('info_no_invoiced_data', lang))

    st.divider()

    # === 2. COLLECTED REVENUE vs PAID EXPENSES (Cash Basis) ===
    st.subheader(get_text('cash_basis_header', lang))
    st.caption(get_text('cash_basis_caption', lang))

    # Filter only cash-basis data (from bank statements)
    collected_revenue = revenue_df[revenue_df['source'].str.contains('Collected', na=False)].copy() if not revenue_df.empty else pd.DataFrame()
    paid_expenses = expense_df[expense_df['source'].str.contains('Paid', na=False)].copy() if not expense_df.empty else pd.DataFrame()

    if not collected_revenue.empty or not paid_expenses.empty:
        # Group by period
        collected_rev_grouped = group_by_period(collected_revenue, period_filter) if not collected_revenue.empty else pd.DataFrame()
        paid_exp_grouped = group_by_period(paid_expenses, period_filter) if not paid_expenses.empty else pd.DataFrame()

        cash_summary_data = []
        if not collected_rev_grouped.empty:
            rev_summary = collected_rev_grouped.groupby('period')['amount'].sum().reset_index()
            rev_summary['type'] = 'Collected Revenue'
            cash_summary_data.append(rev_summary)

        if not paid_exp_grouped.empty:
            exp_summary = paid_exp_grouped.groupby('period')['amount'].sum().reset_index()
            exp_summary['type'] = 'Paid Expenses'
            cash_summary_data.append(exp_summary)

        if cash_summary_data:
            cash_combined = pd.concat(cash_summary_data, ignore_index=True)

            # Net cash flow calculation
            cash_pivot = cash_combined.pivot_table(
                index='period',
                columns='type',
                values='amount',
                fill_value=0
            ).reset_index()

            if 'Collected Revenue' in cash_pivot.columns and 'Paid Expenses' in cash_pivot.columns:
                cash_pivot['Net Cash Flow'] = cash_pivot['Collected Revenue'] - cash_pivot['Paid Expenses']

            # Localize type labels for legend
            local_label_collected_revenue = get_text('label_collected_revenue', lang)
            local_label_paid_expenses = get_text('label_paid_expenses', lang)
            cash_combined['type_label'] = cash_combined['type'].map({
                'Collected Revenue': local_label_collected_revenue,
                'Paid Expenses': local_label_paid_expenses,
            })

            # Chart
            fig_cash = px.bar(
                cash_combined,
                x='period',
                y='amount',
                color='type_label',
                title=f"{local_label_collected_revenue} vs {local_label_paid_expenses} ({period_filter})",
                color_discrete_map={local_label_collected_revenue: '#20B2AA', local_label_paid_expenses: '#DC143C'},
                barmode='group'
            )
            # Show value labels on bars for readability
            fig_cash.update_traces(
                selector=dict(type='bar'),
                texttemplate='%{y:,.0f}',
                textposition='outside'
            )

            # Overlay net cash flow line if available
            if 'Net Cash Flow' in cash_pivot.columns:
                cash_pivot_sorted = cash_pivot.sort_values('period')
                fig_cash.add_trace(
                    go.Scatter(
                        x=cash_pivot_sorted['period'],
                        y=cash_pivot_sorted['Net Cash Flow'],
                        mode='lines+markers',
                        name=get_text('label_net_cash_flow', lang),
                        line=dict(color='#1E90FF', width=3)
                    )
                )

                # Brief textual insights below the chart
                try:
                    max_row_c = cash_pivot_sorted.loc[cash_pivot_sorted['Net Cash Flow'].idxmax()]
                    min_row_c = cash_pivot_sorted.loc[cash_pivot_sorted['Net Cash Flow'].idxmin()]
                    st.caption(
                        f"Best month (cash): {pd.to_datetime(max_row_c['period']).strftime('%b %Y')} — Net Cash {max_row_c['Net Cash Flow']:,.0f}; "
                        f"Worst month: {pd.to_datetime(min_row_c['period']).strftime('%b %Y')} — Net Cash {min_row_c['Net Cash Flow']:,.0f}"
                    )
                except Exception:
                    pass

            fig_cash.update_layout(height=420)
            st.plotly_chart(fig_cash, use_container_width=True)

            # # Net cash flow trend
            # if 'Net Cash Flow' in cash_pivot.columns:
            #     fig_cashflow = px.line(
            #         cash_pivot,
            #         x='period',
            #         y='Net Cash Flow',
            #         title=f"Net Cash Flow Trend ({period_filter})",
            #         line_shape='spline',
            #         markers=True
            #     )
            #     fig_cashflow.update_traces(line_color='#1E90FF', line_width=3)
            #     fig_cashflow.update_layout(height=300)
            #     st.plotly_chart(fig_cashflow, use_container_width=True)
    else:
        st.info(get_text('info_no_cash_data', lang))


def render_detailed_sections(revenue_df, expense_df):
    """Render combined revenue and expense analytics section"""
    lang = st.session_state.get('language', 'en')

    # === COMBINED REVENUE & EXPENSE ANALYTICS ===
    st.subheader(get_text('combined_analytics_header', lang))

    if revenue_df.empty and expense_df.empty:
        st.info(get_text('no_financial_data', lang))
        return

    # Combine revenue and expense data
    combined_data = []

    # Add revenue data - check for payment codes in collected revenue
    if not revenue_df.empty:
        collected_revenue = revenue_df[revenue_df['source'].str.contains('Collected Revenue', na=False)]

        if not collected_revenue.empty and 'payment_code' in collected_revenue.columns and 'payment_name' in collected_revenue.columns:
            revenue_by_code = collected_revenue.groupby(['payment_code', 'payment_name'])['amount'].sum().reset_index()
            revenue_by_code = revenue_by_code.sort_values('amount', ascending=False).head(10)
            revenue_by_code['label'] = revenue_by_code['payment_code'] + ': ' + revenue_by_code['payment_name']
            revenue_by_code['type'] = 'Revenue'
            combined_data.append(revenue_by_code[['label', 'amount', 'type']])
        else:
            # Fallback: show revenue by source if no payment codes
            revenue_by_source = revenue_df.groupby('source')['amount'].sum().reset_index()
            revenue_by_source = revenue_by_source.sort_values('amount', ascending=False)
            revenue_by_source['label'] = revenue_by_source['source']
            revenue_by_source['type'] = 'Revenue'
            combined_data.append(revenue_by_source[['label', 'amount', 'type']])

    # Add expense data - check for payment codes in paid expenses
    if not expense_df.empty:
        paid_expenses = expense_df[expense_df['source'].str.contains('Paid Expense', na=False)]

        if not paid_expenses.empty and 'payment_code' in paid_expenses.columns and 'payment_name' in paid_expenses.columns:
            expense_by_code = paid_expenses.groupby(['payment_code', 'payment_name'])['amount'].sum().reset_index()
            expense_by_code = expense_by_code.sort_values('amount', ascending=False).head(10)
            expense_by_code['label'] = expense_by_code['payment_code'] + ': ' + expense_by_code['payment_name']
            expense_by_code['type'] = 'Expense'
            combined_data.append(expense_by_code[['label', 'amount', 'type']])
        else:
            # Fallback: show expenses by source if no payment codes
            expense_by_source = expense_df.groupby('source')['amount'].sum().reset_index()
            expense_by_source = expense_by_source.sort_values('amount', ascending=False)
            expense_by_source['label'] = expense_by_source['source']
            expense_by_source['type'] = 'Expense'
            combined_data.append(expense_by_source[['label', 'amount', 'type']])

    # Display combined chart if data available
    if combined_data:
        combined_df = pd.concat(combined_data, ignore_index=True)

        # Localize type labels for legend
        type_map = {
            'Revenue': get_text('label_revenue', lang),
            'Expense': get_text('label_expense', lang),
        }
        if 'type' in combined_df.columns:
            combined_df['type_label'] = combined_df['type'].map(lambda v: type_map.get(v, v))
        else:
            combined_df['type_label'] = combined_df.get('type', '')

        # Create grouped bar chart
        fig_combined = px.bar(
            combined_df,
            x='amount',
            y='label',
            color='type_label',
            title=get_text('rev_exp_breakdown_title', lang),
            orientation='h',
            color_discrete_map={get_text('label_revenue', lang): '#00D4AA', get_text('label_expense', lang): '#FF6B6B'},
            barmode='group'
        )
        fig_combined.update_layout(
            height=max(400, len(combined_df) * 30),  # Dynamic height based on data
            xaxis_title=get_text('label_amount', lang),
            yaxis_title="",
            legend_title=get_text('label_type', lang),
            yaxis={'categoryorder': 'total ascending'}
        )
        st.plotly_chart(fig_combined, use_container_width=True)
    else:
        st.info(get_text('no_financial_data', lang))


def render_payment_code_analysis(bank_statements_df, date_range, selected_companies):
    """Render separate payment code analysis section"""
    lang = st.session_state.get('language', 'en')
    st.subheader(get_text('payment_code_analysis_header', lang))
    st.caption(get_text('payment_code_analysis_caption', lang))

    if bank_statements_df is None or bank_statements_df.empty:
        st.info(get_text('no_bank_data_payment_code', lang))
        return

    # Load payment codes mapping
    code_mapping = load_payment_codes()
    if not code_mapping:
        st.warning(get_text('payment_codes_mapping_not_found', lang))
        return

    # Filter bank statements
    filtered_bank = apply_filters(bank_statements_df, date_range, selected_companies)
    if filtered_bank.empty:
        st.info(get_text('no_bank_txn_match_filters', lang))
        return

    # Add payment code categorization
    filtered_bank['payment_code_category'] = filtered_bank.apply(
        lambda row: categorize_by_payment_code(row, code_mapping), axis=1
    )

    # Load categorized payment codes for category breakdown
    code_to_category, code_to_name = load_payment_codes_categorized()

    # Extract payment codes and add category (Revenue/Expense/Other)
    filtered_bank['payment_code_extracted'] = filtered_bank['Payment Purpose'].apply(extract_payment_code) if 'Payment Purpose' in filtered_bank.columns else None
    if filtered_bank['payment_code_extracted'] is not None:
        filtered_bank['category_type'] = filtered_bank['payment_code_extracted'].map(code_to_category).fillna('Other')
    else:
        filtered_bank['category_type'] = 'Other'

    # Summary statistics
    total_transactions = len(filtered_bank)
    coded_transactions = len(filtered_bank[filtered_bank['payment_code_category'] != 'No Payment Code'])
    code_coverage = (coded_transactions / total_transactions * 100) if total_transactions > 0 else 0

    # Display metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(get_text('metric_total_transactions', lang), f"{total_transactions:,}")
    with col2:
        st.metric(get_text('metric_with_payment_codes', lang), f"{coded_transactions:,}")
    with col3:
        st.metric(get_text('metric_code_coverage', lang), f"{code_coverage:.1f}%")

    # Payment Code Category Breakdown (Revenue/Expense/Other)
    if coded_transactions > 0:
        st.write("### " + get_text('payment_code_category_breakdown', lang))

        # Prepare category data
        if 'amount' in filtered_bank.columns:
            category_summary = filtered_bank[filtered_bank['payment_code_category'] != 'No Payment Code'].groupby('category_type')['amount'].sum().abs().reset_index()
            category_summary.columns = ['Category', 'Total Amount']
            category_summary = category_summary[category_summary['Category'] != 'Other']  # Remove 'Other' if exists

            if not category_summary.empty:
                # Create two columns for pie charts
                pie_col1, pie_col2 = st.columns(2)

                with pie_col1:
                    # Pie chart by category (Revenue/Expense)
                    fig_category_pie = px.pie(
                        category_summary,
                        values='Total Amount',
                        names='Category',
                        title=get_text('transaction_amount_by_category', lang),
                        color='Category',
                        color_discrete_map={'Revenue': '#00D4AA', 'Expense': '#FF6B6B', 'Other': '#999999'}
                    )
                    fig_category_pie.update_traces(textposition='inside', textinfo='percent+label')
                    fig_category_pie.update_layout(height=400)
                    st.plotly_chart(fig_category_pie, use_container_width=True)

                with pie_col2:
                    # Transaction count by category
                    category_count = filtered_bank[filtered_bank['payment_code_category'] != 'No Payment Code'].groupby('category_type').size().reset_index()
                    category_count.columns = ['Category', 'Transaction Count']
                    category_count = category_count[category_count['Category'] != 'Other']

                    if not category_count.empty:
                        fig_category_count = px.pie(
                            category_count,
                            values='Transaction Count',
                            names='Category',
                            title=get_text('transaction_count_by_category', lang),
                            color='Category',
                            color_discrete_map={'Revenue': '#00D4AA', 'Expense': '#FF6B6B', 'Other': '#999999'}
                        )
                        fig_category_count.update_traces(textposition='inside', textinfo='percent+label')
                        fig_category_count.update_layout(height=400)
                        st.plotly_chart(fig_category_count, use_container_width=True)

        st.divider()

    # Payment code breakdown charts
    if coded_transactions > 0:
        # Chart type selector
        option_pie = get_text('chart_pie', lang)
        option_line = get_text('chart_line', lang)
        option_area = get_text('chart_area', lang)
        option_bar = get_text('chart_bar', lang)
        chart_type = st.selectbox(
            get_text('select_chart_type', lang),
            [option_pie, option_line, option_area, option_bar],
            index=0
        )

        # Amount by payment code
        if 'amount' in filtered_bank.columns:
            amount_summary = filtered_bank[filtered_bank['payment_code_category'] != 'No Payment Code'].groupby('payment_code_category')['amount'].sum().abs().reset_index()
            amount_summary.columns = ['Payment Code', 'Total Amount']
            amount_summary = amount_summary.sort_values('Total Amount', ascending=False).head(15)

            # Create chart based on selection
            if chart_type == option_pie:
                fig_amount = px.pie(
                    amount_summary,
                    values='Total Amount',
                    names='Payment Code',
                    title=get_text('top15_payment_codes_by_amount_title', lang)
                )
                fig_amount.update_layout(height=500)

            elif chart_type == option_line:
                # Sort by payment code for line chart
                amount_summary_sorted = amount_summary.sort_values('Payment Code')
                fig_amount = px.line(
                    amount_summary_sorted,
                    x='Payment Code',
                    y='Total Amount',
                    title=get_text('top15_payment_codes_by_amount_title', lang),
                    markers=True,
                    line_shape='spline'
                )
                fig_amount.update_layout(height=500, xaxis={'tickangle': -45})
                fig_amount.update_traces(line_color='#636EFA', line_width=3)

            elif chart_type == option_area:
                # Sort by payment code for area chart
                amount_summary_sorted = amount_summary.sort_values('Payment Code')
                fig_amount = px.area(
                    amount_summary_sorted,
                    x='Payment Code',
                    y='Total Amount',
                    title=get_text('top15_payment_codes_by_amount_title', lang)
                )
                fig_amount.update_layout(height=500, xaxis={'tickangle': -45})
            elif chart_type == option_bar:
                fig_amount = px.bar(
                    amount_summary,
                    x='Total Amount',
                    y='Payment Code',
                    title=get_text('top15_payment_codes_by_amount_title', lang),
                    orientation='h',
                    color='Total Amount',
                    color_continuous_scale='viridis'
                )
                fig_amount.update_layout(height=500)

            st.plotly_chart(fig_amount, use_container_width=True)

        # Detailed breakdown table
        with st.expander(get_text('detailed_payment_code_breakdown', lang), expanded=False):
            detailed_summary = filtered_bank[filtered_bank['payment_code_category'] != 'No Payment Code'].groupby('payment_code_category').agg({
                'amount': ['count', 'sum', 'mean'],
            }).round(2)

            detailed_summary.columns = ['Transaction Count', 'Total Amount', 'Average Amount']
            detailed_summary = detailed_summary.sort_values('Total Amount', ascending=False)
            st.dataframe(detailed_summary, use_container_width=True)


    else:
        st.info(get_text('no_txn_with_payment_codes', lang))

        # Show some examples of transactions without codes
        st.write("**" + get_text('sample_txn_without_codes', lang) + "**")
        no_code_transactions = filtered_bank[
            filtered_bank['payment_code_category'] == 'No Payment Code'
        ].head(5)[['date', 'amount', 'Payment Purpose', 'Account Name']]
        st.dataframe(no_code_transactions, use_container_width=True)


def render_header() -> None:
    lang = st.session_state.get('language', 'en')
    st.title(get_text('home_title', lang))
    st.caption(get_text('home_subtitle', lang))


def render_main() -> None:
    # Get available data
    data_sources = get_available_data()

    # Check if any data is available
    has_data = any(df is not None and not df.empty for df in data_sources.values())

    if not has_data:
        lang = st.session_state.get('language', 'en')
        st.warning(get_text('no_processed_data', lang))
        st.page_link("pages/file_upload.py", label=get_text('go_to_file_upload_short', lang), icon="📁")
        return

    revenue_df = prepare_revenue_data(data_sources['invoices_out'], data_sources['bank_statements'])
    expense_df = prepare_expense_data(data_sources['invoices_in'], data_sources['bank_statements'])


    # Render filters
    date_range, period_filter, selected_companies = render_filters(revenue_df, expense_df)

    if date_range is None:  # No data available
        return

    # Apply filters
    filtered_revenue = apply_filters(revenue_df, date_range, selected_companies)
    filtered_expense = apply_filters(expense_df, date_range, selected_companies)

    # === KEY PERFORMANCE INDICATORS (KPIs) - TOP OF DASHBOARD ===
    if not filtered_revenue.empty or not filtered_expense.empty:
        st.markdown("## " + get_text('kpi_header', st.session_state.get('language', 'en')))

        # Calculate KPIs
        total_revenue = filtered_revenue['amount'].sum() if not filtered_revenue.empty else 0
        total_expense = filtered_expense['amount'].sum() if not filtered_expense.empty else 0
        net_profit = total_revenue - total_expense
        profit_margin = (net_profit / total_revenue * 100) if total_revenue > 0 else 0

        # Calculate DSO (Days Sales Outstanding)
        # DSO = (Accounts Receivable / Total Revenue) * Number of Days
        invoiced_revenue = filtered_revenue[filtered_revenue['source'] == 'Invoiced Revenue']['amount'].sum() if not filtered_revenue.empty else 0
        collected_revenue = filtered_revenue[filtered_revenue['source'].str.contains('Collected', na=False)]['amount'].sum() if not filtered_revenue.empty else 0
        accounts_receivable = max(0, invoiced_revenue - collected_revenue)

        # Calculate number of days in the filtered period
        if date_range and len(date_range) == 2:
            days_in_period = (date_range[1] - date_range[0]).days + 1
        else:
            days_in_period = 365  # Default to annual

        # DSO Formula: (Accounts Receivable / Total Revenue) * Days in Period
        dso = (accounts_receivable / total_revenue * days_in_period) if total_revenue > 0 else 0

        lang = st.session_state.get('language', 'en')

        # Display KPIs in 5 columns
        metric_col1, metric_col2, metric_col3, metric_col4, metric_col5 = st.columns(5)

        with metric_col1:
            st.metric(get_text('metric_total_revenue', lang), f"{total_revenue:,.0f}")

        with metric_col2:
            st.metric(get_text('metric_total_expenses', lang), f"{total_expense:,.0f}")

        with metric_col3:
            # Highlighted Net Profit with color indicator
            delta_color = "normal" if net_profit >= 0 else "inverse"
            st.metric(
                "💰 " + get_text('metric_net_profit', lang),
                f"{net_profit:,.0f}",
                delta=f"{profit_margin:.1f}%",
                delta_color=delta_color
            )

        with metric_col4:
            st.metric(get_text('metric_profit_margin', lang), f"{profit_margin:.1f}%")

        with metric_col5:
            # DSO with indicator (lower is better)
            dso_benchmark = 45  # Industry standard benchmark
            dso_delta = dso - dso_benchmark
            st.metric(
                "📅 DSO (Days)",
                f"{dso:.0f}",
                delta=f"{dso_delta:+.0f} vs benchmark",
                delta_color="inverse"  # Lower is better
            )

        st.divider()

    # Main overview
    render_revenue_expense_overview(filtered_revenue, filtered_expense, period_filter)

    st.divider()

    # Detailed sections
    render_detailed_sections(filtered_revenue, filtered_expense)

    st.divider()

    # Payment Code Analysis (separate from main revenue/expense analysis)
    if data_sources['bank_statements'] is not None:
        render_payment_code_analysis(data_sources['bank_statements'], date_range, selected_companies)



def main() -> None:
    render_header()
    render_main()


if __name__ == "__main__":
    main()
