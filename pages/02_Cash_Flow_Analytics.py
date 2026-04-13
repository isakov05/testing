import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import datetime
from datetime import datetime as dt
from translations import get_text
from auth.db_authenticator import protect_page
from utils.pagination import paginate_dataframe, render_pagination_controls

st.set_page_config(page_title="Cash Flow Analytics", page_icon="📈", layout="wide")

protect_page()


def get_available_data():
    """Get available data from session state"""
    return {
        'invoices_in': st.session_state.get('invoices_in_processed'),
        'invoices_out': st.session_state.get('invoices_out_processed'),
        'bank_statements': st.session_state.get('bank_statements_processed')
    }


def prepare_cash_flow_data(data_sources):
    """Prepare cash flow data from all sources"""
    cash_flows = []

    # Process bank statements - most reliable source
    if data_sources['bank_statements'] is not None and not data_sources['bank_statements'].empty:
        bank_df = data_sources['bank_statements'].copy()

        # Ensure required columns exist
        if 'date' in bank_df.columns and 'amount' in bank_df.columns:
            bank_df['source'] = 'Bank Transactions'
            bank_df['type'] = bank_df['amount'].apply(lambda x: 'Inflow' if x > 0 else 'Outflow')
            bank_df['category'] = bank_df.apply(categorize_transaction, axis=1)
            cash_flows.append(bank_df[['date', 'amount', 'source', 'type', 'category', 'Account Name', 'Payment Purpose']])


    if cash_flows:
        combined_df = pd.concat(cash_flows, ignore_index=True)
        combined_df = combined_df.dropna(subset=['date', 'amount'])
        combined_df = combined_df.sort_values('date').reset_index(drop=True)
        return combined_df

    return pd.DataFrame()


def find_amount_column(df):
    """Find the appropriate amount column"""
    amount_columns = [
        'Стоимость поставки с учётом НДС', 'amount', 'Amount', 'Сумма',
        'Стоимость', 'Cost', 'Total', 'Supply Value (incl. VAT)'
    ]

    for col in amount_columns:
        if col in df.columns:
            return col
    return None


def categorize_transaction(row):
    """Categorize transaction based on amount and payment purpose"""
    if pd.isna(row.get('Payment Purpose', '')):
        return 'Uncategorized'

    purpose = str(row.get('Payment Purpose', '')).lower()
    amount = row.get('amount', 0)

    if amount > 0:  # Inflows
        if any(word in purpose for word in ['оплата', 'payment', 'продажа', 'sale']):
            return 'Customer Payments'
        elif any(word in purpose for word in ['возврат', 'refund']):
            return 'Refunds Received'
        elif any(word in purpose for word in ['процент', 'interest']):
            return 'Interest Income'
        else:
            return 'Other Income'
    else:  # Outflows
        if any(word in purpose for word in ['поставщик', 'supplier', 'закуп']):
            return 'Supplier Payments'
        elif any(word in purpose for word in ['зарплата', 'salary', 'оклад']):
            return 'Salary & Wages'
        elif any(word in purpose for word in ['налог', 'tax', 'ндс']):
            return 'Tax Payments'
        elif any(word in purpose for word in ['аренда', 'rent']):
            return 'Rent & Utilities'
        elif any(word in purpose for word in ['банк', 'bank', 'комиссия']):
            return 'Bank Fees'
        else:
            return 'Other Expenses'


def render_cash_flow_filters(df):
    """Render cash flow filter controls"""
    if df.empty:
        return None, None, None

    col1, col2, col3 = st.columns(3)
    lang = st.session_state.get('language', 'en')

    with col1:
        # Date range filter - handle both datetime and date objects
        import pandas as pd
        min_val = df['date'].min()
        max_val = df['date'].max()

        # Convert to date objects, handling Timestamp specifically
        if isinstance(min_val, pd.Timestamp):
            min_date = min_val.date()
        elif hasattr(min_val, 'date') and not isinstance(min_val, datetime.date):
            min_date = min_val.date()
        else:
            min_date = min_val

        if isinstance(max_val, pd.Timestamp):
            max_date = max_val.date()
        elif hasattr(max_val, 'date') and not isinstance(max_val, datetime.date):
            max_date = max_val.date()
        else:
            max_date = max_val

        date_range = st.date_input(
            get_text('date_range', lang),
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )

    with col2:
        # Transaction type filter
        transaction_types = ['All'] + list(df['type'].unique())
        selected_type = st.selectbox(
            "💱 Transaction Type",
            transaction_types
        )

    with col3:
        # Category filter
        categories = ['All'] + list(df['category'].unique())
        selected_category = st.selectbox(
            "🏷️ Category",
            categories
        )

    return date_range, selected_type, selected_category


def apply_cash_flow_filters(df, date_range, selected_type, selected_category):
    """Apply filters to cash flow data"""
    if df.empty:
        return df

    filtered_df = df.copy()

    # Date filter
    if date_range and len(date_range) == 2:
        start_date, end_date = date_range
        filtered_df = filtered_df[
            (filtered_df['date'].dt.date >= start_date) &
            (filtered_df['date'].dt.date <= end_date)
        ]

    # Type filter
    if selected_type != 'All':
        filtered_df = filtered_df[filtered_df['type'] == selected_type]

    # Category filter
    if selected_category != 'All':
        filtered_df = filtered_df[filtered_df['category'] == selected_category]

    return filtered_df


def render_cash_flow_summary(df):
    """Render cash flow summary metrics"""
    if df.empty:
        st.warning(get_text('no_financial_data', st.session_state.get('language', 'en')))
        return

    # Calculate key metrics
    total_inflows = df[df['amount'] > 0]['amount'].sum()
    total_outflows = abs(df[df['amount'] < 0]['amount'].sum())
    net_flow = total_inflows - total_outflows

    # Time period
    date_range_days = (df['date'].max() - df['date'].min()).days + 1
    avg_daily_flow = net_flow / max(date_range_days, 1)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("💰 Total Inflows", f"{total_inflows:,.2f}")

    with col2:
        st.metric("💸 Total Outflows", f"{total_outflows:,.2f}")

    with col3:
        st.metric("📊 Net Cash Flow", f"{net_flow:,.2f}",
                 delta_color="normal" if net_flow >= 0 else "inverse")

    with col4:
        st.metric("⚡ Avg Daily Flow", f"{avg_daily_flow:,.2f}")


def render_cash_flow_charts(df):
    """Render cash flow visualization charts"""
    if df.empty:
        st.info(get_text('no_financial_data', st.session_state.get('language', 'en')))
        return

    # Create tabs for different chart views (Removed Running Balance and Trends)
    tab1, tab2 = st.tabs(["📊 Daily Flows", "🥧 Flow Categories"])

    with tab1:
        st.subheader("📊 Daily Cash Flows")

        # Group by date and sum amounts
        daily_flows = df.groupby(df['date'].dt.date)['amount'].sum().reset_index()
        daily_flows['flow_type'] = daily_flows['amount'].apply(lambda x: 'Inflow' if x > 0 else 'Outflow')
        daily_flows['abs_amount'] = abs(daily_flows['amount'])

        # Daily flow chart
        fig_daily = px.bar(
            daily_flows,
            x='date',
            y='amount',
            color='flow_type',
            title="Daily Cash Flows",
            color_discrete_map={'Inflow': '#00D4AA', 'Outflow': '#FF6B6B'},
            hover_data=['abs_amount']
        )

        fig_daily.update_layout(height=400)
        st.plotly_chart(fig_daily, use_container_width=True)

    with tab2:
        st.subheader("🥧 Cash Flow by Category")

        col1, col2 = st.columns(2)

        with col1:
            # Inflows by category
            inflows = df[df['amount'] > 0].groupby('category')['amount'].sum().reset_index()
            if not inflows.empty:
                fig_in = px.pie(
                    inflows,
                    values='amount',
                    names='category',
                    title="Inflows by Category",
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                st.plotly_chart(fig_in, use_container_width=True)
            else:
                st.info(get_text('no_financial_data', st.session_state.get('language', 'en')))

        with col2:
            # Outflows by category
            outflows = df[df['amount'] < 0].copy()
            outflows['amount'] = abs(outflows['amount'])
            outflows_grouped = outflows.groupby('category')['amount'].sum().reset_index()

            if not outflows_grouped.empty:
                fig_out = px.pie(
                    outflows_grouped,
                    values='amount',
                    names='category',
                    title="Outflows by Category",
                    color_discrete_sequence=px.colors.qualitative.Set2
                )
                st.plotly_chart(fig_out, use_container_width=True)
            else:
                st.info(get_text('no_financial_data', st.session_state.get('language', 'en')))


def render_cash_flow_table(df):
    """Render detailed cash flow transaction table"""
    if df.empty:
        return

    st.subheader("📋 Detailed Cash Flow Transactions")

    # Prepare display dataframe
    display_df = df.copy()
    display_df['date'] = display_df['date'].dt.date

    # Format columns for display
    display_columns = ['date', 'amount', 'type', 'category', 'source']
    if 'Account Name' in display_df.columns:
        display_columns.append('Account Name')
    if 'Payment Purpose' in display_df.columns:
        display_columns.append('Payment Purpose')

    display_df = display_df[display_columns].sort_values('date', ascending=False)

    # Show summary stats (before formatting amount)
    col1, col2 = st.columns([3, 1])

    with col2:
        st.markdown("**📊 Summary:**")
        st.write(f"Total Records: {len(display_df)}")
        st.write(f"Date Range: {display_df['date'].min()} to {display_df['date'].max()}")

        inflow_count = len(display_df[display_df['amount'] > 0])
        outflow_count = len(display_df[display_df['amount'] < 0])
        st.write(f"Inflows: {inflow_count}")
        st.write(f"Outflows: {outflow_count}")

    # Apply pagination (100 records per page) before formatting
    page_df, pagination_info = paginate_dataframe(display_df, page_size=100, key_prefix="cashflow_detail")

    # Format amount for display
    page_df['amount'] = page_df['amount'].apply(lambda x: f"{x:,.2f}")

    st.dataframe(
        page_df,
        use_container_width=True,
        hide_index=True
    )

    # Render pagination controls at the bottom
    render_pagination_controls(pagination_info, key_prefix="cashflow_detail")


def main() -> None:
    lang = st.session_state.get('language', 'en')
    st.title("📈 Cash Flow Analytics")
    st.caption("Monitor your cash inflows and outflows with detailed analysis and projections")

    # Get available data
    data_sources = get_available_data()

    # Check if any data is available
    has_data = any(df is not None and not df.empty for df in data_sources.values())

    if not has_data:
        st.warning(get_text('no_processed_data', lang))
        st.page_link("pages/file_upload.py", label=get_text('go_to_file_upload_short', lang), icon="📁")
        return

    # Prepare cash flow data
    cash_flow_df = prepare_cash_flow_data(data_sources)

    if cash_flow_df.empty:
        st.warning(get_text('no_financial_data', lang))
        st.info("💡 Ensure your data contains date and amount columns for cash flow analysis.")
        return

    # Render filters
    date_range, selected_type, selected_category = render_cash_flow_filters(cash_flow_df)

    # Apply filters
    filtered_df = apply_cash_flow_filters(cash_flow_df, date_range, selected_type, selected_category)

    if filtered_df.empty:
        st.warning("No data matches the selected filters.")
        return

    # Summary metrics
    render_cash_flow_summary(filtered_df)

    st.divider()

    # Charts
    render_cash_flow_charts(filtered_df)

    st.divider()

    # Detailed table
    render_cash_flow_table(filtered_df)


if __name__ == "__main__":
    main()
