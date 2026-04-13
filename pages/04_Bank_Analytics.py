import streamlit as st
import pandas as pd
import numpy as np
import datetime
from datetime import datetime as dt
from datetime import timedelta as td
from pages.file_upload import lang
import plotly.express as px  # pyright: ignore[reportMissingImports]
import plotly.graph_objects as go  # pyright: ignore[reportMissingImports]      
from plotly.subplots import make_subplots  # pyright: ignore[reportMissingImports]  
import json
import os
from typing import List, Dict, Optional

from translations import get_text
from auth.db_authenticator import protect_page

st.set_page_config(page_title="Bank Analytics", page_icon="🏦", layout="wide")

protect_page()



@st.cache_data
def load_column_mappings():
    """Load column name mappings"""
    try:
        dict_path = os.path.join(os.path.dirname(__file__), '..', 'dict_data', 'column_name_dict.json')
        with open(dict_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Could not load column mappings: {e}")
        return {}


@st.cache_data
def load_payment_codes():
    """Load payment codes mapping"""
    try:
        codes_path = os.path.join(os.path.dirname(__file__), '..', 'dict_data', 'bank_payment_codes.xlsx')
        df = pd.read_excel(codes_path)

        code_mapping = {}
        for _, row in df.iterrows():
            code = str(row['Код назначения платежа']).zfill(5)
            description = row.get('Unnamed: 2', '') or row.get('Наименование транша или назначения платежа', '')
            if pd.notna(description):
                code_mapping[code] = str(description).strip()

        return code_mapping
    except Exception as e:
        return {}


def get_bank_data():
    """Get bank statement data from session state"""
    return st.session_state.get('bank_statements_processed')


def get_language():
    """Get current language setting"""
    return st.session_state.get('language', 'en')


def categorize_payment_purpose(payment_purpose):
    """Enhanced payment purpose categorization"""
    if pd.isna(payment_purpose) or not isinstance(payment_purpose, str):
        return 'Unknown'

    purpose_lower = payment_purpose.lower()

    # Salary and wages
    if any(keyword in purpose_lower for keyword in [
        'зарплата', 'заработная', 'оплата труда', 'заработ', 'salary', 'wage'
    ]):
        return 'Salary & Wages'

    # Taxes
    elif any(keyword in purpose_lower for keyword in [
        'налог', 'ндфл', 'подоходный', 'социальный', 'ндс', 'tax'
    ]):
        return 'Taxes & Social Contributions'

    # Rent & Utilities
    elif any(keyword in purpose_lower for keyword in [
        'аренда', 'арендная', 'коммунальные', 'электроэнергия', 'газ', 'вода', 'rent', 'utilities'
    ]):
        return 'Rent & Utilities'

    # Bank services
    elif any(keyword in purpose_lower for keyword in [
        'комиссия', 'банковские услуги', 'обслуживание', 'bank', 'fee', 'commission'
    ]):
        return 'Bank Fees & Services'

    # Suppliers & vendors
    elif any(keyword in purpose_lower for keyword in [
        'поставщик', 'поставка', 'товар', 'материал', 'supplier', 'vendor'
    ]):
        return 'Suppliers & Purchases'

    # Customer payments
    elif any(keyword in purpose_lower for keyword in [
        'оплата за', 'косметик', 'медикамент', 'дори', 'customer', 'payment for'
    ]):
        return 'Customer Payments'

    # Loans & financing
    elif any(keyword in purpose_lower for keyword in [
        'займ', 'кредит', 'ссуда', 'loan', 'credit'
    ]):
        return 'Loans & Financing'

    # Office & operational
    elif any(keyword in purpose_lower for keyword in [
        'офис', 'хозяйственные', 'канцелярия', 'office', 'supplies'
    ]):
        return 'Office & Operational'

    # Marketing
    elif any(keyword in purpose_lower for keyword in [
        'реклама', 'маркетинг', 'продвижение', 'marketing', 'advertising'
    ]):
        return 'Marketing & Advertising'

    else:
        return 'Other'


def extract_counterparty_info(row):
    """Extract counterparty information from transaction data"""
    counterparty = row.get('Account Name', '')
    inn = row.get('inn', '') or row.get('Taxpayer ID (INN)', '')

    if pd.isna(counterparty):
        counterparty = 'Unknown'

    if pd.notna(inn) and inn != 0:
        return f"{counterparty} (INN: {inn})"

    return str(counterparty)


def render_money_flow_analysis(df, period_filter):
    """Render money in/out flow analysis"""
    lang = get_language()
    st.subheader(get_text('bank_money_flow_analysis', lang))

    if df.empty:
        st.info(get_text('bank_no_data', lang))
        return

    # Separate money in and money out
    money_in = df[df['amount'] > 0].copy() if 'amount' in df.columns else pd.DataFrame()
    money_out = df[df['amount'] < 0].copy() if 'amount' in df.columns else pd.DataFrame()

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_in = money_in['amount'].sum() if not money_in.empty else 0
        st.metric(get_text('bank_total_money_in', lang), f"{total_in:,.2f}")

    with col2:
        total_out = abs(money_out['amount'].sum()) if not money_out.empty else 0
        st.metric(get_text('bank_total_money_out', lang), f"{total_out:,.2f}")

    with col3:
        net_flow = total_in - total_out
        st.metric(get_text('bank_net_cash_flow_metric', lang), f"{net_flow:,.2f}",
                 delta_color="normal" if net_flow >= 0 else "inverse")

    with col4:
        transaction_count = len(df)
        st.metric(get_text('bank_total_transactions_metric', lang), f"{transaction_count:,}")

    # Flow analysis charts
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        if not money_in.empty:
            # Group money in by period
            if period_filter == "Daily":
                money_in['period'] = money_in['date'].dt.date
            elif period_filter == "Weekly":
                money_in['period'] = money_in['date'].dt.to_period('W').dt.start_time
            else:  # Monthly
                money_in['period'] = money_in['date'].dt.to_period('M').dt.start_time

            money_in_summary = money_in.groupby('period')['amount'].sum().reset_index()

            fig_in = px.bar(
                money_in_summary,
                x='period',
                y='amount',
                title=get_text('bank_money_in_trend', lang).format(period_filter),
                color='amount',
                color_continuous_scale='Greens'
            )
            fig_in.update_layout(
                title=get_text('bank_daily_flow_balance_title', lang),
                xaxis_title=get_text('label_date', lang),
                yaxis_title=get_text('label_amount_uzs', lang),
                hovermode='x unified',
                height=350
            )
            st.plotly_chart(fig_in, use_container_width=True)

    with chart_col2:
        if not money_out.empty:
            # Group money out by period
            if period_filter == "Daily":
                money_out['period'] = money_out['date'].dt.date
            elif period_filter == "Weekly":
                money_out['period'] = money_out['date'].dt.to_period('W').dt.start_time
            else:  # Monthly
                money_out['period'] = money_out['date'].dt.to_period('M').dt.start_time

            money_out_summary = money_out.groupby('period')['amount'].sum().abs().reset_index()

            fig_out = px.bar(
                money_out_summary,
                x='period',
                y='amount',
                title=get_text('bank_money_out_trend', lang).format(period_filter),
                color='amount',
                color_continuous_scale='Reds'
            )
            fig_out.update_layout(
                title=get_text('bank_money_out_trend', lang).format(period_filter),
                xaxis_title=get_text('label_date', lang),
                yaxis_title=get_text('label_amount_uzs', lang),
                hovermode='x unified',
                height=350
            )
            st.plotly_chart(fig_out, use_container_width=True)


def render_daily_cash_flow_and_balance(df):
    """Render daily cash flow with cumulative balance chart"""
    lang = get_language()
    st.subheader(get_text('bank_tab_timeline', lang))

    if df.empty:
        st.info(get_text('bank_no_data', lang))
        return

    # Prepare data similar to the reference code
    # Group by day and calculate credit/debit/amount
    daily_flow = df.groupby(df['date'].dt.date).agg({
        'amount': lambda x: x[x > 0].sum(),  # credit (inflows)
    }).reset_index()

    # Add debit calculation and rename columns
    daily_flow['debit'] = df.groupby(df['date'].dt.date)['amount'].apply(lambda x: abs(x[x < 0].sum())).values
    daily_flow['amount_total'] = df.groupby(df['date'].dt.date)['amount'].sum().values

    # Rename columns to match reference
    daily_flow.columns = ['day', 'credit', 'debit', 'amount']

    # Calculate net flow and cumulative
    daily_flow['net_flow'] = daily_flow['credit'] - daily_flow['debit']
    daily_flow['cumulative'] = daily_flow['net_flow'].cumsum()

    fig = go.Figure()

    # Add inflow area (green)
    fig.add_trace(go.Scatter(
        x=daily_flow['day'],
        y=daily_flow['credit'],
        name='Inflow',
        mode='lines',
        line=dict(color='green', width=2),
        fill='tozeroy',
        fillcolor='rgba(0,255,0,0.1)'
    ))

    # Add outflow area (red)
    fig.add_trace(go.Scatter(
        x=daily_flow['day'],
        y=daily_flow['debit'],
        name='Outflow',
        mode='lines',
        line=dict(color='red', width=2),
        fill='tozeroy',
        fillcolor='rgba(255,0,0,0.1)'
    ))

    # Add cumulative balance line (blue dashed)
    fig.add_trace(go.Scatter(
        x=daily_flow['day'],
        y=daily_flow['cumulative'],
        name='Cumulative Balance',
        mode='lines',
        line=dict(color='blue', width=3, dash='dash'),
        yaxis='y2'
    ))

    # Update layout exactly as in reference
    fig.update_layout(
        title=get_text('bank_daily_flow_balance_title', lang),
        xaxis_title=get_text('label_date', lang),
        yaxis_title=get_text('label_amount_uzs', lang),
        yaxis2=dict(
            title=get_text('label_cumulative_balance_uzs', lang),
            overlaying='y',
            side='right'
        ),
        hovermode='x unified',
        height=500
    )

    st.plotly_chart(fig, use_container_width=True)


def render_daily_flow_and_category_analysis(df):
    """Render Daily Flow and Flow Category analysis combined"""
    lang = get_language()
    st.subheader(get_text('bank_daily_flow_category', lang))

    if df.empty:
        st.info(get_text('bank_no_flow_data', lang))
        return

    # Create tabs for Daily Flows and Flow Categories
    tab1, tab2 = st.tabs([get_text('bank_tab_daily_flows', lang), get_text('bank_tab_flow_categories', lang)])

    with tab1:
        st.write("**" + get_text('bank_daily_cash_flows', lang) + "**")

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
        st.write("**" + get_text('bank_cash_flow_by_category', lang) + "**")

        # Add payment purpose categories
        df_copy = df.copy()
        df_copy['category'] = df_copy['Payment Purpose'].apply(categorize_payment_purpose) if 'Payment Purpose' in df_copy.columns else 'Unknown'

        col1, col2 = st.columns(2)

        with col1:
            # Inflows by category
            inflows = df_copy[df_copy['amount'] > 0].groupby('category')['amount'].sum().reset_index()
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
                st.info(get_text('bank_no_inflow_data', lang))

        with col2:
            # Outflows by category
            outflows = df_copy[df_copy['amount'] < 0].copy()
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
                st.info(get_text('bank_no_outflow_data', lang))


def render_counterparty_matrix(df):
    """Render counterparty interaction matrix"""
    st.subheader(get_text('bank_counterparty_matrix', get_language()))

    if df.empty:
        st.info(get_text('bank_no_counterparty_data', get_language()))
        return

    # Extract counterparty information (name and INN)
    df['counterparty'] = df.apply(extract_counterparty_info, axis=1)

    # Extract INN for grouping
    if 'inn' not in df.columns:
        # Try to extract INN from counterparty string or use available INN column
        if 'Taxpayer ID (INN)' in df.columns:
            df['counterparty_inn'] = df['Taxpayer ID (INN)'].astype(str).str.replace('.0', '', regex=False).str.strip()
        else:
            df['counterparty_inn'] = df['counterparty']  # Fallback to counterparty name
    else:
        df['counterparty_inn'] = df['inn'].astype(str).str.replace('.0', '', regex=False).str.strip()

    # Separate inbound and outbound transactions
    inbound = df[df['amount'] > 0].copy() if 'amount' in df.columns else pd.DataFrame()
    outbound = df[df['amount'] < 0].copy() if 'amount' in df.columns else pd.DataFrame()

    # Top counterparties analysis
    col1, col2 = st.columns(2)

    with col1:
        if not inbound.empty:
            st.write("**" + get_text('bank_top_senders', get_language()) + "**")
            # Group by INN and get first counterparty name for display
            top_senders = inbound.groupby('counterparty_inn').agg({
                'amount': ['count', 'sum'],
                'counterparty': 'first'
            }).round(2)
            top_senders.columns = ['Transactions', 'Total Amount', 'Name']
            top_senders = top_senders.sort_values('Total Amount', ascending=False).head(10)
            # Reorder columns to show Name first
            top_senders = top_senders[['Name', 'Transactions', 'Total Amount']]
            top_senders.index.name = 'INN'
            st.dataframe(top_senders, use_container_width=True)

    with col2:
        if not outbound.empty:
            st.write("**" + get_text('bank_top_recipients', get_language()) + "**")
            # Group by INN and get first counterparty name for display
            top_recipients = outbound.groupby('counterparty_inn').agg({
                'amount': ['count', 'sum'],
                'counterparty': 'first'
            }).round(2)
            top_recipients.columns = ['Transactions', 'Total Amount', 'Name']
            top_recipients['Total Amount'] = top_recipients['Total Amount'].abs()
            top_recipients = top_recipients.sort_values('Total Amount', ascending=False).head(10)
            # Reorder columns to show Name first
            top_recipients = top_recipients[['Name', 'Transactions', 'Total Amount']]
            top_recipients.index.name = 'INN'
            st.dataframe(top_recipients, use_container_width=True)

    # Transaction Amount Visualizations (Simplified for CEO view)
    if not df.empty:
        st.write("**" + get_text('bank_transaction_type_summary', get_language()) + "**")

        # Add payment purpose categories for heatmap
        df['category'] = df['Payment Purpose'].apply(categorize_payment_purpose) if 'Payment Purpose' in df.columns else 'Unknown'

        # Create two columns for side-by-side charts
        viz_col1, viz_col2 = st.columns(2)

        with viz_col1:
            # Simple Weekly Inflows vs Outflows Bar Chart
            st.write("**" + get_text('bank_weekly_by_type', get_language()) + "**")

            df['week'] = df['date'].dt.to_period('W').apply(lambda x: x.start_time.strftime('%Y-%m-%d'))

            weekly_inflows = df[df['amount'] > 0].groupby('week')['amount'].sum().reset_index()
            weekly_inflows.columns = ['Week', 'Amount']
            weekly_inflows['Type'] = 'Inflows'

            weekly_outflows = df[df['amount'] < 0].groupby('week')['amount'].sum().abs().reset_index()
            weekly_outflows.columns = ['Week', 'Amount']
            weekly_outflows['Type'] = 'Outflows'

            weekly_combined = pd.concat([weekly_inflows, weekly_outflows], ignore_index=True)

            if not weekly_combined.empty:
                fig_weekly = px.bar(
                    weekly_combined,
                    x='Week',
                    y='Amount',
                    color='Type',
                    title=get_text('bank_weekly_in_out_title', get_language()),
                    color_discrete_map={'Inflows': '#00D4AA', 'Outflows': '#FF6B6B'},
                    barmode='group'
                )
                fig_weekly.update_layout(height=400, xaxis_title="", showlegend=True)
                fig_weekly.update_xaxes(tickangle=-45)
                st.plotly_chart(fig_weekly, use_container_width=True)

        with viz_col2:
            # Category-based Heatmap (Net Flow by Category and Month)
            st.write("**" + get_text('bank_net_flow_cat_month', get_language()) + "**")

            df['month'] = df['date'].dt.to_period('M').astype(str)

            # Calculate net flow by category and month
            category_month_flow = df.groupby(['category', 'month'])['amount'].sum().reset_index()
            category_pivot = category_month_flow.pivot(index='category', columns='month', values='amount').fillna(0)

            if not category_pivot.empty:
                fig_heatmap = px.imshow(
                    category_pivot.values,
                    x=category_pivot.columns,
                    y=category_pivot.index,
                    title=get_text('bank_net_flow_cat_month', lang),
                    color_continuous_scale='RdYlGn',
                    aspect='auto',
                    labels=dict(color="Net Flow", x=get_text('label_month', lang), y="Category")
                )
                fig_heatmap.update_layout(height=400)
                st.plotly_chart(fig_heatmap, use_container_width=True)
                st.caption(get_text('bank_heatmap_note', lang))
            else:
                st.info(get_text('bank_not_enough_data_heatmap', lang))


def render_detailed_transactions_table(df):
    """Render detailed transaction table with summary stats"""
    st.subheader(get_text('bank_detailed_transactions', get_language()))

    if df.empty:
        st.info(get_text('bank_no_transaction_data', get_language()))
        return

    # Prepare display dataframe
    display_df = df.copy()

    # Ensure we have the necessary columns
    display_columns = ['date', 'amount']

    # Add type column (Inflow/Outflow)
    if 'amount' in display_df.columns:
        display_df['type'] = display_df['amount'].apply(lambda x: 'Inflow' if x > 0 else 'Outflow')
        display_columns.append('type')

    # Add category if available
    if 'category' not in display_df.columns and 'Payment Purpose' in display_df.columns:
        display_df['category'] = display_df['Payment Purpose'].apply(categorize_payment_purpose)

    if 'category' in display_df.columns:
        display_columns.append('category')

    # Add account name and payment purpose
    if 'Account Name' in display_df.columns:
        display_columns.append('Account Name')
    if 'Payment Purpose' in display_df.columns:
        display_columns.append('Payment Purpose')

    # Format date column
    display_df['date'] = pd.to_datetime(display_df['date']).dt.date

    # Filter only needed columns
    display_df = display_df[[col for col in display_columns if col in display_df.columns]].sort_values('date', ascending=False)

    # Show summary stats and table side by side
    col1, col2 = st.columns([3, 1])

    with col2:
        st.markdown(get_text('summary_header', get_language()))
        st.write(f"{get_text('total_records', get_language())}: {len(display_df)}")
        st.write(f"{get_text('date_range_summary', get_language())}: {display_df['date'].min()} to {display_df['date'].max()}")

        inflow_count = len(df[df['amount'] > 0])
        outflow_count = len(df[df['amount'] < 0])
        st.write(f"{get_text('bank_inflows', get_language())}: {inflow_count}")
        st.write(f"{get_text('bank_outflows', get_language())}: {outflow_count}")

    # Format amount for display (after counting)
    display_df['amount'] = display_df['amount'].apply(lambda x: f"{x:,.2f}")

    with col1:
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            height=600
        )


def render_filters():
    """Render filter controls"""
    st.subheader(get_text('filters', get_language()))

    filter_col1, filter_col2, filter_col3 = st.columns(3)

    with filter_col1:
        # Date range filter
        default_start = dt.now() - td(days=365)
        default_end = dt.now()

        date_range = st.date_input(
            get_text('date_range', get_language()),
            value=(default_start.date(), default_end.date()),
            help=get_text('date_range', get_language())
        )

    with filter_col2:
        # Period grouping
        period_filter = st.selectbox(
            get_text('period_grouping', get_language()),
            [get_text('period_daily', get_language()), get_text('period_weekly', get_language()), get_text('period_monthly', get_language())],
            index=2,  # Default to Monthly
            help=get_text('period_grouping', get_language())
        )

    with filter_col3:
        # Amount filter
        amount_filter = st.selectbox(
            get_text('bank_transaction_type_summary', get_language()),
            ["All Transactions", "Large Transactions (>1000)", "Small Transactions (<1000)"],
            help=get_text('bank_transaction_type_summary', get_language())
        )

    return date_range, period_filter, amount_filter


def apply_filters(df, date_range, amount_filter):
    """Apply selected filters to the dataframe"""
    if df.empty:
        return df

    filtered_df = df.copy()

    # Apply date filter
    if date_range and len(date_range) == 2:
        # Ensure datetimelike before using .dt
        if 'date' in filtered_df.columns:
            filtered_df['date'] = pd.to_datetime(filtered_df['date'], errors='coerce')
        start_date, end_date = date_range
        filtered_df = filtered_df[
            (filtered_df['date'].dt.date >= start_date) &
            (filtered_df['date'].dt.date <= end_date)
        ]

    # Apply amount filter
    if amount_filter == "Large Transactions (>1000)":
        filtered_df = filtered_df[abs(filtered_df['amount']) > 1000]
    elif amount_filter == "Small Transactions (<1000)":
        filtered_df = filtered_df[abs(filtered_df['amount']) <= 1000]

    return filtered_df


def main() -> None:

    lang = st.session_state.get('language', 'en')
    st.title(get_text('bank_title', lang))
    st.caption(get_text('explorer_subtitle', lang))

    # Get bank statement data
    bank_data = get_bank_data()

    if bank_data is None or bank_data.empty:
        st.warning(get_text('no_processed_data', lang))
        st.page_link("pages/file_upload.py", label=get_text('go_to_file_upload_short', lang), icon="📁")
        return

    # Render filters
    date_range, period_filter, amount_filter = render_filters()

    # Apply filters
    filtered_data = apply_filters(bank_data, date_range, amount_filter)

    if filtered_data.empty:
        st.warning(get_text('no_bank_txn_match_filters', lang))
        return

    st.divider()

    # Render analysis sections
    render_money_flow_analysis(filtered_data, period_filter)

    st.divider()

    # Render daily cash flow and cumulative balance chart
    render_daily_cash_flow_and_balance(filtered_data)

    st.divider()

    # Render Daily Flow and Category Analysis (combined from Cash Flow Analytics)
    render_daily_flow_and_category_analysis(filtered_data)

    st.divider()

    render_counterparty_matrix(filtered_data)

    st.divider()

    # Detailed transactions table
    render_detailed_transactions_table(filtered_data)

if __name__ == "__main__":
    main()


