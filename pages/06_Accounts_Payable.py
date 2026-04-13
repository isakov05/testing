import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
from translations import get_text
from auth.db_authenticator import protect_page
from utils.pagination import paginate_dataframe, render_pagination_controls

st.set_page_config(page_title="Accounts Payable", page_icon="🧾", layout="wide")

protect_page()



def get_ap_data():
    """Get data needed for accounts payable analysis from database including reconciliation"""
    from utils.db_operations import get_ap_with_reconciliation

    # Get user_id from session state
    user_id = st.session_state.get('user_id')
    if not user_id:
        return {
            'invoices_in': None,
            'bank_statements': None,
            'reconciliation_in': None
        }

    # Load data from database (includes reconciliation)
    data = get_ap_with_reconciliation(user_id)

    invoices_in = data['invoices_in']
    bank_statements = data['bank_transactions']
    reconciliation_in = data['reconciliation_in']

    # Convert date columns to datetime for consistency
    if not invoices_in.empty and 'Document Date' in invoices_in.columns:
        invoices_in['date'] = pd.to_datetime(invoices_in['Document Date'], errors='coerce')
    if not bank_statements.empty and 'date' in bank_statements.columns:
        bank_statements['date'] = pd.to_datetime(bank_statements['date'], errors='coerce')
    if not reconciliation_in.empty and 'report_date' in reconciliation_in.columns:
        reconciliation_in['report_date'] = pd.to_datetime(reconciliation_in['report_date'], errors='coerce')

    return {
        'invoices_in': invoices_in if not invoices_in.empty else None,
        'bank_statements': bank_statements if not bank_statements.empty else None,
        'reconciliation_in': reconciliation_in if not reconciliation_in.empty else None
    }


def find_amount_column(df):
    """Find the appropriate amount column"""
    amount_columns = [
        'Стоимость поставки с учётом НДС', 'Supply Value (incl. VAT)',
        'amount', 'Amount', 'Сумма', 'Стоимость', 'Cost', 'Total'
    ]

    for col in amount_columns:
        if col in df.columns:
            return col
    return None


def process_ap_data(invoices_df, bank_df):
    """Process accounts payable data from invoices and bank statements"""
    if invoices_df is None or invoices_df.empty:
        return pd.DataFrame()

    ap_df = invoices_df.copy()

    # Standardize amount column
    amount_col = find_amount_column(ap_df)
    if amount_col:
        ap_df['invoice_amount'] = pd.to_numeric(ap_df[amount_col], errors='coerce').fillna(0.0)
    else:
        ap_df['invoice_amount'] = 0.0

    # Standardize date column
    if 'date' not in ap_df.columns:
        date_candidates = ['Дата документ', 'Document Date', 'Date']
        for col in date_candidates:
            if col in ap_df.columns:
                ap_df['date'] = pd.to_datetime(ap_df[col], errors='coerce')
                break

    # Standardize supplier information
    ap_df['supplier_inn'] = ap_df.get('Продавец (ИНН или ПИНФЛ)', ap_df.get('Seller (Tax ID or PINFL)', '')).astype(str).str.replace('.0', '', regex=False).str.strip()
    ap_df['supplier_name'] = ap_df.get('Продавец (наименование)', ap_df.get('Seller (Name)', ''))
    ap_df['invoice_number'] = ap_df.get('Номер документ', ap_df.get('Document Number', ''))
    ap_df['status'] = ap_df.get('СТАТУС', ap_df.get('Status', 'Unknown'))

    # Identify returns and financial discounts
    ap_df['is_return'] = False
    ap_df['is_financial_discount'] = False

    if 'invoice_number' in ap_df.columns:
        ap_df['is_return'] = ap_df['invoice_number'].astype(str).str.contains('возврат|return', case=False, na=False)
        ap_df['is_financial_discount'] = ap_df['invoice_number'].astype(str).str.contains('фин\.скидка|фин скидка|financial discount', case=False, na=False)

    # Calculate total payments per supplier from bank statements (simple sum approach)
    # float64: proportional payment allocation uses non-integer floats; int64 would raise.
    ap_df['paid_amount'] = 0.0
    ap_df['payment_date'] = pd.NaT
    ap_df['payment_status'] = 'Unpaid'

    if bank_df is not None and not bank_df.empty:
        # Ensure bank INN is also string
        if 'inn' in bank_df.columns:
            bank_df['inn'] = bank_df['inn'].astype(str).str.replace('.0', '', regex=False).str.strip()
        if 'Taxpayer ID (INN)' in bank_df.columns:
            bank_df['Taxpayer ID (INN)'] = bank_df['Taxpayer ID (INN)'].astype(str).str.replace('.0', '', regex=False).str.strip()

        # Calculate total payments per supplier (sum all negative amounts for each INN)
        # Uses enhanced matching: direct INN + payment purpose search
        from utils.db_operations import find_payments_for_inn

        for supplier_inn in ap_df['supplier_inn'].unique():
            if pd.notna(supplier_inn) and supplier_inn != '' and supplier_inn != 'nan':
                # Get all payments for this supplier (including third-party payments)
                supplier_payments = find_payments_for_inn(
                    bank_df,
                    supplier_inn,
                    transaction_type='outgoing'
                )

                if not supplier_payments.empty and 'amount' in supplier_payments.columns:
                    # Sum all amounts (already filtered for negative by find_payments_for_inn) - use abs to get positive values
                    total_paid = abs(supplier_payments['amount'].sum())

                    # Get latest payment date (check both 'date' and 'transaction_date' columns)
                    if 'transaction_date' in supplier_payments.columns:
                        latest_payment_date = supplier_payments['transaction_date'].max()
                    elif 'date' in supplier_payments.columns:
                        latest_payment_date = supplier_payments['date'].max()
                    else:
                        latest_payment_date = pd.NaT

                    # Calculate total invoiced for this supplier
                    supplier_invoices = ap_df[ap_df['supplier_inn'] == supplier_inn]
                    total_invoiced = supplier_invoices['invoice_amount'].sum()

                    # Distribute payment proportionally across invoices
                    if total_invoiced > 0:
                        for idx in supplier_invoices.index:
                            invoice_amount = ap_df.at[idx, 'invoice_amount']
                            proportion = invoice_amount / total_invoiced
                            ap_df.at[idx, 'paid_amount'] = total_paid * proportion
                            ap_df.at[idx, 'payment_date'] = latest_payment_date

    # Calculate outstanding amounts and aging
    ap_df['outstanding_amount'] = ap_df['invoice_amount'] - ap_df['paid_amount']
    ap_df['outstanding_amount'] = ap_df['outstanding_amount'].clip(lower=0)

    # Update payment status based on outstanding amount
    ap_df.loc[ap_df['outstanding_amount'] <= 1, 'payment_status'] = 'Paid'
    ap_df.loc[
        (ap_df['outstanding_amount'] > 1) & (ap_df['paid_amount'] > 0),
        'payment_status'
    ] = 'Partial'

    # Calculate days since invoice (payable age)
    today = datetime.now()
    ap_df['days_outstanding'] = (today - ap_df['date']).dt.days

    # Age buckets
    def get_age_bucket(days):
        if pd.isna(days):
            return 'Unknown'
        elif days <= 30:
            return '0-30 days'
        elif days <= 60:
            return '31-60 days'
        elif days <= 90:
            return '61-90 days'
        else:
            return '90+ days'

    ap_df['age_bucket'] = ap_df['days_outstanding'].apply(get_age_bucket)

    # Calculate payment terms (assuming 30 days standard)
    ap_df['due_date'] = ap_df['date'] + timedelta(days=30)
    ap_df['days_until_due'] = (ap_df['due_date'] - today).dt.days
    ap_df['overdue'] = ap_df['days_until_due'] < 0

    return ap_df


def merge_reconciliation_with_ap(ap_df, reconciliation_df):
    """
    Merge reconciliation data with AP calculations to show comparison.

    Reconciliation record_type='IN' matches with seller_inn (our suppliers)
    """
    if reconciliation_df is None or reconciliation_df.empty:
        # No reconciliation data, return AP as-is
        ap_df['recon_outstanding'] = None
        ap_df['recon_report_date'] = None
        ap_df['variance'] = None
        ap_df['has_reconciliation'] = False
        return ap_df

    # Clean and standardize INNs in reconciliation data
    reconciliation_df = reconciliation_df.copy()
    reconciliation_df['Customer_INN'] = reconciliation_df['Customer_INN'].astype(str).str.replace('.0', '', regex=False).str.strip()

    # Group reconciliation by counterparty INN (sum if multiple records)
    recon_summary = reconciliation_df.groupby('Customer_INN').agg({
        'Outstanding_Amount': 'sum',
        'report_date': 'max'  # Get latest report date
    }).reset_index()
    recon_summary.columns = ['supplier_inn', 'recon_outstanding', 'recon_report_date']

    # Merge with AP data
    ap_with_recon = ap_df.merge(
        recon_summary,
        on='supplier_inn',
        how='left'
    )

    # Calculate variance
    ap_with_recon['variance'] = ap_with_recon['outstanding_amount'] - ap_with_recon['recon_outstanding']

    # Mark rows that have reconciliation data
    ap_with_recon['has_reconciliation'] = ap_with_recon['recon_outstanding'].notna()

    return ap_with_recon


def render_reconciliation_comparison_ap(ap_df):
    """Render reconciliation vs calculated outstanding comparison for AP"""
    if 'has_reconciliation' not in ap_df.columns or not ap_df['has_reconciliation'].any():
        return

    st.subheader("🔍 Reconciliation vs Calculated Outstanding")

    # Filter to suppliers with reconciliation data
    recon_suppliers = ap_df[ap_df['has_reconciliation'] == True].copy()

    if recon_suppliers.empty:
        return

    # Group by supplier
    comparison = recon_suppliers.groupby(['supplier_inn', 'supplier_name']).agg({
        'outstanding_amount': 'sum',
        'recon_outstanding': 'first',
        'variance': 'first',
        'recon_report_date': 'first'
    }).reset_index()

    # Calculate variance percentage
    comparison['variance_pct'] = (
        (comparison['variance'] / comparison['recon_outstanding'] * 100)
        .fillna(0)
        .round(1)
    )

    # Sort by absolute variance
    comparison['abs_variance'] = comparison['variance'].abs()
    comparison = comparison.sort_values('abs_variance', ascending=False)

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_calc = comparison['outstanding_amount'].sum()
        st.metric("💻 Calculated Outstanding", f"{total_calc:,.2f}")

    with col2:
        total_recon = comparison['recon_outstanding'].sum()
        st.metric("📋 Reconciliation Outstanding", f"{total_recon:,.2f}")

    with col3:
        total_variance = comparison['variance'].sum()
        variance_color = "normal" if abs(total_variance) < 1000 else "inverse"
        st.metric("⚠️ Total Variance", f"{total_variance:,.2f}", delta_color=variance_color)

    with col4:
        suppliers_with_variance = len(comparison[comparison['abs_variance'] > 100])
        st.metric("🔎 Suppliers with Variance", f"{suppliers_with_variance}")

    # Variance chart
    st.write("#### Top Variances")
    top_variances = comparison.head(10)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        name='Calculated',
        x=top_variances['supplier_name'],
        y=top_variances['outstanding_amount'],
        marker_color='lightblue'
    ))

    fig.add_trace(go.Bar(
        name='Reconciliation',
        x=top_variances['supplier_name'],
        y=top_variances['recon_outstanding'],
        marker_color='lightcoral'
    ))

    fig.update_layout(
        barmode='group',
        height=400,
        xaxis_tickangle=-45,
        title="Calculated vs Reconciliation Outstanding (Top 10)"
    )

    st.plotly_chart(fig, use_container_width=True)

    # Detailed comparison table
    with st.expander("📊 Detailed Reconciliation Comparison", expanded=False):
        display_comparison = comparison[[
            'supplier_name', 'supplier_inn',
            'outstanding_amount', 'recon_outstanding',
            'variance', 'variance_pct', 'recon_report_date'
        ]].copy()

        display_comparison.columns = [
            'Supplier Name', 'Supplier INN',
            'Calculated Outstanding', 'Recon Outstanding',
            'Variance', 'Variance %', 'Report Date'
        ]

        st.dataframe(
            display_comparison,
            use_container_width=True,
            hide_index=True,
            column_config={
                'Calculated Outstanding': st.column_config.NumberColumn(format="%.2f"),
                'Recon Outstanding': st.column_config.NumberColumn(format="%.2f"),
                'Variance': st.column_config.NumberColumn(format="%.2f"),
                'Variance %': st.column_config.NumberColumn(format="%.1f%%"),
                'Report Date': st.column_config.DateColumn(format="YYYY-MM-DD")
            }
        )


def render_ap_summary(ap_df):
    """Render accounts payable summary metrics"""
    if ap_df.empty:
        st.warning("No accounts payable data available")
        return

    # Calculate key metrics
    total_invoices = len(ap_df)
    total_ap = ap_df['outstanding_amount'].sum()
    total_invoiced = ap_df['invoice_amount'].sum()
    total_paid = ap_df['paid_amount'].sum()

    paid_invoices = len(ap_df[ap_df['payment_status'] == 'Paid'])
    payment_rate = (paid_invoices / total_invoices * 100) if total_invoices > 0 else 0

    # Days Payable Outstanding (DPO)
    if total_invoiced > 0:
        avg_daily_purchases = total_invoiced / max((ap_df['date'].max() - ap_df['date'].min()).days, 1)
        dpo = total_ap / avg_daily_purchases if avg_daily_purchases > 0 else 0
    else:
        dpo = 0

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("💸 Total Outstanding", f"{total_ap:,.2f}")

    with col2:
        st.metric("📋 Total Invoices", f"{total_invoices:,}")

    with col3:
        st.metric("📈 Payment Rate", f"{payment_rate:.1f}%")

    with col4:
        st.metric("⏰ DPO (Days)", f"{dpo:.0f}")

    # Additional metrics
    col5, col6, col7, col8 = st.columns(4)

    with col5:
        st.metric("🧾 Total Invoiced", f"{total_invoiced:,.2f}")

    with col6:
        st.metric("✅ Total Paid", f"{total_paid:,.2f}")

    with col7:
        partial_payments = len(ap_df[ap_df['payment_status'] == 'Partial'])
        st.metric("⚠️ Partial Payments", f"{partial_payments:,}")

    with col8:
        overdue_invoices = len(ap_df[(ap_df['overdue'] == True) & (ap_df['outstanding_amount'] > 0)])
        st.metric("🚨 Overdue", f"{overdue_invoices:,}")


def render_ap_aging_analysis(ap_df):
    """Render accounts payable aging analysis"""
    st.subheader("📅 Accounts Payable Aging")

    if ap_df.empty:
        st.info("No data available for aging analysis")
        return

    # Filter to only outstanding amounts
    outstanding_df = ap_df[ap_df['outstanding_amount'] > 0].copy()

    if outstanding_df.empty:
        st.info("🎉 No outstanding payables! All invoices have been paid.")
        return

    # Aging summary
    aging_summary = outstanding_df.groupby('age_bucket').agg({
        'outstanding_amount': ['count', 'sum'],
        'invoice_amount': 'sum'
    }).round(2)

    aging_summary.columns = ['Invoice Count', 'Outstanding Amount', 'Original Amount']

    # Reorder age buckets
    bucket_order = ['0-30 days', '31-60 days', '61-90 days', '90+ days', 'Unknown']
    aging_summary = aging_summary.reindex([b for b in bucket_order if b in aging_summary.index])

    # Charts
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        # Outstanding amount by age bucket
        fig_amount = px.bar(
            aging_summary.reset_index(),
            x='age_bucket',
            y='Outstanding Amount',
            title="Outstanding Payables by Age",
            color='Outstanding Amount',
            color_continuous_scale='Blues'
        )
        fig_amount.update_layout(height=400)
        st.plotly_chart(fig_amount, use_container_width=True)

    with chart_col2:
        # Invoice count by age bucket
        fig_count = px.pie(
            aging_summary.reset_index(),
            values='Invoice Count',
            names='age_bucket',
            title="Payable Count by Age"
        )
        st.plotly_chart(fig_count, use_container_width=True)

    # Aging detail table
    with st.expander("📋 Detailed Aging Analysis", expanded=False):
        aging_summary['Payment %'] = (
            (aging_summary['Original Amount'] - aging_summary['Outstanding Amount']) /
            aging_summary['Original Amount'] * 100
        ).round(1)
        st.dataframe(aging_summary, use_container_width=True)


def render_supplier_analysis(ap_df):
    """Render supplier-level accounts payable analysis"""
    st.subheader("🏭 Supplier Analysis")

    if ap_df.empty:
        st.info("No data available for supplier analysis")
        return

    # Get the most common/recent name for each INN to handle spelling variations
    supplier_names = ap_df.groupby('supplier_inn')['supplier_name'].agg(
        lambda x: x.mode()[0] if not x.mode().empty else x.iloc[0]
    ).reset_index()

    # Supplier summary - GROUP BY INN ONLY to avoid duplicates from name variations
    # Calculate returns and discounts per supplier
    returns_by_supplier = ap_df[ap_df['is_return']].groupby('supplier_inn')['invoice_amount'].sum()
    discounts_by_supplier = ap_df[ap_df['is_financial_discount']].groupby('supplier_inn')['invoice_amount'].sum()

    supplier_summary = ap_df.groupby('supplier_inn').agg({
        'invoice_amount': ['count', 'sum'],
        'outstanding_amount': 'sum',
        'paid_amount': 'sum',
        'days_outstanding': 'mean'
    }).round(2)

    supplier_summary.columns = ['Invoice Count', 'Total Invoiced', 'Outstanding', 'Paid', 'Avg Days Outstanding']
    supplier_summary = supplier_summary.reset_index()

    # Add returns and discounts
    supplier_summary['Returns'] = supplier_summary['supplier_inn'].map(returns_by_supplier).fillna(0)
    supplier_summary['Financial Discounts'] = supplier_summary['supplier_inn'].map(discounts_by_supplier).fillna(0)

    # Merge with supplier names
    supplier_summary = supplier_summary.merge(supplier_names, on='supplier_inn', how='left')

    # Calculate payment rate per supplier
    supplier_summary['Payment %'] = (
        supplier_summary['Paid'] / supplier_summary['Total Invoiced'] * 100
    ).fillna(0).round(1)

    # Filter suppliers with outstanding amounts
    suppliers_with_outstanding = supplier_summary[supplier_summary['Outstanding'] > 0].copy()
    suppliers_with_outstanding = suppliers_with_outstanding.sort_values('Outstanding', ascending=False).head(20)

    # Format supplier display
    suppliers_with_outstanding_display = suppliers_with_outstanding.copy()
    suppliers_with_outstanding_display['Supplier'] = suppliers_with_outstanding_display.apply(
        lambda row: f"{row['supplier_name']} (INN: {row['supplier_inn']})" if pd.notna(row['supplier_inn']) and row['supplier_inn'] != '' else row['supplier_name'],
        axis=1
    )

    col1, col2 = st.columns(2)

    with col1:
        # Top suppliers by outstanding amount
        if not suppliers_with_outstanding_display.empty:
            fig_outstanding = px.bar(
                suppliers_with_outstanding_display.head(10),
                x='Outstanding',
                y='Supplier',
                title="Top 10 Suppliers by Outstanding Amount",
                orientation='h'
            )
            fig_outstanding.update_layout(height=400)
            st.plotly_chart(fig_outstanding, use_container_width=True)
        else:
            st.info("No suppliers with outstanding amounts")

    with col2:
        # Payment rate by supplier volume
        if not suppliers_with_outstanding_display.empty:
            fig_payment_rate = px.scatter(
                suppliers_with_outstanding_display,
                x='Total Invoiced',
                y='Payment %',
                size='Outstanding',
                hover_data=['Supplier', 'Invoice Count'],
                title="Supplier Payment Rate vs Volume",
                color='Outstanding',
                color_continuous_scale='RdYlBu'
            )
            fig_payment_rate.update_layout(height=400)
            st.plotly_chart(fig_payment_rate, use_container_width=True)

    # Supplier detail table
    with st.expander("📋 Detailed Supplier Analysis", expanded=False):
        display_suppliers = suppliers_with_outstanding_display[
            ['Supplier', 'Invoice Count', 'Total Invoiced', 'Outstanding', 'Paid', 'Payment %', 'Avg Days Outstanding']
        ]
        st.dataframe(display_suppliers, use_container_width=True)


def render_cash_flow_forecast(ap_df):
    """Render cash flow forecast based on payables"""
    st.subheader("💰 Cash Flow Forecast")

    if ap_df.empty:
        st.info("No data available for cash flow forecast")
        return

    # Filter unpaid invoices
    unpaid_df = ap_df[ap_df['outstanding_amount'] > 0].copy()

    if unpaid_df.empty:
        st.info("No outstanding payables to forecast")
        return

    # Generate forecast for next 90 days
    today = datetime.now()
    forecast_days = 90
    forecast_dates = [today + timedelta(days=x) for x in range(forecast_days)]

    # Calculate daily cash requirements
    daily_cash_req = []

    for date in forecast_dates:
        # Invoices due on this date
        due_today = unpaid_df[unpaid_df['due_date'].dt.date == date.date()]
        cash_req = due_today['outstanding_amount'].sum()
        daily_cash_req.append({
            'date': date,
            'cash_required': cash_req,
            'cumulative_cash': 0  # Will calculate below
        })

    forecast_df = pd.DataFrame(daily_cash_req)

    # Calculate cumulative cash requirements
    forecast_df['cumulative_cash'] = forecast_df['cash_required'].cumsum()

    # Charts
    col1, col2 = st.columns(2)

    with col1:
        # Daily cash requirements
        fig_daily = px.bar(
            forecast_df[forecast_df['cash_required'] > 0],
            x='date',
            y='cash_required',
            title="Daily Cash Requirements (Next 90 Days)",
            color='cash_required',
            color_continuous_scale='Reds'
        )
        fig_daily.update_layout(height=400)
        st.plotly_chart(fig_daily, use_container_width=True)

    with col2:
        # Cumulative cash requirements
        fig_cumulative = px.line(
            forecast_df,
            x='date',
            y='cumulative_cash',
            title="Cumulative Cash Requirements",
            markers=True
        )
        fig_cumulative.update_layout(height=400)
        st.plotly_chart(fig_cumulative, use_container_width=True)

    # Key forecast metrics
    forecast_col1, forecast_col2, forecast_col3 = st.columns(3)

    with forecast_col1:
        next_7_days = forecast_df[forecast_df['date'] <= today + timedelta(days=7)]['cash_required'].sum()
        st.metric("💸 Next 7 Days", f"{next_7_days:,.2f}")

    with forecast_col2:
        next_30_days = forecast_df[forecast_df['date'] <= today + timedelta(days=30)]['cash_required'].sum()
        st.metric("💸 Next 30 Days", f"{next_30_days:,.2f}")

    with forecast_col3:
        next_90_days = forecast_df['cash_required'].sum()
        st.metric("💸 Next 90 Days", f"{next_90_days:,.2f}")


def render_payment_trends(ap_df):
    """Render payment trends and patterns"""
    st.subheader("📈 Payment Trends")

    if ap_df.empty:
        st.info("No data available for payment trends")
        return

    # Monthly payment analysis
    paid_invoices = ap_df[ap_df['payment_status'] == 'Paid'].copy()

    if not paid_invoices.empty and 'payment_date' in paid_invoices.columns:
        # Group by month
        paid_invoices['payment_month'] = paid_invoices['payment_date'].dt.to_period('M').astype(str)
        paid_invoices['invoice_month'] = paid_invoices['date'].dt.to_period('M').astype(str)

        monthly_payments = paid_invoices.groupby('payment_month')['paid_amount'].agg(['count', 'sum']).reset_index()
        monthly_payments.columns = ['Month', 'Payment Count', 'Total Payments']

        # Payment trend chart
        fig_trend = px.line(
            monthly_payments,
            x='Month',
            y='Total Payments',
            title="Monthly Payment Trends",
            markers=True,
            line_shape='spline'
        )
        fig_trend.update_layout(height=400)
        st.plotly_chart(fig_trend, use_container_width=True)

        # Payment timing analysis
        paid_invoices['days_to_payment'] = (paid_invoices['payment_date'] - paid_invoices['date']).dt.days

        if 'days_to_payment' in paid_invoices.columns:
            st.write("**⏱️ Payment Timing Analysis**")

            timing_col1, timing_col2 = st.columns(2)

            with timing_col1:
                avg_days_to_payment = paid_invoices['days_to_payment'].mean()
                median_days_to_payment = paid_invoices['days_to_payment'].median()

                st.metric("Avg Days to Payment", f"{avg_days_to_payment:.1f}")
                st.metric("Median Days to Payment", f"{median_days_to_payment:.1f}")

            with timing_col2:
                # Payment timing histogram
                fig_timing = px.histogram(
                    paid_invoices,
                    x='days_to_payment',
                    title="Payment Timing Distribution",
                    nbins=20,
                    color_discrete_sequence=['#FF6B6B']
                )
                fig_timing.update_layout(height=300)
                st.plotly_chart(fig_timing, use_container_width=True)


def render_ap_detail_table(ap_df):
    """Render detailed accounts payable table"""
    if ap_df.empty:
        return

    st.subheader("📋 Detailed Accounts Payable")

    # Filter options
    filter_col1, filter_col2, filter_col3 = st.columns(3)

    with filter_col1:
        payment_status_filter = st.selectbox(
            "Payment Status",
            options=['All', 'Unpaid', 'Partial', 'Paid']
        )

    with filter_col2:
        age_filter = st.selectbox(
            "Age Bucket",
            options=['All'] + list(ap_df['age_bucket'].unique())
        )

    with filter_col3:
        min_amount = st.number_input(
            "Min Outstanding Amount",
            min_value=0.0,
            value=0.0,
            step=100.0
        )

    # Apply filters
    filtered_df = ap_df.copy()

    if payment_status_filter != 'All':
        filtered_df = filtered_df[filtered_df['payment_status'] == payment_status_filter]

    if age_filter != 'All':
        filtered_df = filtered_df[filtered_df['age_bucket'] == age_filter]

    filtered_df = filtered_df[filtered_df['outstanding_amount'] >= min_amount]

    # Remove duplicates based on invoice_number and date
    if 'invoice_number' in filtered_df.columns and 'date' in filtered_df.columns:
        filtered_df = filtered_df.drop_duplicates(subset=['invoice_number', 'date'], keep='first')

    # Prepare display columns
    display_columns = [
        'date', 'due_date', 'invoice_number', 'supplier_inn', 'supplier_name', 'invoice_amount',
        'paid_amount', 'outstanding_amount', 'days_outstanding', 'overdue',
        'payment_status', 'age_bucket'
    ]

    available_columns = [col for col in display_columns if col in filtered_df.columns]
    display_df = filtered_df[available_columns].sort_values('due_date', ascending=True)

    # Show summary
    summary_col1, summary_col2 = st.columns([3, 1])

    with summary_col2:
        st.markdown("**📊 Filtered Summary:**")
        st.write(f"Records: {len(display_df)}")
        st.write(f"Outstanding: {display_df['outstanding_amount'].sum():,.2f}")
        overdue_count = len(display_df[display_df.get('overdue', False) == True])
        st.write(f"Overdue: {overdue_count}")

    # Apply pagination (100 records per page)
    page_df, pagination_info = paginate_dataframe(display_df, page_size=100, key_prefix="ap_detail")

    st.dataframe(
        page_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "supplier_inn": st.column_config.TextColumn(
                "Supplier INN",
                width="medium"
            ),
            "supplier_name": st.column_config.TextColumn(
                "Supplier Name",
                width="large"
            ),
            "invoice_amount": st.column_config.NumberColumn(
                "Invoice Amount",
                format="%.2f"
            ),
            "paid_amount": st.column_config.NumberColumn(
                "Paid Amount",
                format="%.2f"
            ),
            "outstanding_amount": st.column_config.NumberColumn(
                "Outstanding",
                format="%.2f"
            ),
            "date": st.column_config.DateColumn(
                "Invoice Date",
                format="YYYY-MM-DD"
            ),
            "due_date": st.column_config.DateColumn(
                "Due Date",
                format="YYYY-MM-DD"
            ),
            "overdue": st.column_config.CheckboxColumn("Overdue")
        }
    )

    # Render pagination controls at the bottom
    render_pagination_controls(pagination_info, key_prefix="ap_detail")


def main() -> None:

    # Get language preference
    lang = st.session_state.get('language', 'en')

    st.title("🧾 Accounts Payable")
    st.caption("Track outstanding supplier invoices, monitor payment obligations, and forecast cash requirements")

    # Get data
    ap_data = get_ap_data()
    invoices_df = ap_data['invoices_in']
    bank_df = ap_data['bank_statements']
    reconciliation_df = ap_data.get('reconciliation_in')

    if invoices_df is None or invoices_df.empty:
        st.warning("📁 No incoming invoice data available. Please upload and process invoice files first.")
        st.page_link("pages/file_upload.py", label="→ Go to File Upload", icon="📁")
        return

    # Process AP data
    with st.spinner("Processing accounts payable data..."):
        ap_df = process_ap_data(invoices_df, bank_df)

        # Merge with reconciliation data
        ap_df = merge_reconciliation_with_ap(ap_df, reconciliation_df)

    if ap_df.empty:
        st.warning("No accounts payable data could be processed.")
        return

    # Date filtering section
    st.subheader("📅 Date Filters" if lang == 'en' else "📅 Фильтры дат")

    # Get date range from data
    if 'date' in ap_df.columns and ap_df['date'].notna().any():
        # Handle both datetime and date objects - ensure we get date objects
        import datetime
        import pandas as pd
        min_val = ap_df['date'].min()
        max_val = ap_df['date'].max()

        # Convert to date objects for comparison
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

        # Set default start date to 2025-01-01 if within data range
        from datetime import date as dt_date
        default_start = dt_date(2025, 1, 1)
        if default_start < min_date:
            default_start = min_date
        elif default_start > max_date:
            default_start = min_date

        # Handle reset before widget renders
        if st.session_state.get("ap_reset_trigger"):
            st.session_state.ap_date_range = (min_date, max_date)
            st.session_state.ap_reset_trigger = False

        col1, col2, col3 = st.columns([3, 2, 1])

        with col1:
            date_range = st.date_input(
                "Date Range" if lang == 'en' else "Диапазон дат",
                value=(default_start, max_date),
                min_value=min_date,
                max_value=max_date,
                key="ap_date_range"
            )

            # Handle date range selection
            if isinstance(date_range, tuple) and len(date_range) == 2:
                start_date, end_date = date_range
            else:
                start_date = end_date = date_range if date_range else default_start

        with col2:
            st.write("")  # Spacer
            st.write("")  # Spacer
            if st.button("Reset" if lang == 'en' else "Сброс", key="ap_reset"):
                st.session_state.ap_reset_trigger = True
                st.rerun()

        # Validate date range
        if start_date > end_date:
            st.error("From date must be before To date" if lang == 'en' else "Дата 'с' должна быть раньше даты 'по'")
            return

        # # Filter data by date range based on selected filter type
        # if date_filter_type == "Invoice Date":
        #     ap_df = ap_df[
        #         (ap_df['date'].dt.date >= start_date) &
        #         (ap_df['date'].dt.date <= end_date)
        #     ]
        #     filter_desc = "invoice date" if lang == 'en' else "дате счета"
        # elif date_filter_type == "Payment Date":
        #     # Filter by payment date (only show invoices with payments in range)
        #     ap_df = ap_df[
        #         ap_df['payment_date'].notna() &
        #         (ap_df['payment_date'].dt.date >= start_date) &
        #         (ap_df['payment_date'].dt.date <= end_date)
        #     ]
        #     filter_desc = "payment date" if lang == 'en' else "дате платежа"
        # elif date_filter_type == "Either Date":
        #     # Show if either invoice date or payment date is in range
        #     invoice_in_range = (ap_df['date'].dt.date >= start_date) & (ap_df['date'].dt.date <= end_date)
        #     payment_in_range = ap_df['payment_date'].notna() & (ap_df['payment_date'].dt.date >= start_date) & (ap_df['payment_date'].dt.date <= end_date)
        #     ap_df = ap_df[invoice_in_range | payment_in_range]
        #     filter_desc = "invoice or payment date" if lang == 'en' else "дате счета или платежа"
        # else:  # Both Dates
        #     # Filter invoices by date range AND filter their payments to only those in range
        #     # This shows all invoices in range, but only considers payments that are also in range
        ap_df = ap_df[
            (ap_df['date'].dt.date >= start_date) &
            (ap_df['date'].dt.date <= end_date)
          ]

        # For invoices with payments outside the date range, set payment info to null
        payment_out_of_range = (
            ap_df['payment_date'].notna() &
            ((ap_df['payment_date'].dt.date < start_date) |
             (ap_df['payment_date'].dt.date > end_date))
          )
        ap_df.loc[payment_out_of_range, 'paid_amount'] = 0
        ap_df.loc[payment_out_of_range, 'payment_date'] = pd.NaT
        ap_df.loc[payment_out_of_range, 'payment_status'] = 'Unpaid'

        # Recalculate outstanding amounts
        ap_df['outstanding_amount'] = ap_df['invoice_amount'] - ap_df['paid_amount']
        ap_df['outstanding_amount'] = ap_df['outstanding_amount'].clip(lower=0)

        # Update payment status
        ap_df.loc[ap_df['outstanding_amount'] <= 1, 'payment_status'] = 'Paid'
        ap_df.loc[
            (ap_df['outstanding_amount'] > 1) & (ap_df['paid_amount'] > 0),
            'payment_status'
        ] = 'Partial'

        filter_desc = "invoice and payment dates (filtered separately)" if lang == 'en' else "датам счетов и платежей (раздельная фильтрация)"

        # Show filtered date range info
        total_records = len(ap_df)
        st.info(
            f"📊 Showing data from {start_date} to {end_date} by {filter_desc} ({total_records:,} invoices)" if lang == 'en'
            else f"📊 Показ данных с {start_date} по {end_date} по {filter_desc} ({total_records:,} счетов)"
        )

        if ap_df.empty:
            st.warning("No data available for the selected date range and filter type." if lang == 'en' else "Нет данных за выбранный период и тип фильтра.")
            return

    st.divider()

    # Reconciliation comparison (if data available)
    render_reconciliation_comparison_ap(ap_df)

    st.divider()

    # Summary metrics
    render_ap_summary(ap_df)

    st.divider()

    # Aging analysis
    render_ap_aging_analysis(ap_df)

    st.divider()

    # Supplier analysis
    render_supplier_analysis(ap_df)

    st.divider()

    # Cash flow forecast
    render_cash_flow_forecast(ap_df)

    st.divider()

    # Payment trends
    render_payment_trends(ap_df)

    st.divider()

    # Detailed table
    render_ap_detail_table(ap_df)


if __name__ == "__main__":
    main()


