import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
from io import BytesIO
from translations import get_text
from auth.db_authenticator import protect_page
from utils.pagination import paginate_dataframe, render_pagination_controls

st.set_page_config(page_title="Accounts Receivable", page_icon="💵", layout="wide")

protect_page()



def to_excel(df):
    """Convert DataFrame to Excel format"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()


def get_ar_data():
    """Get data needed for accounts receivable analysis from database including reconciliation"""
    from utils.db_operations import get_ar_with_reconciliation

    # Get user_id from session state
    user_id = st.session_state.get('user_id')
    if not user_id:
        return {
            'invoices_out': None,
            'bank_statements': None,
            'reconciliation_out': None
        }

    # Load data from database (includes reconciliation)
    data = get_ar_with_reconciliation(user_id)

    invoices_out = data['invoices_out']
    bank_statements = data['bank_transactions']
    reconciliation_out = data['reconciliation_out']

    # Convert date columns to datetime for consistency
    if not invoices_out.empty and 'Document Date' in invoices_out.columns:
        invoices_out['date'] = pd.to_datetime(invoices_out['Document Date'], errors='coerce')
    if not bank_statements.empty and 'date' in bank_statements.columns:
        bank_statements['date'] = pd.to_datetime(bank_statements['date'], errors='coerce')
    if not reconciliation_out.empty and 'report_date' in reconciliation_out.columns:
        reconciliation_out['report_date'] = pd.to_datetime(reconciliation_out['report_date'], errors='coerce')

    return {
        'invoices_out': invoices_out if not invoices_out.empty else None,
        'bank_statements': bank_statements if not bank_statements.empty else None,
        'reconciliation_out': reconciliation_out if not reconciliation_out.empty else None
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


def process_ar_data(invoices_df, bank_df):
    """Process accounts receivable data from invoices and bank statements"""
    if invoices_df is None or invoices_df.empty:
        return pd.DataFrame()

    ar_df = invoices_df.copy()

    # Standardize amount column
    amount_col = find_amount_column(ar_df)
    if amount_col:
        ar_df['invoice_amount'] = pd.to_numeric(ar_df[amount_col], errors='coerce').fillna(0.0)
    else:
        ar_df['invoice_amount'] = 0.0

    print("AR DF",ar_df.columns)

    # Standardize date column
    if 'date' not in ar_df.columns:
        date_candidates = ['Дата документ', 'Document Date', 'Date']
        for col in date_candidates:
            if col in ar_df.columns:
                ar_df['date'] = pd.to_datetime(ar_df[col], errors='coerce')
                break

    # Standardize customer information
    ar_df['customer_inn'] = ar_df.get('Покупатель (ИНН или ПИНФЛ)', ar_df.get('Buyer (Tax ID or PINFL)', '')).astype(str).str.replace('.0', '', regex=False).str.strip()
    ar_df['customer_name'] = ar_df.get('Покупатель (наименование)', ar_df.get('Buyer (Name)', ''))
    ar_df['invoice_number'] = ar_df.get('Номер документ', ar_df.get('Document Number', ''))
    ar_df['status'] = ar_df.get('СТАТУС', ar_df.get('Status', 'Unknown'))

    # Identify returns and financial discounts
    ar_df['is_return'] = False
    ar_df['is_financial_discount'] = False

    if 'invoice_number' in ar_df.columns:
        ar_df['is_return'] = ar_df['invoice_number'].astype(str).str.contains('возврат|return', case=False, na=False)
        ar_df['is_financial_discount'] = ar_df['invoice_number'].astype(str).str.contains('фин\.скидка|фин скидка|financial discount', case=False, na=False)

    # Calculate total payments per customer from bank statements (simple sum approach)
    # Use float64: proportional allocation assigns non-integer floats; int64 column would raise.
    ar_df['paid_amount'] = 0.0
    ar_df['payment_date'] = pd.NaT
    ar_df['payment_status'] = 'Unpaid'

    if bank_df is not None and not bank_df.empty:
        # Ensure bank INN is also string
        if 'inn' in bank_df.columns:
            bank_df['inn'] = bank_df['inn'].astype(str).str.replace('.0', '', regex=False).str.strip()
        if 'Taxpayer ID (INN)' in bank_df.columns:
            bank_df['Taxpayer ID (INN)'] = bank_df['Taxpayer ID (INN)'].astype(str).str.replace('.0', '', regex=False).str.strip()

        # Calculate total payments per customer (sum all positive amounts for each INN)
        # Uses enhanced matching: direct INN + payment purpose search
        from utils.db_operations import find_payments_for_inn

        for customer_inn in ar_df['customer_inn'].unique():
            if pd.notna(customer_inn) and customer_inn != '' and customer_inn != 'nan':
                # Get all payments for this customer (including third-party payments)
                customer_payments = find_payments_for_inn(
                    bank_df,
                    customer_inn,
                    transaction_type='incoming'
                )

                if not customer_payments.empty and 'amount' in customer_payments.columns:
                    # Sum all amounts (already filtered for positive by find_payments_for_inn)
                    total_paid = customer_payments['amount'].sum()

                    # Get latest payment date (check both 'date' and 'transaction_date' columns)
                    if 'transaction_date' in customer_payments.columns:
                        latest_payment_date = customer_payments['transaction_date'].max()
                    elif 'date' in customer_payments.columns:
                        latest_payment_date = customer_payments['date'].max()
                    else:
                        latest_payment_date = pd.NaT

                    # Calculate total invoiced for this customer
                    customer_invoices = ar_df[ar_df['customer_inn'] == customer_inn]
                    total_invoiced = customer_invoices['invoice_amount'].sum()

                    # Distribute payment proportionally across invoices
                    if total_invoiced > 0:
                        for idx in customer_invoices.index:
                            invoice_amount = ar_df.at[idx, 'invoice_amount']
                            proportion = invoice_amount / total_invoiced
                            ar_df.at[idx, 'paid_amount'] = total_paid * proportion
                            ar_df.at[idx, 'payment_date'] = latest_payment_date

    # Calculate outstanding amounts and aging
    ar_df['outstanding_amount'] = ar_df['invoice_amount'] - ar_df['paid_amount']
    ar_df['outstanding_amount'] = ar_df['outstanding_amount'].clip(lower=0)

    # Update payment status based on outstanding amount
    ar_df.loc[ar_df['outstanding_amount'] <= 1, 'payment_status'] = 'Paid'
    ar_df.loc[
        (ar_df['outstanding_amount'] > 1) & (ar_df['paid_amount'] > 0),
        'payment_status'
    ] = 'Partial'

    # Calculate due date (default 30 days from invoice date)
    ar_df['due_date'] = ar_df['date'] + pd.Timedelta(days=30)
    
    # Calculate days since invoice and days overdue from due date
    today = datetime.now()
    ar_df['days_outstanding'] = (today - ar_df['date']).dt.days
    ar_df['days_overdue'] = (today - ar_df['due_date']).dt.days
    
    # Age buckets based on due date (not invoice date)
    def get_age_bucket(days_overdue):
        if pd.isna(days_overdue):
            return 'Unknown'
        elif days_overdue < 0:
            return 'Not Due'
        elif days_overdue <= 30:
            return '0-30 days overdue'
        elif days_overdue <= 60:
            return '31-60 days overdue'
        elif days_overdue <= 90:
            return '61-90 days overdue'
        else:
            return '90+ days overdue'

    ar_df['age_bucket'] = ar_df['days_overdue'].apply(get_age_bucket)

    return ar_df


def merge_reconciliation_with_ar(ar_df, reconciliation_df):
    """
    Merge reconciliation data with AR calculations to show comparison.

    Reconciliation record_type='OUT' matches with buyer_inn (our customers)
    """
    if reconciliation_df is None or reconciliation_df.empty:
        # No reconciliation data, return AR as-is
        ar_df['recon_outstanding'] = None
        ar_df['recon_report_date'] = None
        ar_df['variance'] = None
        ar_df['has_reconciliation'] = False
        return ar_df

    # Clean and standardize INNs in reconciliation data
    reconciliation_df = reconciliation_df.copy()
    reconciliation_df['Customer_INN'] = reconciliation_df['Customer_INN'].astype(str).str.replace('.0', '', regex=False).str.strip()

    # Group reconciliation by counterparty INN (sum if multiple records)
    recon_summary = reconciliation_df.groupby('Customer_INN').agg({
        'Outstanding_Amount': 'sum',
        'report_date': 'max'  # Get latest report date
    }).reset_index()
    recon_summary.columns = ['customer_inn', 'recon_outstanding', 'recon_report_date']

    # Merge with AR data
    ar_with_recon = ar_df.merge(
        recon_summary,
        on='customer_inn',
        how='left'
    )

    # Calculate variance
    ar_with_recon['variance'] = ar_with_recon['outstanding_amount'] - ar_with_recon['recon_outstanding']

    # Mark rows that have reconciliation data
    ar_with_recon['has_reconciliation'] = ar_with_recon['recon_outstanding'].notna()

    return ar_with_recon


def render_reconciliation_comparison(ar_df):
    """Render reconciliation vs calculated outstanding comparison"""
    if 'has_reconciliation' not in ar_df.columns or not ar_df['has_reconciliation'].any():
        return

    st.subheader("🔍 Reconciliation vs Calculated Outstanding")

    # Filter to customers with reconciliation data
    recon_customers = ar_df[ar_df['has_reconciliation'] == True].copy()

    if recon_customers.empty:
        return

    # Group by customer
    comparison = recon_customers.groupby(['customer_inn', 'customer_name']).agg({
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
        customers_with_variance = len(comparison[comparison['abs_variance'] > 100])
        st.metric("🔎 Customers with Variance", f"{customers_with_variance}")

    # Variance chart
    st.write("#### Top Variances")
    top_variances = comparison.head(10)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        name='Calculated',
        x=top_variances['customer_name'],
        y=top_variances['outstanding_amount'],
        marker_color='lightblue'
    ))

    fig.add_trace(go.Bar(
        name='Reconciliation',
        x=top_variances['customer_name'],
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
            'customer_name', 'customer_inn',
            'outstanding_amount', 'recon_outstanding',
            'variance', 'variance_pct', 'recon_report_date'
        ]].copy()

        display_comparison.columns = [
            'Customer Name', 'Customer INN',
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


def render_ar_summary(ar_df):
    """Render accounts receivable summary metrics"""
    if ar_df.empty:
        st.warning(get_text('ar_no_ar_data', st.session_state.get('language', 'en')))
        return

    # Calculate key metrics
    total_invoices = len(ar_df)
    total_ar = ar_df['outstanding_amount'].sum()
    total_invoiced = ar_df['invoice_amount'].sum()
    total_collected = ar_df['paid_amount'].sum()

    paid_invoices = len(ar_df[ar_df['payment_status'] == 'Paid'])
    collection_rate = (paid_invoices / total_invoices * 100) if total_invoices > 0 else 0

    # Days Sales Outstanding (DSO)
    if total_invoiced > 0:
        avg_daily_sales = total_invoiced / max((ar_df['date'].max() - ar_df['date'].min()).days, 1)
        dso = total_ar / avg_daily_sales if avg_daily_sales > 0 else 0
    else:
        dso = 0

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(get_text('ar_metric_total_outstanding', st.session_state.get('language', 'en')) if 'ar_metric_total_outstanding' in globals() else "💰 Total Outstanding", f"{total_ar:,.2f}")

    with col2:
        st.metric(get_text('ar_metric_total_invoices', st.session_state.get('language', 'en')) if 'ar_metric_total_invoices' in globals() else "📋 Total Invoices", f"{total_invoices:,}")

    with col3:
        st.metric(get_text('ar_collection_rate', st.session_state.get('language', 'en')) if 'ar_collection_rate' in globals() else "📈 Collection Rate", f"{collection_rate:.1f}%")

    with col4:
        st.metric(get_text('ar_metric_dso_days', st.session_state.get('language', 'en')), f"{dso:.0f}")

    # Additional metrics
    col5, col6, col7, col8 = st.columns(4)

    with col5:
        st.metric(get_text('label_total_amount', st.session_state.get('language', 'en')) if 'label_total_amount' in globals() else "💵 Total Invoiced", f"{total_invoiced:,.2f}")

    with col6:
        st.metric("✅ " + get_text('ar_metric_total_collected', st.session_state.get('language', 'en')), f"{total_collected:,.2f}")

    with col7:
        partial_invoices = len(ar_df[ar_df['payment_status'] == 'Partial'])
        st.metric("⚠️ " + get_text('ar_metric_partial_payments', st.session_state.get('language', 'en')), f"{partial_invoices:,}")

    with col8:
        overdue_invoices = len(ar_df[(ar_df['days_outstanding'] > 30) & (ar_df['outstanding_amount'] > 0)])
        st.metric("🚨 " + get_text('ar_metric_overdue_30', st.session_state.get('language', 'en')), f"{overdue_invoices:,}")


def render_aging_analysis(ar_df):
    """Render accounts receivable aging analysis"""
    st.subheader("📅 Accounts Receivable Aging")

    if ar_df.empty:
        st.info(get_text('ar_no_aging_data', st.session_state.get('language', 'en')))
        return

    # Filter to only outstanding amounts
    outstanding_df = ar_df[ar_df['outstanding_amount'] > 0].copy()

    if outstanding_df.empty:
        st.info(get_text('ar_no_outstanding_all_paid', st.session_state.get('language', 'en')))
        return

    # Aging summary
    aging_summary = outstanding_df.groupby('age_bucket').agg({
        'outstanding_amount': ['count', 'sum'],
        'invoice_amount': 'sum'
    }).round(2)

    aging_summary.columns = ['Invoice Count', 'Outstanding Amount', 'Original Amount']

    # Reorder age buckets
    bucket_order = ['Not Due', '0-30 days overdue', '31-60 days overdue', '61-90 days overdue', '90+ days overdue', 'Unknown']
    aging_summary = aging_summary.reindex([b for b in bucket_order if b in aging_summary.index])

    # Charts
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        # Outstanding amount by age bucket
        fig_amount = px.bar(
            aging_summary.reset_index(),
            x='age_bucket',
            y='Outstanding Amount',
            title="Outstanding Amount by Age",
            color='Outstanding Amount',
            color_continuous_scale='Reds'
        )
        fig_amount.update_layout(height=400)
        st.plotly_chart(fig_amount, use_container_width=True)

    with chart_col2:
        # Invoice count by age bucket
        fig_count = px.pie(
            aging_summary.reset_index(),
            values='Invoice Count',
            names='age_bucket',
            title="Invoice Count by Age"
        )
        st.plotly_chart(fig_count, use_container_width=True)

    # Aging detail table
    with st.expander("📋 Detailed Aging Analysis", expanded=False):
        aging_summary['Collection %'] = (
            (aging_summary['Original Amount'] - aging_summary['Outstanding Amount']) /
            aging_summary['Original Amount'] * 100
        ).round(1)
        st.dataframe(aging_summary, use_container_width=True)


def render_customer_analysis(ar_df):
    """Render customer-level accounts receivable analysis"""
    st.subheader(get_text('ar_customer_analysis_header', st.session_state.get('language', 'en')))

    if ar_df.empty:
        st.info(get_text('ar_no_customer_analysis', st.session_state.get('language', 'en')))
        return

    # Get the most common/recent name for each INN to handle spelling variations
    customer_names = ar_df.groupby('customer_inn')['customer_name'].agg(
        lambda x: x.mode()[0] if not x.mode().empty else x.iloc[0]
    ).reset_index()

    # Customer summary - GROUP BY INN ONLY to avoid duplicates from name variations
    # Calculate returns and discounts per customer
    returns_by_customer = ar_df[ar_df['is_return']].groupby('customer_inn')['invoice_amount'].sum()
    discounts_by_customer = ar_df[ar_df['is_financial_discount']].groupby('customer_inn')['invoice_amount'].sum()

    customer_summary = ar_df.groupby('customer_inn').agg({
        'invoice_amount': ['count', 'sum'],
        'outstanding_amount': 'sum',
        'paid_amount': 'sum',
        'days_outstanding': 'mean'
    }).round(2)

    customer_summary.columns = ['Invoice Count', 'Total Invoiced', 'Outstanding', 'Collected', 'Avg Days Outstanding']
    customer_summary = customer_summary.reset_index()

    # Add returns and discounts
    customer_summary['Returns'] = customer_summary['customer_inn'].map(returns_by_customer).fillna(0)
    customer_summary['Financial Discounts'] = customer_summary['customer_inn'].map(discounts_by_customer).fillna(0)

    # Merge with customer names
    customer_summary = customer_summary.merge(customer_names, on='customer_inn', how='left')

    # Calculate collection rate per customer
    customer_summary['Collection %'] = (
        customer_summary['Collected'] / customer_summary['Total Invoiced'] * 100
    ).fillna(0).round(1)

    # Filter customers with outstanding amounts
    customers_with_outstanding = customer_summary[customer_summary['Outstanding'] > 0].copy()
    customers_with_outstanding = customers_with_outstanding.sort_values('Outstanding', ascending=False).head(20)

    # Format customer display
    customers_with_outstanding_display = customers_with_outstanding.copy()
    customers_with_outstanding_display['Customer'] = customers_with_outstanding_display.apply(
        lambda row: f"{row['customer_name']} (INN: {row['customer_inn']})" if pd.notna(row['customer_inn']) and row['customer_inn'] != '' else row['customer_name'],
        axis=1
    )

    col1, col2 = st.columns(2)

    with col1:
        # Top customers by outstanding amount
        if not customers_with_outstanding_display.empty:
            fig_outstanding = px.bar(
                customers_with_outstanding_display.head(10),
                x='Outstanding',
                y='Customer',
                title=get_text('ar_top_outstanding_chart', st.session_state.get('language', 'en')),
                orientation='h'
            )
            fig_outstanding.update_layout(height=400)
            st.plotly_chart(fig_outstanding, use_container_width=True)
        else:
            st.info(get_text('ar_no_customers_outstanding', st.session_state.get('language', 'en')))

    with col2:
        # Collection rate by customer (for customers with outstanding amounts)
        if not customers_with_outstanding_display.empty:
            fig_collection = px.scatter(
                customers_with_outstanding_display,
                x='Total Invoiced',
                y='Collection %',
                size='Outstanding',
                hover_data=['Customer', 'Invoice Count'],
                title=get_text('ar_customer_collection_vs_volume', st.session_state.get('language', 'en')),
                color='Outstanding',
                color_continuous_scale='RdYlGn_r'
            )
            fig_collection.update_layout(height=400)
            st.plotly_chart(fig_collection, use_container_width=True)

    # Customer detail table
    with st.expander(get_text('ar_customer_analysis_details', st.session_state.get('language', 'en')) if 'ar_customer_analysis_details' in globals() else "📋 Detailed Customer Analysis", expanded=False):
        display_customers = customers_with_outstanding_display[
            ['Customer', 'Invoice Count', 'Total Invoiced', 'Outstanding', 'Collected', 'Collection %', 'Avg Days Outstanding']
        ]
        # Translate column headers
        lang = st.session_state.get('language', 'en')
        header_map = {
            'Customer': get_text('col_customer', lang),
            'Invoice Count': get_text('col_invoice_count', lang),
            'Total Invoiced': get_text('col_total_invoiced', lang),
            'Outstanding': get_text('col_outstanding', lang),
            'Collected': get_text('col_collected', lang) if 'col_collected' in globals() else 'Collected',
            'Collection %': get_text('col_collection_percent', lang),
            'Avg Days Outstanding': get_text('col_avg_days_outstanding', lang),
        }
        st.dataframe(display_customers.rename(columns=header_map), use_container_width=True)


def render_customer_payment_analysis(customer_df, lang='en'):
    """Render comprehensive customer payment analysis table with download"""
    # Customer Payment Analysis Table
    st.header(get_text("ar_customer_analysis", lang))

    if not customer_df.empty:
        # Sort by outstanding amount (highest first)
        customer_df_sorted = customer_df.sort_values('Outstanding', ascending=False)

        # Format for display
        display_df = customer_df_sorted.copy()
        display_df['Total_Invoiced'] = display_df['Total_Invoiced'].apply(lambda x: f"{x:,.0f} UZS")

        # Format bank payment columns if they exist
        if 'Paid_Bank1' in display_df.columns:
            display_df['Paid_Bank1'] = display_df['Paid_Bank1'].apply(lambda x: f"{x:,.0f} UZS")
        if 'Paid_Bank2' in display_df.columns:
            display_df['Paid_Bank2'] = display_df['Paid_Bank2'].apply(lambda x: f"{x:,.0f} UZS")

        display_df['Total_Paid'] = display_df['Total_Paid'].apply(lambda x: f"{x:,.0f} UZS")
        display_df['Returns'] = display_df['Returns'].apply(lambda x: f"{x:,.0f} UZS")
        display_df['Financial_Discounts'] = display_df['Financial_Discounts'].apply(lambda x: f"{x:,.0f} UZS")
        display_df['Outstanding'] = display_df['Outstanding'].apply(lambda x: f"{x:,.0f} UZS")
        display_df['Payment_Rate'] = display_df['Payment_Rate'].apply(lambda x: f"{x:.1f}%")

        # Rename columns for better display
        display_df = display_df.rename(columns={
            'Customer_INN': 'Customer INN',
            'Customer_Name': 'Customer Name',
            'Total_Invoiced': 'Total Invoiced',
            'Paid_Bank1': 'Paid Bank 1',
            'Paid_Bank2': 'Paid Bank 2',
            'Total_Paid': 'Total Paid',
            'Returns': 'Returns',
            'Financial_Discounts': 'Financial Discounts',
            'Outstanding': 'Outstanding',
            'Invoice_Count': 'Invoice Count',
            'Payment_Rate': 'Payment Rate'
        })

        st.dataframe(display_df, hide_index=True, use_container_width=True)

        # Download as XLSX (use the unformatted data for accuracy)
        download_df = customer_df_sorted.rename(columns={
            'Customer_INN': 'ИНН',
            'Customer_Name': 'Наименование клиента',
            'Total_Invoiced': 'Выставлено к оплате',
            'Paid_Bank1': 'Получено Банк 1',
            'Paid_Bank2': 'Получено Банк 2',
            'Total_Paid': 'Получено',
            'Returns': 'Возвраты',
            'Financial_Discounts': 'Финансовые скидки',
            'Outstanding': 'Задолженность',
            'Invoice_Count': 'Количество счетов',
            'Payment_Rate': 'Процент оплаты'
        }).copy()

        # Convert rate to percentage number
        if 'Payment_Rate' in download_df.columns:
            download_df['Процент оплаты'] = (download_df['Payment_Rate'] * 1.0).round(2)
            download_df.drop(columns=['Payment_Rate'], inplace=True, errors='ignore')

        xlsx_bytes = to_excel(download_df)
        st.download_button(
            label=("Download as XLSX" if lang == 'en' else "Скачать XLSX"),
            data=xlsx_bytes,
            file_name=f"customer_payment_analysis_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # Top debtors chart
        st.subheader(get_text("ar_top_outstanding_header", lang))
        top_debtors = customer_df_sorted.head(10)

        if not top_debtors.empty:
            fig = px.bar(
                top_debtors,
                x='Outstanding',
                y='Customer_Name',
                orientation='h',
                title=get_text('ar_top_outstanding_chart', lang),
                labels={'Outstanding': get_text('label_outstanding_amount_uzs', lang), 'Customer_Name': get_text('label_customer', lang)}
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(get_text("ar_no_customer_data", lang))


def render_collection_trends(ar_df):
    """Render collection trends and payment patterns"""
    st.subheader(get_text('ar_collection_trends', st.session_state.get('language', 'en')))

    if ar_df.empty:
        st.info(get_text('ar_no_collection_trends', st.session_state.get('language', 'en')))
        return

    # Monthly collection analysis
    paid_invoices = ar_df[ar_df['payment_status'] == 'Paid'].copy()

    if not paid_invoices.empty and 'payment_date' in paid_invoices.columns:
        # Convert date columns to datetime to use .dt accessor
        paid_invoices['payment_date'] = pd.to_datetime(paid_invoices['payment_date'])
        paid_invoices['date'] = pd.to_datetime(paid_invoices['date'])

        # Group by month
        paid_invoices['payment_month'] = paid_invoices['payment_date'].dt.to_period('M').astype(str)
        paid_invoices['invoice_month'] = paid_invoices['date'].dt.to_period('M').astype(str)

        monthly_collections = paid_invoices.groupby('payment_month')['paid_amount'].agg(['count', 'sum']).reset_index()
        monthly_collections.columns = ['Month', 'Payment Count', 'Total Collections']

        # Collection trend chart
        fig_trend = px.line(
            monthly_collections,
            x='Month',
            y='Total Collections',
            title="Monthly Collection Trends",
            markers=True,
            line_shape='spline'
        )
        fig_trend.update_layout(height=400)
        st.plotly_chart(fig_trend, use_container_width=True)

        # Payment timing analysis
        paid_invoices['days_to_payment'] = (paid_invoices['payment_date'] - paid_invoices['date']).dt.days

        if 'days_to_payment' in paid_invoices.columns:
            st.write(get_text('ar_payment_timing_header', st.session_state.get('language', 'en')))

            timing_col1, timing_col2 = st.columns(2)

            with timing_col1:
                avg_days_to_payment = paid_invoices['days_to_payment'].mean()
                median_days_to_payment = paid_invoices['days_to_payment'].median()

                st.metric(get_text('ar_avg_days_to_payment', st.session_state.get('language', 'en')), f"{avg_days_to_payment:.1f}")
                st.metric(get_text('ar_median_days_to_payment', st.session_state.get('language', 'en')), f"{median_days_to_payment:.1f}")

            with timing_col2:
                # Payment timing histogram
                fig_timing = px.histogram(
                    paid_invoices,
                    x='days_to_payment',
                    title=get_text('ar_payment_timing_distribution', st.session_state.get('language', 'en')),
                    nbins=20,
                    color_discrete_sequence=['#00D4AA']
                )
                fig_timing.update_layout(height=300)
                st.plotly_chart(fig_timing, use_container_width=True)


def render_ar_detail_table(ar_df):
    """Render detailed accounts receivable table"""
    if ar_df.empty:
        return

    st.subheader(get_text('ar_detailed_ar', st.session_state.get('language', 'en')))

    # Filter options
    filter_col1, filter_col2, filter_col3 = st.columns(3)

    with filter_col1:
        payment_status_filter = st.selectbox(
            get_text('ar_payment_status', st.session_state.get('language', 'en')),
            options=['All', 'Unpaid', 'Partial', 'Paid']
        )

    with filter_col2:
        age_filter = st.selectbox(
            get_text('ar_age_bucket', st.session_state.get('language', 'en')),
            options=['All'] + sorted(ar_df['age_bucket'].unique())
        )

    with filter_col3:
        min_amount = st.number_input(
            get_text('ar_min_outstanding_amount', st.session_state.get('language', 'en')),
            min_value=0.0,
            value=0.0,
            step=100.0
        )

    # Add counterparty (customer) filter
    st.write("#### " + ("Counterparty Filter" if st.session_state.get('language', 'en') == 'en' else "Фильтр контрагента"))
    
    # Get unique customers grouped by INN (pick most common/recent name for each INN)
    customer_data = ar_df[['customer_inn', 'customer_name']].copy()
    customer_data = customer_data.dropna(subset=['customer_inn'])
    customer_data = customer_data[customer_data['customer_inn'] != '']
    
    # Group by INN and get the most common name (mode), or first if no mode
    unique_customers = customer_data.groupby('customer_inn')['customer_name'].agg(
        lambda x: x.mode()[0] if not x.mode().empty else x.iloc[0]
    ).reset_index()
    unique_customers.columns = ['customer_inn', 'customer_name']
    unique_customers = unique_customers.sort_values('customer_name')
    
    # Create display format: "Customer Name (INN: 123456789)"
    customer_options = ['All'] + [
        f"{row['customer_name']} (INN: {row['customer_inn']})" 
        for _, row in unique_customers.iterrows()
    ]
    
    selected_customer = st.selectbox(
        "Select Counterparty" if st.session_state.get('language', 'en') == 'en' else "Выберите контрагента",
        options=customer_options,
        key="ar_counterparty_filter"
    )

    # Apply filters
    filtered_df = ar_df.copy()

    if payment_status_filter != 'All':
        filtered_df = filtered_df[filtered_df['payment_status'] == payment_status_filter]

    if age_filter != 'All':
        filtered_df = filtered_df[filtered_df['age_bucket'] == age_filter]

    filtered_df = filtered_df[filtered_df['outstanding_amount'] >= min_amount]
    
    # Apply counterparty filter
    if selected_customer != 'All':
        # Extract INN from selected option (format: "Name (INN: 123456789)")
        selected_inn = selected_customer.split('INN: ')[-1].rstrip(')')
        filtered_df = filtered_df[filtered_df['customer_inn'] == selected_inn]

    # Prepare display columns - include due_date and days_overdue
    display_columns = [
        'date', 'due_date', 'invoice_number', 'customer_name', 'invoice_amount',
        'paid_amount', 'outstanding_amount', 'days_outstanding', 'days_overdue',
        'payment_status', 'age_bucket'
    ]

    available_columns = [col for col in display_columns if col in filtered_df.columns]

    # Remove duplicates based on invoice number and date
    if 'invoice_number' in filtered_df.columns and 'date' in filtered_df.columns:
        filtered_df = filtered_df.drop_duplicates(subset=['invoice_number', 'date'], keep='first')

    display_df = filtered_df[available_columns].sort_values('outstanding_amount', ascending=False)

    # Show summary
    summary_col1, summary_col2 = st.columns([3, 1])

    with summary_col2:
        lang = st.session_state.get('language', 'en')
        st.markdown(get_text('ar_filtered_summary', lang))
        st.write(f"{get_text('ar_filtered_records', lang)}: {len(display_df)}")
        st.write(f"{get_text('ar_filtered_outstanding', lang)}: {display_df['outstanding_amount'].sum():,.2f}")
        
        # Show average days overdue if the column exists
        if 'days_overdue' in display_df.columns:
            avg_overdue = display_df[display_df['days_overdue'] > 0]['days_overdue'].mean()
            if pd.notna(avg_overdue):
                st.write(f"{'Avg Days Overdue' if lang == 'en' else 'Средняя просрочка'}: {avg_overdue:.0f} {get_text('ar_days', lang)}")
        
        st.write(f"{get_text('ar_filtered_avg_age', lang)}: {display_df['days_outstanding'].mean():.0f} {get_text('ar_days', lang)}")

    # Apply pagination (100 records per page)
    page_df, pagination_info = paginate_dataframe(display_df, page_size=100, key_prefix="ar_detail")

    # Translate column headers for main AR table
    lang = st.session_state.get('language', 'en')
    header_map_detail = {
        'invoice_amount': get_text('col_invoice_amount', lang),
        'paid_amount': get_text('col_paid_amount', lang),
        'outstanding_amount': get_text('col_outstanding_amount', lang),
        'date': get_text('col_invoice_date', lang),
        'due_date': 'Due Date' if lang == 'en' else 'Дата оплаты',
        'invoice_number': get_text('col_invoice_number', lang),
        'customer_name': get_text('col_customer_name', lang),
        'days_outstanding': get_text('col_days_outstanding', lang),
        'days_overdue': 'Days Overdue' if lang == 'en' else 'Дней просрочки',
        'payment_status': get_text('col_payment_status', lang),
        'age_bucket': get_text('col_age_bucket', lang),
    }

    st.dataframe(
        page_df.rename(columns=header_map_detail),
        use_container_width=True,
        hide_index=True,
        column_config={
            get_text('col_invoice_amount', lang): st.column_config.NumberColumn(
                get_text('col_invoice_amount', lang),
                format="%.2f"
            ),
            get_text('col_paid_amount', lang): st.column_config.NumberColumn(
                get_text('col_paid_amount', lang),
                format="%.2f"
            ),
            get_text('col_outstanding_amount', lang): st.column_config.NumberColumn(
                get_text('col_outstanding_amount', lang),
                format="%.2f"
            ),
            get_text('col_invoice_date', lang): st.column_config.DateColumn(
                get_text('col_invoice_date', lang),
                format="YYYY-MM-DD"
            ),
            'Due Date' if lang == 'en' else 'Дата оплаты': st.column_config.DateColumn(
                'Due Date' if lang == 'en' else 'Дата оплаты',
                format="YYYY-MM-DD"
            ),
            'Days Overdue' if lang == 'en' else 'Дней просрочки': st.column_config.NumberColumn(
                'Days Overdue' if lang == 'en' else 'Дней просрочки',
                format="%d"
            )
        }
    )

    # Render pagination controls at the bottom
    render_pagination_controls(pagination_info, key_prefix="ar_detail")


def main() -> None:

    # Get language preference
    lang = st.session_state.get('language', 'en')

    st.title(get_text('ar_page_title', lang))
    st.caption(get_text('ar_page_caption', lang))

    # Get data
    ar_data = get_ar_data()
    invoices_df = ar_data['invoices_out']
    bank_df = ar_data['bank_statements']
    reconciliation_df = ar_data.get('reconciliation_out')

    if invoices_df is None or invoices_df.empty:
        st.warning(get_text('ar_no_invoice_out', lang))
        st.page_link("pages/file_upload.py", label=get_text('go_to_file_upload_short', lang), icon="📁")
        return

    # Process AR data
    with st.spinner(get_text('ar_processing', lang)):
        ar_df = process_ar_data(invoices_df, bank_df)

        # Merge with reconciliation data
        ar_df = merge_reconciliation_with_ar(ar_df, reconciliation_df)

    if ar_df.empty:
        st.warning(get_text('ar_no_processed', lang))
        return

    # Date filtering section
    st.subheader(get_text('ar_date_filters', lang))

    # Get date range from data
    if 'date' in ar_df.columns and ar_df['date'].notna().any():
        # Handle both datetime and date objects - ensure we get date objects
        import datetime
        import pandas as pd
        min_val = ar_df['date'].min()
        max_val = ar_df['date'].max()

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
        if st.session_state.get("ar_reset_trigger"):
            st.session_state.ar_date_range = (min_date, max_date)
            st.session_state.ar_reset_trigger = False

        col1, col2, col3 = st.columns([3, 2, 1])

        with col1:
            date_range = st.date_input(
                get_text('date_range', lang) if 'date_range' in globals() else ("Date Range" if lang == 'en' else "Диапазон дат"),
                value=(default_start, max_date),
                min_value=min_date,
                max_value=max_date,
                key="ar_date_range"
            )

            # Handle date range selection
            if isinstance(date_range, tuple) and len(date_range) == 2:
                start_date, end_date = date_range
            else:
                start_date = end_date = date_range if date_range else default_start

        with col2:
            st.write("")  # Spacer
            st.write("")  # Spacer
            if st.button(get_text('reset', lang), key="ar_reset"):
                st.session_state.ar_reset_trigger = True
                st.rerun()

        # Validate date range
        if start_date > end_date:
            st.error(get_text('inv_from_before_to', lang) if 'inv_from_before_to' in globals() else ("From date must be before To date" if lang == 'en' else "Дата 'с' должна быть раньше даты 'по'"))
            return

        # Filter data by date range based on selected filter type
        # if date_filter_type == get_text('ar_filter_invoice_date', lang):
        #     ar_df = ar_df[
        #         (ar_df['date'].dt.date >= start_date) &
        #         (ar_df['date'].dt.date <= end_date)
        #     ]
        #     filter_desc = "invoice date" if lang == 'en' else "дате счета"
        # elif date_filter_type == get_text('ar_filter_payment_date', lang):
        #     # Filter by payment date (only show invoices with payments in range)

        #     ar_df = ar_df[
        #         ar_df['payment_date'].notna() &
        #         (ar_df['payment_date'].dt.date >= start_date) &
        #         (ar_df['payment_date'].dt.date <= end_date)
        #     ]
        #     filter_desc = "payment date" if lang == 'en' else "дате платежа"
        # elif date_filter_type == get_text('ar_filter_either', lang):
        #     # Show if either invoice date or payment date is in range
        #     invoice_in_range = (ar_df['date'].dt.date >= start_date) & (ar_df['date'].dt.date <= end_date)
        #     payment_in_range = ar_df['payment_date'].notna() & (ar_df['payment_date'].dt.date >= start_date) & (ar_df['payment_date'].dt.date <= end_date)
        #     ar_df = ar_df[invoice_in_range | payment_in_range]
        #     filter_desc = "invoice or payment date" if lang == 'en' else "дате счета или платежа"
        # Both Dates
            # Filter invoices by date range AND filter their payments to only those in range
            # This shows all invoices in range, but only considers payments that are also in range
        ar_df = ar_df[
                (ar_df['date'].dt.date >= start_date) &
                (ar_df['date'].dt.date <= end_date)
            ]

            # For invoices with payments outside the date range, set payment info to null
        payment_out_of_range = (
            ar_df['date'].notna() &
            ((ar_df['date'].dt.date < start_date) |
            (ar_df['date'].dt.date > end_date))
        )
        ar_df.loc[payment_out_of_range, 'paid_amount'] = 0
        ar_df.loc[payment_out_of_range, 'date'] = pd.NaT
        ar_df.loc[payment_out_of_range, 'payment_status'] = 'Unpaid'
        # Recalculate outstanding amounts
        ar_df['outstanding_amount'] = ar_df['invoice_amount'] - ar_df['paid_amount']
        ar_df['outstanding_amount'] = ar_df['outstanding_amount'].clip(lower=0)
        # Update payment status
        ar_df.loc[ar_df['outstanding_amount'] <= 1, 'payment_status'] = 'Paid'
        ar_df.loc[
            (ar_df['outstanding_amount'] > 1) & (ar_df['paid_amount'] > 0),
            'payment_status'
        ] = 'Partial'
        filter_desc = get_text('ar_filter_both', lang)
            

        # Show filtered date range info
        total_records = len(ar_df)
        st.info(get_text('ar_range_by', lang).format(start_date, end_date, filter_desc, total_records))

        if ar_df.empty:
            st.warning(get_text('ar_no_range_data', lang))
            return

    st.divider()

    # Reconciliation comparison (if data available)
    render_reconciliation_comparison(ar_df)

    st.divider()

    # Summary metrics
    render_ar_summary(ar_df)

    st.divider()

    # Enhanced Customer Payment Analysis
    # Create customer payment summary - properly aggregate invoices and payments by INN
    if not ar_df.empty and 'customer_inn' in ar_df.columns:
        # Filter to only include signed invoices (СТАТУС = "Подписан")
        signed_invoices = ar_df[ar_df['status'].isin(['Подписан', 'Signed'])].copy()

        if not signed_invoices.empty:
            # Get the most common/recent name for each INN
            customer_names = signed_invoices.groupby('customer_inn')['customer_name'].agg(
                lambda x: x.mode()[0] if not x.mode().empty else x.iloc[0]
            ).reset_index()
            customer_names.columns = ['Customer_INN', 'Customer_Name']

            # Group invoices by INN
            invoice_summary = signed_invoices.groupby('customer_inn').agg({
                'invoice_amount': ['sum', 'count']
            }).round(2)
            invoice_summary.columns = ['Total_Invoiced', 'Invoice_Count']
            invoice_summary = invoice_summary.reset_index()
            invoice_summary.columns = ['Customer_INN', 'Total_Invoiced', 'Invoice_Count']

            # Now get payments from bank statements for these customers
            # Use enhanced matching: direct INN + payment purpose search
            if bank_df is not None and not bank_df.empty:
                from utils.db_operations import find_payments_for_inn

                # Get unique customer INNs from signed invoices
                customer_inns = signed_invoices['customer_inn'].unique()

                # Collect all payments for all customers using enhanced matching
                all_customer_payments = []
                for customer_inn in customer_inns:
                    if pd.notna(customer_inn) and str(customer_inn).strip() not in ['', 'nan']:
                        # Use enhanced matching (includes payment purpose search)
                        customer_payments = find_payments_for_inn(
                            bank_df,
                            customer_inn,
                            transaction_type='incoming'
                        )

                        if not customer_payments.empty:
                            # Add customer INN for grouping
                            customer_payments['customer_inn_matched'] = str(customer_inn)
                            all_customer_payments.append(customer_payments)

                # Combine all customer payments
                if all_customer_payments:
                    combined_payments = pd.concat(all_customer_payments, ignore_index=True)

                    # Create payment summary
                    payment_summary = combined_payments.groupby('customer_inn_matched')['amount'].sum().reset_index()
                    payment_summary.columns = ['Customer_INN', 'Total_Paid']
                    # Ensure Customer_INN is string type
                    payment_summary['Customer_INN'] = payment_summary['Customer_INN'].astype(str)
                else:
                    payment_summary = pd.DataFrame(columns=['Customer_INN', 'Total_Paid'])
            else:
                # No bank data available
                payment_summary = pd.DataFrame(columns=['Customer_INN', 'Total_Paid'])

            # Merge invoices with payments
            # Ensure Customer_INN is string type for consistent merging
            invoice_summary['Customer_INN'] = invoice_summary['Customer_INN'].astype(str)
            customer_names['Customer_INN'] = customer_names['Customer_INN'].astype(str)

            customer_payment_df = invoice_summary.merge(customer_names, on='Customer_INN', how='left')
            customer_payment_df = customer_payment_df.merge(payment_summary, on='Customer_INN', how='left')

            # Fill missing payments with 0
            customer_payment_df['Total_Paid'] = customer_payment_df['Total_Paid'].fillna(0)

            # Calculate outstanding
            customer_payment_df['Outstanding'] = (customer_payment_df['Total_Invoiced'] - customer_payment_df['Total_Paid']).clip(lower=0)

            # Reorder columns
            customer_payment_df = customer_payment_df[['Customer_INN', 'Customer_Name', 'Total_Invoiced', 'Invoice_Count', 'Total_Paid', 'Outstanding']]

            # Calculate Returns and Financial Discounts per customer
            returns_by_customer = ar_df[ar_df['is_return'] == True].groupby('customer_inn')['invoice_amount'].sum()
            discounts_by_customer = ar_df[ar_df['is_financial_discount'] == True].groupby('customer_inn')['invoice_amount'].sum()

            customer_payment_df['Returns'] = customer_payment_df['Customer_INN'].map(returns_by_customer).fillna(0)
            customer_payment_df['Financial_Discounts'] = customer_payment_df['Customer_INN'].map(discounts_by_customer).fillna(0)

            # Calculate payment rate
            customer_payment_df['Payment_Rate'] = (
                customer_payment_df['Total_Paid'] / customer_payment_df['Total_Invoiced'] * 100
            ).fillna(0).round(1)

            # Only show customers with data
            customer_payment_df = customer_payment_df[customer_payment_df['Total_Invoiced'] > 0]

            if not customer_payment_df.empty:
                render_customer_payment_analysis(customer_payment_df, lang)
                st.divider()

    # Aging analysis
    render_aging_analysis(ar_df)

    st.divider()

    # Customer analysis
    render_customer_analysis(ar_df)

    st.divider()

    # Collection trends
    render_collection_trends(ar_df)

    st.divider()

    # Detailed table
    render_ar_detail_table(ar_df)


if __name__ == "__main__":
    main()


