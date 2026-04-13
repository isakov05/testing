import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import os

from translations import get_text
from auth.db_authenticator import protect_page


st.set_page_config(page_title="Invoice Analytics", page_icon="📊", layout="wide")

protect_page()



def get_invoice_data():
    """Get invoice data from session state"""
    return {
        'invoices_in': st.session_state.get('invoices_in_processed'),
        'invoices_out': st.session_state.get('invoices_out_processed')
    }


def find_amount_column(df):
    """Find the appropriate amount column from invoice data"""
    amount_columns = [
        'Стоимость поставки с учётом НДС',
        'Сумма к оплате',
        'amount', 'Amount', 'Сумма', 'Стоимость', 'Cost', 'Total'
    ]

    for col in amount_columns:
        if col in df.columns:
            return col
    return None


def process_invoice_data(df, invoice_type):
    """Process invoice data and standardize columns"""
    if df is None or df.empty:
        return pd.DataFrame()

    processed_df = df.copy()

    # Standardize amount column
    amount_col = find_amount_column(processed_df)
    if amount_col:
        processed_df['amount'] = pd.to_numeric(processed_df[amount_col], errors='coerce').fillna(0)
    else:
        processed_df['amount'] = 0

    # Standardize date column
    if 'date' not in processed_df.columns:
        date_candidates = ['Дата документ', 'Дата документа', 'Document Date', 'Date']
        for col in date_candidates:
            if col in processed_df.columns:
                processed_df['date'] = pd.to_datetime(processed_df[col], errors='coerce')
                break

    # Standardize company information
    if invoice_type == 'in':
        # For invoices in, supplier is the seller
        processed_df['company_inn'] = processed_df.get('Продавец (ИНН или ПИНФЛ)', '')
        processed_df['company_name'] = processed_df.get('Продавец (наименование)', '')
    else:
        # For invoices out, customer is the buyer
        processed_df['company_inn'] = processed_df.get('Покупатель (ИНН или ПИНФЛ)', '')
        processed_df['company_name'] = processed_df.get('Покупатель (наименование)', '')

    # Standardize status
    processed_df['status'] = processed_df.get('СТАТУС', processed_df.get('Статус', 'Unknown'))

    # Add invoice type
    processed_df['invoice_type'] = invoice_type

    return processed_df


def render_invoice_status_analysis(df, title):
    """Render invoice status analysis"""
    st.subheader(f"📊 {title} - Status Analysis")

    if df.empty:
        st.info(f"No {title.lower()} data available")
        return

    # Status summary
    status_summary = df.groupby('status').agg({
        'amount': ['count', 'sum', 'mean']
    }).round(2)

    status_summary.columns = ['Count', 'Total Amount', 'Average Amount']
    status_summary = status_summary.sort_values('Total Amount', ascending=False)

    col1, col2 = st.columns(2)

    with col1:
        # Status distribution pie chart
        fig_status = px.pie(
            status_summary.reset_index(),
            values='Count',
            names='status',
            title=f"{title} by Status (Count)"
        )
        st.plotly_chart(fig_status, use_container_width=True)

    with col2:
        # Status amount bar chart
        fig_amount = px.bar(
            status_summary.reset_index(),
            x='Total Amount',
            y='status',
            title=f"{title} by Status (Amount)",
            orientation='h',
            color='Total Amount',
            color_continuous_scale='viridis'
        )
        st.plotly_chart(fig_amount, use_container_width=True)

    # Status summary table
    with st.expander(f"📋 Detailed {title} Status Breakdown", expanded=False):
        st.dataframe(status_summary, use_container_width=True)


def render_company_summary(df, title, company_type):
    """Render supplier/customer summary analysis"""
    st.subheader(f"🏢 {title} - {company_type} Summary")

    if df.empty:
        st.info(f"No {title.lower()} data available")
        return

    # Company summary
    company_summary = df.groupby(['company_inn', 'company_name']).agg({
        'amount': ['count', 'sum', 'mean']
    }).round(2)

    company_summary.columns = ['Invoice Count', 'Total Amount', 'Average Amount']
    company_summary = company_summary.sort_values('Total Amount', ascending=False).head(20)

    # Format company display
    company_summary_display = company_summary.reset_index()
    company_summary_display['Company'] = company_summary_display.apply(
        lambda row: f"{row['company_name']} (INN: {row['company_inn']})" if pd.notna(row['company_inn']) and row['company_inn'] != '' else row['company_name'],
        axis=1
    )

    col1, col2 = st.columns(2)

    with col1:
        # Top companies by amount
        fig_companies = px.bar(
            company_summary_display.head(10),
            x='Total Amount',
            y='Company',
            title=f"Top 10 {company_type}s by Amount",
            orientation='h'
        )
        fig_companies.update_layout(height=400)
        st.plotly_chart(fig_companies, use_container_width=True)

    with col2:
        # Top companies by invoice count
        fig_count = px.bar(
            company_summary_display.head(10),
            x='Invoice Count',
            y='Company',
            title=f"Top 10 {company_type}s by Invoice Count",
            orientation='h',
            color='Invoice Count',
            color_continuous_scale='blues'
        )
        fig_count.update_layout(height=400)
        st.plotly_chart(fig_count, use_container_width=True)

    # Company summary table
    with st.expander(f"📋 Detailed {company_type} Summary", expanded=False):
        st.dataframe(company_summary_display[['Company', 'Invoice Count', 'Total Amount', 'Average Amount']], use_container_width=True)


def render_invoice_details(df, title):
    """Render detailed invoice analysis"""
    st.subheader(f"📄 {title} - Invoice Details")

    if df.empty:
        st.info(f"No {title.lower()} data available")
        return

    # Time series analysis
    if 'date' in df.columns:
        df['month'] = df['date'].dt.to_period('M').astype(str)
        monthly_summary = df.groupby('month').agg({
            'amount': ['count', 'sum']
        }).round(2)

        monthly_summary.columns = ['Invoice Count', 'Total Amount']

        # Monthly trends
        fig_trends = make_subplots(
            specs=[[{"secondary_y": True}]],
            subplot_titles=[f"{title} Monthly Trends"]
        )

        fig_trends.add_trace(
            go.Scatter(
                x=monthly_summary.index,
                y=monthly_summary['Total Amount'],
                name="Total Amount",
                line=dict(color='#00D4AA', width=3)
            ),
            secondary_y=False,
        )

        fig_trends.add_trace(
            go.Scatter(
                x=monthly_summary.index,
                y=monthly_summary['Invoice Count'],
                name="Invoice Count",
                line=dict(color='#FF6B6B', width=3)
            ),
            secondary_y=True,
        )

        fig_trends.update_xaxes(title_text="Month")
        fig_trends.update_yaxes(title_text="Amount", secondary_y=False)
        fig_trends.update_yaxes(title_text="Invoice Count", secondary_y=True)

        st.plotly_chart(fig_trends, use_container_width=True)

    # Recent invoices table
    st.write(f"**Recent {title}**")
    recent_columns = ['date', 'status', 'company_name', 'amount']
    available_columns = [col for col in recent_columns if col in df.columns]

    if available_columns:
        recent_invoices = df.nlargest(10, 'date')[available_columns] if 'date' in df.columns else df.head(10)[available_columns]
        st.dataframe(recent_invoices, use_container_width=True)


def render_monthly_revenue_expenses(invoices_in_df, invoices_out_df):
    """Render monthly revenue vs expenses analysis"""
    st.subheader("📈 Monthly Revenue vs Expenses Analytics")

    if (invoices_in_df.empty or invoices_in_df is None) and (invoices_out_df.empty or invoices_out_df is None):
        st.info("No invoice data available for revenue/expense analysis")
        return

    # Prepare data
    monthly_data = []

    if not invoices_in_df.empty and 'date' in invoices_in_df.columns:
        revenue_monthly = invoices_in_df.groupby(invoices_in_df['date'].dt.to_period('M'))['amount'].sum().reset_index()
        revenue_monthly['date'] = revenue_monthly['date'].astype(str)
        revenue_monthly['type'] = 'Revenue'
        monthly_data.append(revenue_monthly)

    if not invoices_out_df.empty and 'date' in invoices_out_df.columns:
        expense_monthly = invoices_out_df.groupby(invoices_out_df['date'].dt.to_period('M'))['amount'].sum().reset_index()
        expense_monthly['date'] = expense_monthly['date'].astype(str)
        expense_monthly['type'] = 'Expenses'
        monthly_data.append(expense_monthly)

    if monthly_data:
        combined_monthly = pd.concat(monthly_data, ignore_index=True)

        # Company filter
        companies = []
        if not invoices_in_df.empty:
            companies.extend(invoices_in_df['company_name'].dropna().unique().tolist())
        if not invoices_out_df.empty:
            companies.extend(invoices_out_df['company_name'].dropna().unique().tolist())

        companies = list(set(companies))[:20]  # Limit to top 20 companies

        if companies:
            selected_companies = st.multiselect(
                "Filter by Company",
                options=['All'] + companies,
                default=['All'],
                help="Filter the revenue/expense analysis by specific companies"
            )

            if 'All' not in selected_companies and selected_companies:
                # Filter data by selected companies
                filtered_in = invoices_in_df[invoices_in_df['company_name'].isin(selected_companies)] if not invoices_in_df.empty else pd.DataFrame()
                filtered_out = invoices_out_df[invoices_out_df['company_name'].isin(selected_companies)] if not invoices_out_df.empty else pd.DataFrame()

                # Recalculate monthly data
                monthly_data = []
                if not filtered_in.empty:
                    revenue_monthly = filtered_in.groupby(filtered_in['date'].dt.to_period('M'))['amount'].sum().reset_index()
                    revenue_monthly['date'] = revenue_monthly['date'].astype(str)
                    revenue_monthly['type'] = 'Revenue'
                    monthly_data.append(revenue_monthly)

                if not filtered_out.empty:
                    expense_monthly = filtered_out.groupby(filtered_out['date'].dt.to_period('M'))['amount'].sum().reset_index()
                    expense_monthly['date'] = expense_monthly['date'].astype(str)
                    expense_monthly['type'] = 'Expenses'
                    monthly_data.append(expense_monthly)

                if monthly_data:
                    combined_monthly = pd.concat(monthly_data, ignore_index=True)

        # Charts
        col1, col2 = st.columns(2)

        with col1:
            # Revenue vs Expenses trend
            fig_trend = px.line(
                combined_monthly,
                x='date',
                y='amount',
                color='type',
                title="Monthly Revenue vs Expenses Trend",
                color_discrete_map={'Revenue': '#00D4AA', 'Expenses': '#FF6B6B'}
            )
            st.plotly_chart(fig_trend, use_container_width=True)

        with col2:
            # Monthly comparison bar chart
            pivot_data = combined_monthly.pivot_table(
                index='date',
                columns='type',
                values='amount',
                fill_value=0
            ).reset_index()

            if 'Revenue' in pivot_data.columns and 'Expenses' in pivot_data.columns:
                pivot_data['Net Profit'] = pivot_data['Revenue'] - pivot_data['Expenses']

                fig_profit = px.bar(
                    pivot_data,
                    x='date',
                    y='Net Profit',
                    title="Monthly Net Profit",
                    color='Net Profit',
                    color_continuous_scale='RdYlGn'
                )
                st.plotly_chart(fig_profit, use_container_width=True)


def render_invoice_status_dashboard(invoices_in_df, invoices_out_df):
    """Render comprehensive invoice status dashboard"""
    st.subheader("📊 Invoice Status Dashboard")

    # Combined status overview
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_in = len(invoices_in_df) if not invoices_in_df.empty else 0
        st.metric("📥 Total Invoices In", f"{total_in:,}")

    with col2:
        total_out = len(invoices_out_df) if not invoices_out_df.empty else 0
        st.metric("📤 Total Invoices Out", f"{total_out:,}")

    with col3:
        total_in_amount = invoices_in_df['amount'].sum() if not invoices_in_df.empty else 0
        st.metric("💰 Total Revenue", f"{total_in_amount:,.2f}")

    with col4:
        total_out_amount = invoices_out_df['amount'].sum() if not invoices_out_df.empty else 0
        st.metric("💸 Total Expenses", f"{total_out_amount:,.2f}")

    # Status comparison
    status_col1, status_col2 = st.columns(2)

    with status_col1:
        if not invoices_in_df.empty:
            in_status = invoices_in_df['status'].value_counts().reset_index()
            in_status.columns = ['Status', 'Count']
            fig_in_status = px.pie(
                in_status,
                values='Count',
                names='Status',
                title="Invoices In - Status Distribution"
            )
            st.plotly_chart(fig_in_status, use_container_width=True)

    with status_col2:
        if not invoices_out_df.empty:
            out_status = invoices_out_df['status'].value_counts().reset_index()
            out_status.columns = ['Status', 'Count']
            fig_out_status = px.pie(
                out_status,
                values='Count',
                names='Status',
                title="Invoices Out - Status Distribution"
            )
            st.plotly_chart(fig_out_status, use_container_width=True)


def render_invoice_trends(invoices_in_df, invoices_out_df):
    """Render invoice trends and insights"""
    st.subheader("📈 Invoice Trends & Insights")

    # Weekly trends
    if not invoices_in_df.empty and not invoices_out_df.empty:
        # Combine data for weekly analysis
        invoices_in_df['week'] = invoices_in_df['date'].dt.to_period('W').astype(str)
        invoices_out_df['week'] = invoices_out_df['date'].dt.to_period('W').astype(str)

        weekly_in = invoices_in_df.groupby('week')['amount'].agg(['count', 'sum']).reset_index()
        weekly_in.columns = ['week', 'count_in', 'amount_in']

        weekly_out = invoices_out_df.groupby('week')['amount'].agg(['count', 'sum']).reset_index()
        weekly_out.columns = ['week', 'count_out', 'amount_out']

        weekly_combined = pd.merge(weekly_in, weekly_out, on='week', how='outer').fillna(0)

        # Weekly trends chart
        fig_weekly = go.Figure()

        fig_weekly.add_trace(go.Scatter(
            x=weekly_combined['week'],
            y=weekly_combined['count_in'],
            mode='lines+markers',
            name='Invoices In (Count)',
            line=dict(color='#00D4AA')
        ))

        fig_weekly.add_trace(go.Scatter(
            x=weekly_combined['week'],
            y=weekly_combined['count_out'],
            mode='lines+markers',
            name='Invoices Out (Count)',
            line=dict(color='#FF6B6B')
        ))

        fig_weekly.update_layout(
            title="Weekly Invoice Volume Trends",
            xaxis_title="Week",
            yaxis_title="Invoice Count",
            height=400
        )

        st.plotly_chart(fig_weekly, use_container_width=True)

    # Key insights
    insights_col1, insights_col2 = st.columns(2)

    with insights_col1:
        st.write("**📊 Key Insights:**")
        insights = []

        if not invoices_in_df.empty:
            avg_in_amount = invoices_in_df['amount'].mean()
            insights.append(f"• Average invoice in amount: {avg_in_amount:,.2f}")

            top_supplier = invoices_in_df.groupby('company_name')['amount'].sum().idxmax() if 'company_name' in invoices_in_df.columns else None
            if top_supplier:
                insights.append(f"• Top supplier: {top_supplier}")

        if not invoices_out_df.empty:
            avg_out_amount = invoices_out_df['amount'].mean()
            insights.append(f"• Average invoice out amount: {avg_out_amount:,.2f}")

            top_customer = invoices_out_df.groupby('company_name')['amount'].sum().idxmax() if 'company_name' in invoices_out_df.columns else None
            if top_customer:
                insights.append(f"• Top customer: {top_customer}")

        for insight in insights:
            st.write(insight)

    with insights_col2:
        st.write("**⚠️ Alerts & Recommendations:**")
        alerts = []

        if not invoices_in_df.empty:
            pending_in = invoices_in_df[invoices_in_df['status'].str.contains('PENDING|pending', case=False, na=False)]
            if not pending_in.empty:
                alerts.append(f"• {len(pending_in)} pending invoices in")

        if not invoices_out_df.empty:
            pending_out = invoices_out_df[invoices_out_df['status'].str.contains('PENDING|pending', case=False, na=False)]
            if not pending_out.empty:
                alerts.append(f"• {len(pending_out)} pending invoices out")

        if not alerts:
            alerts.append("• All invoices appear to be processed")

        for alert in alerts:
            st.write(alert)


def main() -> None:
    lang = st.session_state.get('language', 'en')
    st.title(get_text('inv_page_title', lang))
    st.caption(get_text('inv_subtitle', lang))

    # Get invoice data
    invoice_data = get_invoice_data()
    invoices_in_raw = invoice_data['invoices_in']
    invoices_out_raw = invoice_data['invoices_out']

    # Check if data is available
    if (invoices_in_raw is None or invoices_in_raw.empty) and (invoices_out_raw is None or invoices_out_raw.empty):
        st.warning(get_text('no_processed_data', lang))
        st.page_link("pages/file_upload.py", label=get_text('go_to_file_upload_short', lang), icon="📁")
        return

    # Process invoice data
    invoices_in_df = process_invoice_data(invoices_in_raw, 'in')
    invoices_out_df = process_invoice_data(invoices_out_raw, 'out')

    # Tabs for different analysis sections
    tab1, tab2, tab3, tab4 = st.tabs(["📥 Invoice In", "📤 Invoice Out", "📈 Analytics", "📊 Status Dashboard"])

    with tab1:
        if not invoices_in_df.empty:
            render_invoice_status_analysis(invoices_in_df, "Invoice In")
            st.divider()
            render_company_summary(invoices_in_df, "Invoice In", "Supplier")
            st.divider()
            render_invoice_details(invoices_in_df, "Invoice In")
        else:
            st.info("No invoice in data available")

    with tab2:
        if not invoices_out_df.empty:
            render_invoice_status_analysis(invoices_out_df, "Invoice Out")
            st.divider()
            render_company_summary(invoices_out_df, "Invoice Out", "Customer")
            st.divider()
            render_invoice_details(invoices_out_df, "Invoice Out")
        else:
            st.info("No invoice out data available")

    with tab3:
        render_monthly_revenue_expenses(invoices_in_df, invoices_out_df)
        st.divider()
        render_invoice_trends(invoices_in_df, invoices_out_df)

    with tab4:
        render_invoice_status_dashboard(invoices_in_df, invoices_out_df)


if __name__ == "__main__":
    main()


