import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
from translations import get_text
from auth.db_authenticator import protect_page

st.set_page_config(page_title="Sales Analytics", page_icon="📉", layout="wide")

protect_page()



def get_sales_data():
    """Get data needed for sales analysis"""
    return {
        'invoices_out': st.session_state.get('invoices_out_processed')
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


def find_date_column(df):
    """Find the appropriate date column"""
    date_columns = ['Дата документ', 'Document Date', 'date', 'Date']

    for col in date_columns:
        if col in df.columns:
            return col
    return None


def find_customer_column(df):
    """Find the appropriate customer column"""
    customer_columns = [
        'Покупатель (наименование)', 'Buyer (Name)',
        'Покупатель (ИНН или ПИНФЛ)', 'Buyer (Tax ID or PINFL)',
        'customer', 'Customer'
    ]

    for col in customer_columns:
        if col in df.columns:
            return col
    return None


def process_sales_data(invoices_df):
    """Process sales data from outgoing invoices"""
    if invoices_df is None or invoices_df.empty:
        return pd.DataFrame()

    sales_df = invoices_df.copy()

    # Standardize date column
    date_col = find_date_column(sales_df)
    if date_col:
        sales_df['date'] = pd.to_datetime(sales_df[date_col], errors='coerce')
    else:
        sales_df['date'] = pd.NaT

    # Standardize amount column
    amount_col = find_amount_column(sales_df)
    if amount_col:
        sales_df['sales_amount'] = pd.to_numeric(sales_df[amount_col], errors='coerce').fillna(0)
    else:
        sales_df['sales_amount'] = 0

    # Standardize customer information
    customer_col = find_customer_column(sales_df)
    if customer_col:
        sales_df['customer'] = sales_df[customer_col].astype(str).str.strip()
    else:
        sales_df['customer'] = 'Unknown'

    # Get invoice number
    sales_df['invoice_number'] = sales_df.get('Номер документ', sales_df.get('Document Number', ''))

    # Filter for signed/valid invoices only
    if 'СТАТУС' in sales_df.columns:
        sales_df = sales_df[sales_df['СТАТУС'] == 'Подписан']
    elif 'Status' in sales_df.columns:
        sales_df = sales_df[sales_df['Status'] == 'Подписан']

    return sales_df


def calculate_sales_metrics(df):
    """Calculate key sales metrics"""
    if df.empty:
        return {
            'total_sales': 0,
            'total_invoices': 0,
            'unique_customers': 0,
            'avg_invoice_value': 0,
            'sales_growth': 0,
            'top_customers': pd.Series(dtype=float)
        }

    metrics = {
        'total_sales': df['sales_amount'].sum(),
        'total_invoices': len(df),
        'unique_customers': df['customer'].nunique(),
        'avg_invoice_value': df['sales_amount'].mean(),
    }

    # Calculate growth (compare current month vs previous month)
    if 'date' in df.columns and df['date'].notna().any():
        current_month = df['date'].max().replace(day=1) if df['date'].max() is not pd.NaT else datetime.now().replace(day=1)
        prev_month = current_month - timedelta(days=32)
        prev_month = prev_month.replace(day=1)

        current_sales = df[df['date'] >= current_month]['sales_amount'].sum()
        prev_sales = df[(df['date'] >= prev_month) & (df['date'] < current_month)]['sales_amount'].sum()

        metrics['sales_growth'] = ((current_sales - prev_sales) / prev_sales * 100) if prev_sales > 0 else 0
    else:
        metrics['sales_growth'] = 0

    # Top customers
    metrics['top_customers'] = df.groupby('customer')['sales_amount'].sum().sort_values(ascending=False).head(10)

    return metrics


def create_sales_trend_chart(df):
    """Create sales trend chart"""
    if df.empty or 'date' not in df.columns or df['date'].notna().sum() == 0:
        return None

    # Group by month
    df_trend = df[df['date'].notna()].copy()
    df_trend['month'] = df_trend['date'].dt.to_period('M')

    monthly_sales = df_trend.groupby('month').agg({
        'sales_amount': 'sum',
        'customer': 'nunique'
    }).reset_index()

    monthly_sales['month'] = monthly_sales['month'].astype(str)

    fig = go.Figure()

    # Sales amount line
    fig.add_trace(go.Scatter(
        x=monthly_sales['month'],
        y=monthly_sales['sales_amount'],
        mode='lines+markers',
        name=get_text('label_sales_amount', st.session_state.get('language', 'en')),
        line=dict(color='#1f77b4', width=3),
        yaxis='y'
    ))

    # Customer count line
    fig.add_trace(go.Scatter(
        x=monthly_sales['month'],
        y=monthly_sales['customer'],
        mode='lines+markers',
        name=get_text('label_unique_customers', st.session_state.get('language', 'en')),
        line=dict(color='#ff7f0e', width=2),
        yaxis='y2'
    ))

    fig.update_layout(
        title=get_text('sales_trend_title', st.session_state.get('language', 'en')),
        xaxis_title=get_text('label_month', st.session_state.get('language', 'en')),
        yaxis=dict(title=get_text('label_sales_amount', st.session_state.get('language', 'en')), side='left'),
        yaxis2=dict(title=get_text('label_num_customers', st.session_state.get('language', 'en')), side='right', overlaying='y'),
        height=400,
        hovermode='x unified'
    )

    return fig


def create_customer_analysis(df):
    """Create customer analysis charts"""
    if df.empty:
        return None, None

    # Top customers by sales
    top_customers = df.groupby('customer')['sales_amount'].sum().sort_values(ascending=False).head(10)

    fig_customers = px.bar(
        x=top_customers.values,
        y=top_customers.index,
        orientation='h',
        title=get_text('sales_top_customers_title', st.session_state.get('language', 'en')),
        labels={'x': get_text('label_sales_amount', st.session_state.get('language', 'en')), 'y': get_text('label_customer', st.session_state.get('language', 'en'))},
        color=top_customers.values,
        color_continuous_scale='Viridis'
    )
    fig_customers.update_layout(height=500, showlegend=False)

    # Customer concentration (Pareto analysis)
    customer_sales = df.groupby('customer')['sales_amount'].sum().sort_values(ascending=False)
    customer_sales_pct = (customer_sales / customer_sales.sum() * 100).cumsum()

    pareto_df = pd.DataFrame({
        'customer': customer_sales.index[:20],  # Top 20 customers
        'sales': customer_sales.values[:20],
        'cumulative_pct': customer_sales_pct.values[:20]
    })

    fig_pareto = go.Figure()

    fig_pareto.add_trace(go.Bar(
        x=list(range(len(pareto_df))),
        y=pareto_df['sales'],
        name='Sales Amount',
        yaxis='y',
        marker_color='lightblue'
    ))

    fig_pareto.add_trace(go.Scatter(
        x=list(range(len(pareto_df))),
        y=pareto_df['cumulative_pct'],
        mode='lines+markers',
        name='Cumulative %',
        yaxis='y2',
        line=dict(color='red', width=2)
    ))

    fig_pareto.update_layout(
        title=get_text('customer_pareto_title', st.session_state.get('language', 'en')),
        xaxis=dict(title=get_text('label_customer_rank', st.session_state.get('language', 'en')), tickvals=list(range(len(pareto_df))),
                  ticktext=[f"C{i+1}" for i in range(len(pareto_df))]),
        yaxis=dict(title=get_text('label_sales_amount', st.session_state.get('language', 'en')), side='left'),
        yaxis2=dict(title='Cumulative Percentage', side='right', overlaying='y', range=[0, 100]),
        height=400
    )

    return fig_customers, fig_pareto


def create_invoice_analysis(df):
    """Create invoice size and frequency analysis"""
    if df.empty:
        return None, None

    # Invoice size distribution
    fig_size = px.histogram(
        df,
        x='sales_amount',
        nbins=30,
        title=get_text('invoice_size_dist_title', st.session_state.get('language', 'en')),
        labels={'sales_amount': get_text('label_invoice_amount', st.session_state.get('language', 'en')), 'count': get_text('label_num_invoices', st.session_state.get('language', 'en'))},
        color_discrete_sequence=['skyblue']
    )
    fig_size.update_layout(height=400)

    # Invoice frequency by day of week
    if 'date' in df.columns and df['date'].notna().any():
        df_freq = df[df['date'].notna()].copy()
        df_freq['day_of_week'] = df_freq['date'].dt.day_name()

        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        freq_summary = df_freq.groupby('day_of_week').size().reindex(day_order, fill_value=0)

        fig_freq = px.bar(
            x=freq_summary.index,
            y=freq_summary.values,
            title=get_text('invoice_freq_title', st.session_state.get('language', 'en')),
            labels={'x': get_text('label_day_of_week', st.session_state.get('language', 'en')), 'y': get_text('label_num_invoices', st.session_state.get('language', 'en'))},
            color=freq_summary.values,
            color_continuous_scale='Blues'
        )
        fig_freq.update_layout(height=400, showlegend=False)
    else:
        fig_freq = None

    return fig_size, fig_freq


def render_sales_summary(sales_df, metrics):
    """Render sales summary metrics"""
    st.subheader(get_text('key_sales_metrics', st.session_state.get('language', 'en')))

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            get_text('metric_total_sales', st.session_state.get('language', 'en')),
            f"{metrics['total_sales']:,.2f}",
        )

    with col2:
        st.metric(
            get_text('metric_total_invoices', st.session_state.get('language', 'en')),
            f"{metrics['total_invoices']:,}",
            delta=get_text('delta_signed_only', st.session_state.get('language', 'en'))
        )

    with col3:
        st.metric(
            get_text('metric_unique_customers', st.session_state.get('language', 'en')),
            f"{metrics['unique_customers']:,}",
            delta=get_text('delta_active_customers', st.session_state.get('language', 'en'))
        )

    with col4:
        st.metric(
            get_text('metric_avg_invoice_value', st.session_state.get('language', 'en')),
            f"{metrics['avg_invoice_value']:,.2f}"
        )


def render_sales_trends(sales_df):
    """Render sales trends section"""
    st.subheader(get_text('sales_perf_over_time', st.session_state.get('language', 'en')))

    # Sales trend chart
    trend_chart = create_sales_trend_chart(sales_df)
    if trend_chart:
        st.plotly_chart(trend_chart, use_container_width=True)
    else:
        st.info(get_text('no_date_info_trend', st.session_state.get('language', 'en')))

    col1, col2 = st.columns(2)

    with col1:
        # Monthly sales table
        if 'date' in sales_df.columns and sales_df['date'].notna().any():
            st.subheader(get_text('monthly_sales_summary', st.session_state.get('language', 'en')))

            monthly_summary = sales_df[sales_df['date'].notna()].copy()
            monthly_summary['month'] = monthly_summary['date'].dt.to_period('M')

            summary_table = monthly_summary.groupby('month').agg({
                'sales_amount': ['sum', 'count', 'mean'],
                'customer': 'nunique'
            }).round(0)

            summary_table.columns = ['Total Sales', 'Invoice Count', 'Avg Invoice', 'Unique Customers']

            # Sort from newest to oldest (2025 -> 2023)
            summary_table = summary_table.sort_index(ascending=False)
            summary_table.index = summary_table.index.astype(str)

            # Format for display
            summary_display = summary_table.copy()
            for col in ['Total Sales', 'Avg Invoice']:
                summary_display[col] = summary_display[col].apply(lambda x: f"{x:,.2f}")

            st.dataframe(summary_display, use_container_width=True)

    with col2:
        # Growth analysis
        st.subheader(get_text('growth_analysis', st.session_state.get('language', 'en')))

        if 'date' in sales_df.columns and sales_df['date'].notna().any():
            # Calculate month-over-month growth
            monthly_sales = sales_df[sales_df['date'].notna()].copy()
            monthly_sales['month'] = monthly_sales['date'].dt.to_period('M')

            growth_data = monthly_sales.groupby('month')['sales_amount'].sum()
            growth_rates = growth_data.pct_change() * 100
            growth_rates = growth_rates.dropna()

            if len(growth_rates) > 0:
                fig_growth = px.bar(
                    x=growth_rates.index.astype(str),
                    y=growth_rates.values,
                    title=get_text('sales_growth_rate_title', st.session_state.get('language', 'en')),
                    color=growth_rates.values,
                    color_continuous_scale='RdYlGn',
                    color_continuous_midpoint=0
                )
                fig_growth.update_layout(height=350, showlegend=False)
                st.plotly_chart(fig_growth, use_container_width=True)

                avg_growth = growth_rates.mean()
                st.metric(get_text('avg_monthly_growth', st.session_state.get('language', 'en')), f"{avg_growth:+.1f}%")
            else:
                st.info(get_text('insufficient_growth_data', st.session_state.get('language', 'en')))
        else:
            st.info(get_text('no_date_info', st.session_state.get('language', 'en')))


def render_customer_analysis(sales_df):
    """Render customer analysis section"""
    st.subheader(get_text('customer_perf_analysis', st.session_state.get('language', 'en')))

    customer_chart, pareto_chart = create_customer_analysis(sales_df)

    col1, col2 = st.columns(2)

    with col1:
        if customer_chart:
            st.plotly_chart(customer_chart, use_container_width=True)

    with col2:
        if pareto_chart:
            st.plotly_chart(pareto_chart, use_container_width=True)

    # Customer segmentation
    st.subheader(get_text('customer_segmentation', st.session_state.get('language', 'en')))

    customer_stats = sales_df.groupby('customer').agg({
        'sales_amount': ['sum', 'count', 'mean'],
        'date': ['min', 'max'] if 'date' in sales_df.columns and sales_df['date'].notna().any() else ['count', 'count']
    }).round(2)

    customer_stats.columns = ['Total Sales', 'Invoice Count', 'Avg Invoice', 'First Sale', 'Last Sale']

    # Segment customers
    total_sales_median = customer_stats['Total Sales'].median()
    freq_median = customer_stats['Invoice Count'].median()

    def segment_customer(row):
        if row['Total Sales'] > total_sales_median and row['Invoice Count'] > freq_median:
            return get_text('segment_vip', st.session_state.get('language', 'en'))
        elif row['Total Sales'] > total_sales_median:
            return get_text('segment_high_value', st.session_state.get('language', 'en'))
        elif row['Invoice Count'] > freq_median:
            return get_text('segment_frequent', st.session_state.get('language', 'en'))
        else:
            return get_text('segment_standard', st.session_state.get('language', 'en'))

    customer_stats['Segment'] = customer_stats.apply(segment_customer, axis=1)
    customer_stats = customer_stats.sort_values('Total Sales', ascending=False)

    # Format for display
    display_stats = customer_stats.head(20).copy()
    for col in ['Total Sales', 'Avg Invoice']:
        if col in display_stats.columns:
            display_stats[col] = display_stats[col].apply(lambda x: f"{x:,.2f}")

    st.dataframe(display_stats, use_container_width=True)

    # Segment summary
    segment_summary = customer_stats['Segment'].value_counts()
    fig_segments = px.pie(
        values=segment_summary.values,
        names=segment_summary.index,
        title=get_text('customer_segmentation_distribution', st.session_state.get('language', 'en'))
    )
    st.plotly_chart(fig_segments, use_container_width=True)


def render_invoice_analysis(sales_df):
    """Render invoice analysis section"""
    st.subheader(get_text('invoice_pattern_analysis', st.session_state.get('language', 'en')))

    size_chart, freq_chart = create_invoice_analysis(sales_df)

    col1, col2 = st.columns(2)

    with col1:
        if size_chart:
            st.plotly_chart(size_chart, use_container_width=True)

    with col2:
        if freq_chart:
            st.plotly_chart(freq_chart, use_container_width=True)

    # Invoice statistics
    st.subheader(get_text('invoice_statistics', st.session_state.get('language', 'en')))

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(get_text('median_invoice', st.session_state.get('language', 'en')), f"{sales_df['sales_amount'].median():,.2f}")
        st.metric(get_text('min_invoice', st.session_state.get('language', 'en')), f"{sales_df['sales_amount'].min():,.2f}")
        st.metric(get_text('max_invoice', st.session_state.get('language', 'en')), f"{sales_df['sales_amount'].max():,.2f}")

    with col2:
        percentiles = sales_df['sales_amount'].quantile([0.25, 0.75, 0.95])
        st.metric(get_text('p25', st.session_state.get('language', 'en')), f"{percentiles[0.25]:,.2f}")
        st.metric(get_text('p75', st.session_state.get('language', 'en')), f"{percentiles[0.75]:,.2f}")
        st.metric(get_text('p95', st.session_state.get('language', 'en')), f"{percentiles[0.95]:,.2f}")

    with col3:
        std_dev = sales_df['sales_amount'].std()
        cv = std_dev / sales_df['sales_amount'].mean() * 100 if sales_df['sales_amount'].mean() > 0 else 0
        st.metric(get_text('std_dev', st.session_state.get('language', 'en')), f"{std_dev:,.2f}")
        st.metric(get_text('coeff_variation', st.session_state.get('language', 'en')), f"{cv:.1f}%")

        # Invoice size categories
        small_threshold = sales_df['sales_amount'].quantile(0.33)
        large_threshold = sales_df['sales_amount'].quantile(0.67)

        small_count = (sales_df['sales_amount'] <= small_threshold).sum()
        medium_count = ((sales_df['sales_amount'] > small_threshold) &
                      (sales_df['sales_amount'] <= large_threshold)).sum()
        large_count = (sales_df['sales_amount'] > large_threshold).sum()

        st.write("**" + get_text('invoice_size_distribution', st.session_state.get('language', 'en')) + "**")
        st.write(get_text('invoice_small_tpl', st.session_state.get('language', 'en')).format(val=f"{small_threshold:,.2f}") + f": {small_count}")
        st.write(get_text('invoice_medium', st.session_state.get('language', 'en')) + f": {medium_count}")
        st.write(get_text('invoice_large_tpl', st.session_state.get('language', 'en')).format(val=f"{large_threshold:,.2f}") + f": {large_count}")


def render_performance_insights(sales_df, metrics):
    """Render performance insights section"""
    st.subheader(get_text('performance_insights_header', st.session_state.get('language', 'en')))

    # Key insights
    insights = []

    # Revenue concentration
    top_5_customers = metrics['top_customers'].head(5)
    top_5_percentage = (top_5_customers.sum() / metrics['total_sales'] * 100) if metrics['total_sales'] > 0 else 0

    if top_5_percentage > 80:
        insights.append({
            'type': 'warning',
            'title': get_text('ins_high_conc_title', st.session_state.get('language', 'en')),
            'description': get_text('ins_high_conc_desc_tpl', st.session_state.get('language', 'en')).format(pct=top_5_percentage)
        })
    elif top_5_percentage > 60:
        insights.append({
            'type': 'info',
            'title': get_text('ins_mod_conc_title', st.session_state.get('language', 'en')),
            'description': get_text('ins_mod_conc_desc_tpl', st.session_state.get('language', 'en')).format(pct=top_5_percentage)
        })
    else:
        insights.append({
            'type': 'success',
            'title': get_text('ins_diversified_title', st.session_state.get('language', 'en')),
            'description': get_text('ins_diversified_desc_tpl', st.session_state.get('language', 'en')).format(pct=top_5_percentage)
        })

    # Invoice size analysis
    if sales_df['sales_amount'].mean() > 0:
        cv = sales_df['sales_amount'].std() / sales_df['sales_amount'].mean() * 100
        if cv > 100:
            insights.append({
                'type': 'info',
                'title': get_text('ins_high_var_title', st.session_state.get('language', 'en')),
                'description': get_text('ins_high_var_desc_tpl', st.session_state.get('language', 'en')).format(cv=cv)
            })

    # Growth trend
    if metrics['sales_growth'] > 10:
        insights.append({
            'type': 'success',
            'title': get_text('ins_strong_growth_title', st.session_state.get('language', 'en')),
            'description': get_text('ins_strong_growth_desc_tpl', st.session_state.get('language', 'en')).format(gr=metrics["sales_growth"])
        })
    elif metrics['sales_growth'] < -10:
        insights.append({
            'type': 'warning',
            'title': get_text('ins_decline_title', st.session_state.get('language', 'en')),
            'description': get_text('ins_decline_desc_tpl', st.session_state.get('language', 'en')).format(gr=metrics["sales_growth"])
        })

    # Display insights
    for insight in insights:
        if insight['type'] == 'success':
            st.success(f"✅ **{insight['title']}**: {insight['description']}")
        elif insight['type'] == 'warning':
            st.warning(f"⚠️ **{insight['title']}**: {insight['description']}")
        else:
            st.info(f"ℹ️ **{insight['title']}**: {insight['description']}")

    # Performance summary
    st.subheader(get_text('sales_performance_summary', st.session_state.get('language', 'en')))

    performance_data = {
        'Metric': [
            get_text('metric_revenue_eff', st.session_state.get('language', 'en')),
            get_text('metric_retention', st.session_state.get('language', 'en')),
            get_text('metric_processing', st.session_state.get('language', 'en')),
            get_text('metric_concentration', st.session_state.get('language', 'en')),
            get_text('metric_growth', st.session_state.get('language', 'en'))
        ],
        'Score': [
            85 if metrics['avg_invoice_value'] > 1000000 else 70,
            80 if metrics['unique_customers'] > 50 else 60,
            90 if metrics['total_invoices'] > 100 else 75,
            60 if top_5_percentage > 70 else 85,
            85 if metrics['sales_growth'] > 0 else 50
        ],
        'Status': [
            get_text('status_good', st.session_state.get('language', 'en')) if metrics['avg_invoice_value'] > 1000000 else get_text('status_fair', st.session_state.get('language', 'en')),
            get_text('status_good', st.session_state.get('language', 'en')) if metrics['unique_customers'] > 50 else get_text('status_fair', st.session_state.get('language', 'en')),
            get_text('status_excellent', st.session_state.get('language', 'en')) if metrics['total_invoices'] > 100 else get_text('status_good', st.session_state.get('language', 'en')),
            get_text('status_fair', st.session_state.get('language', 'en')) if top_5_percentage > 70 else get_text('status_good', st.session_state.get('language', 'en')),
            get_text('status_good', st.session_state.get('language', 'en')) if metrics['sales_growth'] > 0 else get_text('status_needs_improvement', st.session_state.get('language', 'en'))
        ]
    }

    performance_df = pd.DataFrame(performance_data)

    fig_performance = px.bar(
        performance_df,
        x='Metric',
        y='Score',
        color='Score',
        color_continuous_scale='RdYlGn',
        title=get_text('sales_performance_scorecard', st.session_state.get('language', 'en')),
        text='Status'
    )
    fig_performance.update_traces(textposition='outside')
    fig_performance.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig_performance, use_container_width=True)

    # Export functionality
    st.subheader(get_text('export_data', st.session_state.get('language', 'en')))

    col1, col2 = st.columns(2)

    with col1:
        # Customer summary export
        customer_export = sales_df.groupby('customer').agg({
            'sales_amount': ['sum', 'count', 'mean'],
            'date': ['min', 'max'] if 'date' in sales_df.columns and sales_df['date'].notna().any() else ['count', 'count']
        }).round(2)
        customer_export.columns = ['Total_Sales', 'Invoice_Count', 'Avg_Invoice', 'First_Sale', 'Last_Sale']

        csv_customers = customer_export.to_csv()
        st.download_button(
            get_text('download_customer_analysis', st.session_state.get('language', 'en')),
            data=csv_customers,
            file_name=f"customer_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

    with col2:
        # Sales summary export
        if 'date' in sales_df.columns and sales_df['date'].notna().any():
            sales_export = sales_df[sales_df['date'].notna()].copy()
            sales_export['month'] = sales_export['date'].dt.to_period('M')

            monthly_export = sales_export.groupby('month').agg({
                'sales_amount': ['sum', 'count', 'mean'],
                'customer': 'nunique'
            }).round(2)
            monthly_export.columns = ['Total_Sales', 'Invoice_Count', 'Avg_Invoice', 'Unique_Customers']

            csv_monthly = monthly_export.to_csv()
            st.download_button(
                get_text('download_monthly_sales_report', st.session_state.get('language', 'en')),
                data=csv_monthly,
                file_name=f"monthly_sales_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )


def render_product_explorer(sales_df):
    """Render integrated Product Explorer section - Complete version"""
    st.subheader(get_text('product_explorer_header', st.session_state.get('language', 'en')))
    st.caption(get_text('product_explorer_caption', st.session_state.get('language', 'en')))

    notes_col = 'Примечание к товару (работе, услуге)'

    # Filter for records with non-empty notes
    df_with_notes = sales_df[sales_df[notes_col].notna() & (sales_df[notes_col].str.strip() != '')].copy()

    if df_with_notes.empty:
        st.info(get_text('no_notes_data', st.session_state.get('language', 'en')))
        return

    # Calculate metrics
    total_notes = len(df_with_notes)
    unique_notes = df_with_notes[notes_col].nunique()
    notes_coverage = (len(df_with_notes) / len(sales_df) * 100) if len(sales_df) > 0 else 0
    avg_note_length = df_with_notes[notes_col].str.len().mean()
    total_notes_revenue = df_with_notes['sales_amount'].sum()

    # Key Metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(get_text('metric_total_notes', st.session_state.get('language', 'en')), f"{total_notes:,}", delta=get_text('delta_records_with_notes', st.session_state.get('language', 'en')))

    with col2:
        st.metric(get_text('metric_unique_products', st.session_state.get('language', 'en')), f"{unique_notes:,}", delta=get_text('delta_different_desc', st.session_state.get('language', 'en')))

    with col3:
        st.metric(get_text('metric_notes_coverage', st.session_state.get('language', 'en')), f"{notes_coverage:.1f}%", delta=get_text('delta_of_all_invoices', st.session_state.get('language', 'en')))

    with col4:
        st.metric(get_text('metric_avg_note_length', st.session_state.get('language', 'en')), f"{avg_note_length:.0f} {get_text('suffix_chars', st.session_state.get('language', 'en'))}")

    st.divider()

    # === FREQUENCY ANALYSIS ===
    st.subheader(get_text('frequency_analysis_header', st.session_state.get('language', 'en')))

    col1, col2 = st.columns(2)

    with col1:
        # Top products by frequency
        st.write("**" + get_text('top15_by_freq', st.session_state.get('language', 'en')) + "**")
        notes_counts = df_with_notes[notes_col].value_counts().head(15)

        fig_freq = px.bar(
            x=notes_counts.values,
            y=[note[:50] + "..." if len(note) > 50 else note for note in notes_counts.index],
            orientation='h',
            labels={'x': get_text('label_frequency', st.session_state.get('language', 'en')), 'y': get_text('col_product', st.session_state.get('language', 'en'))},
            color=notes_counts.values,
            color_continuous_scale='Viridis'
        )
        fig_freq.update_layout(height=600, showlegend=False)
        st.plotly_chart(fig_freq, use_container_width=True)

    with col2:
        # Top products by revenue
        st.write("**" + get_text('top15_by_revenue', st.session_state.get('language', 'en')) + "**")
        notes_revenue = df_with_notes.groupby(notes_col)['sales_amount'].sum().sort_values(ascending=False).head(15)

        fig_revenue = px.bar(
            x=notes_revenue.values,
            y=[note[:50] + "..." if len(note) > 50 else note for note in notes_revenue.index],
            orientation='h',
            labels={'x': get_text('label_revenue', st.session_state.get('language', 'en')), 'y': get_text('col_product', st.session_state.get('language', 'en'))},
            color=notes_revenue.values,
            color_continuous_scale='Oranges'
        )
        fig_revenue.update_layout(height=600, showlegend=False)
        st.plotly_chart(fig_revenue, use_container_width=True)

    st.divider()

    # === PRODUCT DISTRIBUTION ===
    st.subheader(get_text('product_distribution_header', st.session_state.get('language', 'en')))

    # Prepare product data
    all_products = df_with_notes.copy()
    all_products['product'] = all_products[notes_col]
    top_products = all_products.groupby('product')['sales_amount'].sum().sort_values(ascending=False).head(20)

    # Treemap visualization
    if len(top_products) > 0:
        treemap_data = []
        for product in top_products.index:
            product_data = all_products[all_products['product'] == product]
            total_amount = product_data['sales_amount'].sum()
            treemap_data.append({
                'product': product[:50] + '...' if len(product) > 50 else product,
                'direction': 'Sales',
                'amount': total_amount
            })

        if treemap_data:
            treemap_df = pd.DataFrame(treemap_data)

            fig_treemap = px.treemap(
                treemap_df,
                path=['direction', 'product'],
                values='amount',
                title=get_text('product_distribution_title', st.session_state.get('language', 'en')),
                color='amount',
                color_continuous_scale='Viridis',
                height=600
            )
            st.plotly_chart(fig_treemap, use_container_width=True)

    # Product trends over time
    st.write("**" + get_text('top_product_trends_title', st.session_state.get('language', 'en')) + "**")

    if len(top_products) >= 5 and 'date' in all_products.columns and all_products['date'].notna().any():
        # Get top 5 products
        top5_products = top_products.head(5).index

        # Create monthly trend for top products
        product_df_with_month = all_products.copy()
        product_df_with_month['month'] = product_df_with_month['date'].dt.to_period('M')

        product_trend = []
        for product in top5_products:
            trend = product_df_with_month[product_df_with_month['product'] == product].groupby('month')['sales_amount'].sum()
            for month, amount in trend.items():
                product_trend.append({
                    'product': product[:30] + '...' if len(product) > 30 else product,
                    'month': str(month),
                    'amount': amount
                })

        if product_trend:
            trend_df = pd.DataFrame(product_trend)

            fig_trend = px.line(
                trend_df,
                x='month',
                y='amount',
                color='product',
                title=get_text('top5_products_trend_title', st.session_state.get('language', 'en')),
                labels={'amount': get_text('label_revenue', st.session_state.get('language', 'en')), 'month': get_text('label_month', st.session_state.get('language', 'en'))}
            )
            fig_trend.update_layout(height=400)
            st.plotly_chart(fig_trend, use_container_width=True)

    st.divider()

    # === PRODUCT STATISTICS ===
    st.subheader(get_text('product_statistics_header', st.session_state.get('language', 'en')))

    # Find quantity column variants
    quantity_col = None
    for col in ['Количество', 'Quantity', 'qty', 'quantity']:
        if col in all_products.columns:
            quantity_col = col
            # Ensure it's numeric
            all_products[quantity_col] = pd.to_numeric(all_products[quantity_col], errors='coerce').fillna(0)
            break

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### " + get_text('top_sold_by_revenue', st.session_state.get('language', 'en')))

        # Check if quantity column exists
        if quantity_col:
            sold_products = all_products.groupby('product').agg({
                'sales_amount': ['sum', 'count'],
                quantity_col: 'sum'
            }).round(0)
            sold_products.columns = ['Total Revenue', 'Invoice Count', 'Total Quantity']
        else:
            sold_products = all_products.groupby('product').agg({
                'sales_amount': ['sum', 'count']
            }).round(0)
            sold_products.columns = ['Total Revenue', 'Invoice Count']

        sold_products = sold_products.sort_values('Total Revenue', ascending=False).head(10)

        # Format for display
        sold_products_display = sold_products.copy()
        sold_products_display['Total Revenue'] = sold_products_display['Total Revenue'].apply(lambda x: f"{x:,.0f}")
        if 'Total Quantity' in sold_products_display.columns:
            sold_products_display['Total Quantity'] = sold_products_display['Total Quantity'].apply(lambda x: f"{x:,.0f}")
        sold_products_display.index = [idx[:50] + '...' if len(idx) > 50 else idx for idx in sold_products_display.index]

        st.dataframe(sold_products_display, use_container_width=True, height=400)

    with col2:
        if quantity_col:
            st.markdown("### " + get_text('top_sold_by_quantity', st.session_state.get('language', 'en')))
            quantity_products = all_products.groupby('product').agg({
                quantity_col: 'sum',
                'sales_amount': 'sum'
            }).round(0)
            quantity_products.columns = ['Total Quantity', 'Total Revenue']
            quantity_products = quantity_products.sort_values('Total Quantity', ascending=False).head(10)

            # Format for display
            quantity_products_display = quantity_products.copy()
            quantity_products_display['Total Revenue'] = quantity_products_display['Total Revenue'].apply(lambda x: f"{x:,.0f}")
            quantity_products_display['Total Quantity'] = quantity_products_display['Total Quantity'].apply(lambda x: f"{x:,.0f}")
            quantity_products_display.index = [idx[:50] + '...' if len(idx) > 50 else idx for idx in quantity_products_display.index]

            st.dataframe(quantity_products_display, use_container_width=True, height=400)
        else:
            # st.info(get_text('no_quantity_info', st.session_state.get('language', 'en')))
            pass

    st.divider()

    # === DETAILED TABLE ===
    st.subheader(get_text('detailed_notes_header', st.session_state.get('language', 'en')))

    # Prepare detailed analysis
    if 'quantity' in df_with_notes.columns:
        notes_analysis = df_with_notes.groupby(notes_col).agg({
            'sales_amount': ['sum', 'count', 'mean'],
            'quantity': 'sum',
            'customer': 'nunique'
        }).round(2).reset_index()
        notes_analysis.columns = ['Product Note', 'Total Revenue', 'Frequency', 'Avg Revenue', 'Total Quantity', 'Customers']
    else:
        notes_analysis = df_with_notes.groupby(notes_col).agg({
            'sales_amount': ['sum', 'count', 'mean'],
            'customer': 'nunique'
        }).round(2).reset_index()
        notes_analysis.columns = ['Product Note', 'Total Revenue', 'Frequency', 'Avg Revenue', 'Customers']

    notes_analysis = notes_analysis.sort_values('Total Revenue', ascending=False)

    # Add note length column
    notes_analysis['Note Length'] = notes_analysis['Product Note'].str.len()

    # Format for display
    display_notes = notes_analysis.head(50).copy()
    display_notes['Total Revenue'] = display_notes['Total Revenue'].apply(lambda x: f"{x:,.2f}")
    display_notes['Avg Revenue'] = display_notes['Avg Revenue'].apply(lambda x: f"{x:,.2f}")
    display_notes['Product Note'] = display_notes['Product Note'].apply(lambda x: x[:120] + "..." if len(x) > 120 else x)

    # Search functionality
    search_term = st.text_input(get_text('search_in_notes_label', st.session_state.get('language', 'en')))

    if search_term:
        filtered_notes = display_notes[display_notes['Product Note'].str.contains(search_term, case=False, na=False)]
        st.write(get_text('found_notes_tpl', st.session_state.get('language', 'en')).format(len(filtered_notes), search_term))
        st.dataframe(filtered_notes, use_container_width=True, height=400)
    else:
        st.dataframe(display_notes, use_container_width=True, height=400)
        st.info(get_text('showing_top_notes_tpl', st.session_state.get('language', 'en')).format(len(notes_analysis)))

    # Export functionality
    csv_notes = notes_analysis.to_csv(index=False)
    st.download_button(
        get_text('download_product_notes', st.session_state.get('language', 'en')),
        data=csv_notes,
        file_name=f"product_notes_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )


def main() -> None:

    # Get language preference
    lang = st.session_state.get('language', 'en')

    st.title(get_text('sales_title', lang))
    st.caption(get_text('sales_caption', lang))

    # Add cache refresh button at the top
    col1, col2 = st.columns([6, 1])
    with col2:
        if st.button("🔄 Refresh", help="Clear cache and reload data from database"):
            from utils.data_loader import refresh_user_data
            refresh_user_data()
            st.rerun()

    # Get data
    sales_data = get_sales_data()
    invoices_df = sales_data['invoices_out']

    if invoices_df is None or invoices_df.empty:
        st.warning(get_text('sales_no_outgoing_warning', lang))
        st.page_link("pages/file_upload.py", label=get_text('go_to_file_upload_short', lang), icon="📁")
        return

    # Process sales data
    with st.spinner(get_text('sales_processing', lang)):
        sales_df = process_sales_data(invoices_df)

    if sales_df.empty:
        st.warning(get_text('sales_no_processed', lang))
        return

    # Calculate metrics
    metrics = calculate_sales_metrics(sales_df)

    # Summary metrics
    render_sales_summary(sales_df, metrics)

    st.divider()

    # Tabs for different analyses
    tabs = st.tabs([
        get_text('sales_tab_trends', lang),
        get_text('sales_tab_customers', lang),
        get_text('sales_tab_products', lang)
    ])

    with tabs[0]:
        render_sales_trends(sales_df)

    with tabs[1]:
        render_customer_analysis(sales_df)

    with tabs[2]:
        render_product_explorer(sales_df)


if __name__ == "__main__":
    main()
