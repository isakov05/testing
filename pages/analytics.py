"""
Business Overview — single-page analytics dashboard for company owners.
Combines AR, AP, Sales analytics and company health rating.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date, timedelta

from auth.db_authenticator import protect_page
from utils.db_operations import load_user_invoices, load_user_bank_transactions, load_user_reconciliation
from utils.dashboard_metrics import calculate_company_health_rating
from utils.risk_engine import RiskEngine, load_risk_config
from utils.risk_queries import (
    get_all_invoices_and_payments,
    get_invoices_with_payments,
    calculate_counterparty_lookback_period,
)
from utils.analytics_metrics import (
    format_currency,
    calc_revenue,
    calc_expenses,
    calc_gross_profit_margin,
    calc_ar_total,
    calc_ap_total,
    calc_net_position,
    calc_current_ratio,
    calc_dso,
    calc_dpo,
    calc_cash_conversion_cycle,
    calc_active_counterparties,
    calc_overdue_ar,
    calc_concentration_risk,
    calc_bank_activity,
    monthly_revenue_trend,
    top_buyers,
    top_suppliers,
    ar_aging_buckets,
    ap_aging_buckets,
    counterparty_summary,
    generate_alerts,
)

protect_page()

# ---------------------------------------------------------------------------
# Page header & date filter
# ---------------------------------------------------------------------------
st.title("Business Overview")
st.caption("All your key financial metrics in one place")

header_left, header_right = st.columns([5, 1])
with header_right:
    if st.button("Refresh", help="Reload data from database"):
        st.cache_data.clear()
        st.rerun()

col_start, col_end, col_spacer = st.columns([1, 1, 3])
with col_start:
    date_start = st.date_input("From", value=datetime.now() - timedelta(days=365))
with col_end:
    date_end = st.date_input("To", value=datetime.now())

start_dt = datetime.combine(date_start, datetime.min.time())
end_dt = datetime.combine(date_end, datetime.max.time())

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
user_id = st.session_state.get('user_id')
if not user_id:
    st.warning("No user session found. Please log in.")
    st.stop()

with st.spinner("Loading data..."):
    uid_str = str(user_id)
    invoices_out = load_user_invoices(uid_str, 'OUT')
    invoices_in = load_user_invoices(uid_str, 'IN')
    bank_txns = load_user_bank_transactions(uid_str)
    recon_ar = load_user_reconciliation(uid_str, 'IN')
    recon_ap = load_user_reconciliation(uid_str, 'OUT')

    # Convert empty DataFrames to None for consistent checks
    if invoices_out.empty:
        invoices_out = None
    if invoices_in.empty:
        invoices_in = None
    if bank_txns.empty:
        bank_txns = None
    if recon_ar.empty:
        recon_ar = None
    if recon_ap.empty:
        recon_ap = None

# Check if we have any data at all
has_data = invoices_out is not None or invoices_in is not None or bank_txns is not None

if not has_data:
    st.info("No data available yet. Upload invoices to see your analytics.")
    st.stop()

has_bank_data = bank_txns is not None

# Use empty DataFrames as fallback for downstream functions
if invoices_out is None:
    invoices_out = pd.DataFrame()
if invoices_in is None:
    invoices_in = pd.DataFrame()
if bank_txns is None:
    bank_txns = pd.DataFrame()
if recon_ar is None:
    recon_ar = pd.DataFrame()
if recon_ap is None:
    recon_ap = pd.DataFrame()

# ---------------------------------------------------------------------------
# Compute KPIs
# ---------------------------------------------------------------------------
revenue = calc_revenue(invoices_out, start_dt, end_dt)
expenses = calc_expenses(invoices_in, start_dt, end_dt)
gross_margin = calc_gross_profit_margin(revenue['value'], expenses['value'])
ar_total = calc_ar_total(recon_ar, invoices_out, bank_txns)
ap_total = calc_ap_total(recon_ap, invoices_in, bank_txns)
# Handle None AR/AP (no bank/reconciliation data)
ar_available = ar_total is not None
ap_available = ap_total is not None
ar_total = ar_total if ar_total is not None else 0.0
ap_total = ap_total if ap_total is not None else 0.0
net_pos = calc_net_position(ar_total, ap_total) if (ar_available or ap_available) else None
current_ratio = calc_current_ratio(ar_total, ap_total) if (ar_available or ap_available) else None
dso = calc_dso(invoices_out, bank_txns, start_dt, end_dt)
dpo = calc_dpo(invoices_in, bank_txns, start_dt, end_dt)
ccc = calc_cash_conversion_cycle(dso, dpo)
counterparties = calc_active_counterparties(invoices_out, invoices_in, start_dt, end_dt)
overdue = calc_overdue_ar(invoices_out, bank_txns)
concentration = calc_concentration_risk(invoices_out, start_dt, end_dt)
bank_activity = calc_bank_activity(bank_txns, start_dt, end_dt)
health = calculate_company_health_rating(str(user_id))

# ---------------------------------------------------------------------------
# Section 1: Health Grade + KPI Cards
# ---------------------------------------------------------------------------
st.markdown("---")

# Health badge row
h_col1, h_col2, h_col3 = st.columns([1, 2, 2])
with h_col1:
    grade = health.get('rating', 'N/A')
    score = health.get('score', 0)
    color_map = {'A+': '#10B981', 'A': '#10B981', 'B+': '#3B82F6', 'B': '#3B82F6',
                 'C+': '#F59E0B', 'C': '#F59E0B', 'D': '#EF4444'}
    grade_color = color_map.get(grade, '#6B7280')
    st.markdown(f"""
    <div style="text-align:center; padding:16px; background:{grade_color}15; border-radius:12px; border:2px solid {grade_color};">
        <div style="font-size:48px; font-weight:bold; color:{grade_color};">{grade}</div>
        <div style="font-size:14px; color:#6B7280;">Health Rating</div>
        <div style="font-size:20px; font-weight:600; color:{grade_color};">{score:.0f}/100</div>
    </div>
    """, unsafe_allow_html=True)

with h_col2:
    trend = health.get('trend', 'stable')
    trend_icon = {'improving': '&uarr;', 'declining': '&darr;', 'stable': '&rarr;'}
    trend_color = {'improving': '#10B981', 'declining': '#EF4444', 'stable': '#6B7280'}
    st.markdown(f"""
    <div style="padding:16px; background:#F9FAFB; border-radius:12px;">
        <div style="font-size:14px; color:#6B7280;">Trend</div>
        <div style="font-size:24px; color:{trend_color.get(trend, '#6B7280')};">
            {trend_icon.get(trend, '&rarr;')} {trend.capitalize()}
        </div>
    </div>
    """, unsafe_allow_html=True)

    if health.get('explanation'):
        for exp in health['explanation'][:3]:
            st.caption(f"- {exp}")

with h_col3:
    # Alerts
    alerts = generate_alerts(revenue, ar_total, ap_total, overdue, concentration, dso, health)
    for alert in alerts[:4]:
        sev = alert['severity']
        icon = {'danger': '🔴', 'warning': '🟡', 'success': '🟢'}.get(sev, '⚪')
        st.markdown(f"{icon} {alert['message']}")

# ---------------------------------------------------------------------------
# KPI Row 1: The Big Picture
# ---------------------------------------------------------------------------
st.markdown("### Financial Summary")

r1c1, r1c2, r1c3, r1c4 = st.columns(4)

with r1c1:
    delta_str = f"{revenue['growth_pct']:+.1f}%" if revenue['previous'] > 0 else None
    st.metric("Revenue", format_currency(revenue['value']), delta=delta_str)

with r1c2:
    delta_str = f"{expenses['growth_pct']:+.1f}%" if expenses['previous'] > 0 else None
    st.metric("Expenses", format_currency(expenses['value']),
              delta=delta_str, delta_color="inverse")

with r1c3:
    st.metric("Net Position (AR-AP)",
              format_currency(net_pos) if net_pos is not None else "N/A",
              help="Requires bank statements or reconciliation data" if net_pos is None else None)

with r1c4:
    st.metric("Gross Margin", f"{gross_margin:.1f}%")

# ---------------------------------------------------------------------------
# KPI Row 2: Receivables & Payables
# ---------------------------------------------------------------------------
r2c1, r2c2, r2c3, r2c4 = st.columns(4)

with r2c1:
    st.metric("Accounts Receivable",
              format_currency(ar_total) if ar_available else "N/A",
              help="Upload bank statements or reconciliation to calculate" if not ar_available else None)

with r2c2:
    st.metric("Accounts Payable",
              format_currency(ap_total) if ap_available else "N/A",
              help="Upload bank statements or reconciliation to calculate" if not ap_available else None)

with r2c3:
    if current_ratio is not None and current_ratio != float('inf'):
        st.metric("Current Ratio (AR/AP)", f"{current_ratio:.2f}")
    else:
        st.metric("Current Ratio (AR/AP)", "N/A")

with r2c4:
    st.metric("Overdue AR", format_currency(overdue['total']),
              delta=f"{overdue['count']} invoices", delta_color="inverse")

# ---------------------------------------------------------------------------
# KPI Row 3: Operational Efficiency
# ---------------------------------------------------------------------------
r3c1, r3c2, r3c3, r3c4 = st.columns(4)

with r3c1:
    dso_label = f"{dso:.0f} days" if has_bank_data else f"~{dso:.0f} days"
    st.metric("DSO (Days to Collect)", dso_label,
              help="Estimated (no bank data)" if not has_bank_data else None)

with r3c2:
    dpo_label = f"{dpo:.0f} days" if has_bank_data else f"~{dpo:.0f} days"
    st.metric("DPO (Days to Pay)", dpo_label,
              help="Estimated (no bank data)" if not has_bank_data else None)

with r3c3:
    st.metric("Cash Cycle (DSO-DPO)", f"{ccc:.0f} days")

with r3c4:
    st.metric("Concentration Risk", f"{concentration['top_n_pct']:.0f}%",
              help=f"% of revenue from top {concentration['top_n']} clients")

# ---------------------------------------------------------------------------
# KPI Row 4: Activity
# ---------------------------------------------------------------------------
r4c1, r4c2, r4c3, r4c4 = st.columns(4)

with r4c1:
    st.metric("Active Buyers", counterparties['buyers'])

with r4c2:
    st.metric("Active Suppliers", counterparties['suppliers'])

with r4c3:
    st.metric("Bank Transactions",
              bank_activity['total_txns'] if has_bank_data else "N/A")

with r4c4:
    rev_growth_display = f"{revenue['growth_pct']:+.1f}%" if revenue['previous'] > 0 else "N/A"
    st.metric("Revenue Growth", rev_growth_display)

# ---------------------------------------------------------------------------
# Section 2: Money Flow (charts)
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown("### Money Flow")

chart_left, chart_right = st.columns(2)

with chart_left:
    # Dual-axis: Revenue + Unique Customers
    trend_df = monthly_revenue_trend(invoices_out, months=12)
    if not trend_df.empty:
        # Count unique customers per month
        out_signed = invoices_out.copy() if invoices_out is not None and not invoices_out.empty else pd.DataFrame()
        if not out_signed.empty and 'Status' in out_signed.columns:
            out_signed = out_signed[out_signed['Status'].isin(['Подписан', 'Signed', 'signed'])]
        if not out_signed.empty and 'Document Date' in out_signed.columns and 'Buyer (Tax ID or PINFL)' in out_signed.columns:
            out_signed['Document Date'] = pd.to_datetime(out_signed['Document Date'], errors='coerce')
            out_signed['_month'] = out_signed['Document Date'].dt.to_period('M').dt.to_timestamp()
            cust_monthly = out_signed.groupby('_month')['Buyer (Tax ID or PINFL)'].nunique().reset_index()
            cust_monthly.columns = ['month', 'customers']
            trend_df = trend_df.merge(cust_monthly, on='month', how='left')
            trend_df['customers'] = trend_df['customers'].fillna(0).astype(int)
        else:
            trend_df['customers'] = 0

        fig = go.Figure()
        fig.add_trace(go.Bar(x=trend_df['month'], y=trend_df['revenue'],
                             name='Revenue', marker_color='#0B5C5F', yaxis='y'))
        fig.add_trace(go.Scatter(x=trend_df['month'], y=trend_df['customers'],
                                 mode='lines+markers', name='Customers',
                                 line=dict(color='#F59E0B', width=3), yaxis='y2'))
        fig.update_layout(
            title='Monthly Revenue & Customers',
            yaxis=dict(title='Revenue', side='left'),
            yaxis2=dict(title='Customers', side='right', overlaying='y'),
            height=380, margin=dict(l=20, r=20, t=40, b=20),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No revenue data to chart")

with chart_right:
    # AR vs AP comparison
    if ar_total > 0 or ap_total > 0:
        fig = go.Figure()
        fig.add_trace(go.Bar(name='Receivable (AR)', x=['AR vs AP'], y=[ar_total],
                             marker_color='#10B981'))
        fig.add_trace(go.Bar(name='Payable (AP)', x=['AR vs AP'], y=[ap_total],
                             marker_color='#EF4444'))
        fig.update_layout(barmode='group', title='AR vs AP',
                          height=380, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No AR/AP data available")

# Top buyers and suppliers side by side
buy_col, sup_col = st.columns(2)

with buy_col:
    st.markdown("#### Top Buyers")
    buyers_df = top_buyers(invoices_out, start_dt, end_dt, n=10)
    if not buyers_df.empty:
        fig = px.bar(buyers_df, x='total', y='buyer', orientation='h',
                     title='', labels={'total': 'Revenue', 'buyer': ''})
        fig.update_layout(yaxis={'categoryorder': 'total ascending'},
                          height=400, margin=dict(l=20, r=20, t=10, b=20),
                          showlegend=False)
        fig.update_traces(marker_color='#0B5C5F')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No buyer data")

with sup_col:
    st.markdown("#### Top Suppliers")
    suppliers_df = top_suppliers(invoices_in, start_dt, end_dt, n=10)
    if not suppliers_df.empty:
        fig = px.bar(suppliers_df, x='total', y='supplier', orientation='h',
                     title='', labels={'total': 'Spend', 'supplier': ''})
        fig.update_layout(yaxis={'categoryorder': 'total ascending'},
                          height=400, margin=dict(l=20, r=20, t=10, b=20),
                          showlegend=False)
        fig.update_traces(marker_color='#F59E0B')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No supplier data")

# ---------------------------------------------------------------------------
# Section 2b: Sales Analytics
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown("### Sales Analytics")

# Prepare sales data
_sales_df = invoices_out.copy() if invoices_out is not None and not invoices_out.empty else pd.DataFrame()
if not _sales_df.empty:
    if 'Status' in _sales_df.columns:
        _sales_df = _sales_df[_sales_df['Status'].isin(['Подписан', 'Signed', 'signed'])]
    _sales_df['_date'] = pd.to_datetime(_sales_df.get('Document Date'), errors='coerce')
    _sales_df['_amount'] = pd.to_numeric(_sales_df.get('Supply Value (incl. VAT)'), errors='coerce').fillna(0)
    _sales_df['_customer'] = _sales_df.get('Buyer (Name)', pd.Series(['Unknown'] * len(_sales_df))).astype(str).str.strip()
    _sales_df = _sales_df[(_sales_df['_date'] >= pd.Timestamp(start_dt)) & (_sales_df['_date'] <= pd.Timestamp(end_dt))]

if not _sales_df.empty and _sales_df['_amount'].sum() > 0:
    sa_col1, sa_col2 = st.columns(2)

    with sa_col1:
        # Pareto / Concentration chart
        st.markdown("#### Customer Concentration (Pareto)")
        cust_sales = _sales_df.groupby('_customer')['_amount'].sum().sort_values(ascending=False)
        cust_cum_pct = (cust_sales / cust_sales.sum() * 100).cumsum()

        pareto_n = min(20, len(cust_sales))
        pareto_df = pd.DataFrame({
            'customer': cust_sales.index[:pareto_n],
            'sales': cust_sales.values[:pareto_n],
            'cumulative_pct': cust_cum_pct.values[:pareto_n]
        })

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=list(range(pareto_n)), y=pareto_df['sales'],
            name='Sales', marker_color='#3B82F6', yaxis='y'))
        fig.add_trace(go.Scatter(
            x=list(range(pareto_n)), y=pareto_df['cumulative_pct'],
            mode='lines+markers', name='Cumulative %',
            line=dict(color='#EF4444', width=2), yaxis='y2'))
        fig.update_layout(
            xaxis=dict(title='Customer Rank',
                       tickvals=list(range(pareto_n)),
                       ticktext=[f"C{i+1}" for i in range(pareto_n)]),
            yaxis=dict(title='Sales', side='left'),
            yaxis2=dict(title='Cumulative %', side='right', overlaying='y', range=[0, 105]),
            height=400, margin=dict(l=20, r=20, t=10, b=20),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
        )
        st.plotly_chart(fig, use_container_width=True)

    with sa_col2:
        # Invoice size distribution
        st.markdown("#### Invoice Size Distribution")
        fig = px.histogram(_sales_df, x='_amount', nbins=30,
                           labels={'_amount': 'Invoice Amount', 'count': 'Count'},
                           color_discrete_sequence=['#0B5C5F'])
        fig.update_layout(height=400, margin=dict(l=20, r=20, t=10, b=20), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    sa_col3, sa_col4 = st.columns(2)

    with sa_col3:
        # Invoice frequency by day of week
        st.markdown("#### Invoicing by Day of Week")
        if _sales_df['_date'].notna().any():
            dow = _sales_df[_sales_df['_date'].notna()].copy()
            dow['_dow'] = dow['_date'].dt.day_name()
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            freq = dow.groupby('_dow').size().reindex(day_order, fill_value=0)
            fig = px.bar(x=freq.index, y=freq.values,
                         labels={'x': '', 'y': 'Invoices'},
                         color=freq.values, color_continuous_scale='Blues')
            fig.update_layout(height=350, margin=dict(l=20, r=20, t=10, b=20), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No date data for day-of-week analysis")

    with sa_col4:
        # Performance Scorecard
        st.markdown("#### Performance Scorecard")
        total_sales = _sales_df['_amount'].sum()
        total_inv = len(_sales_df)
        unique_cust = _sales_df['_customer'].nunique()
        avg_inv = _sales_df['_amount'].mean()
        top5_pct = concentration['top_n_pct'] if concentration['top_n_pct'] > 0 else 0

        perf_data = pd.DataFrame({
            'Metric': ['Revenue Efficiency', 'Client Diversity', 'Invoice Volume', 'Concentration', 'Growth'],
            'Score': [
                85 if avg_inv > 1_000_000 else 70 if avg_inv > 100_000 else 55,
                85 if unique_cust > 50 else 70 if unique_cust > 20 else 55,
                90 if total_inv > 100 else 75 if total_inv > 30 else 55,
                85 if top5_pct < 50 else 60 if top5_pct < 70 else 40,
                85 if revenue['growth_pct'] > 5 else 65 if revenue['growth_pct'] >= 0 else 40,
            ],
        })
        fig = px.bar(perf_data, x='Metric', y='Score', color='Score',
                     color_continuous_scale='RdYlGn', range_color=[0, 100],
                     text='Score')
        fig.update_traces(textposition='outside')
        fig.update_layout(height=350, margin=dict(l=20, r=20, t=10, b=20),
                          showlegend=False, yaxis_range=[0, 105])
        st.plotly_chart(fig, use_container_width=True)

    # Product Explorer
    notes_col = 'Примечание к товару (работе, услуге)'
    if notes_col in _sales_df.columns:
        _with_notes = _sales_df[_sales_df[notes_col].notna() & (_sales_df[notes_col].str.strip() != '')].copy()
        if not _with_notes.empty:
            st.markdown("#### Product Explorer")

            pe1, pe2, pe3, pe4 = st.columns(4)
            with pe1:
                st.metric("Products with Notes", f"{len(_with_notes):,}")
            with pe2:
                st.metric("Unique Products", f"{_with_notes[notes_col].nunique():,}")
            with pe3:
                coverage = len(_with_notes) / len(_sales_df) * 100 if len(_sales_df) > 0 else 0
                st.metric("Notes Coverage", f"{coverage:.1f}%")
            with pe4:
                st.metric("Product Revenue", format_currency(_with_notes['_amount'].sum()))

            prod_left, prod_right = st.columns(2)

            with prod_left:
                st.markdown("**Top Products by Revenue**")
                prod_rev = _with_notes.groupby(notes_col)['_amount'].sum().sort_values(ascending=False).head(15)
                fig = px.bar(
                    x=prod_rev.values,
                    y=[n[:50] + '...' if len(n) > 50 else n for n in prod_rev.index],
                    orientation='h', labels={'x': 'Revenue', 'y': ''},
                    color=prod_rev.values, color_continuous_scale='Oranges')
                fig.update_layout(height=500, showlegend=False,
                                  margin=dict(l=20, r=20, t=10, b=20))
                st.plotly_chart(fig, use_container_width=True)

            with prod_right:
                st.markdown("**Top Products by Frequency**")
                prod_freq = _with_notes[notes_col].value_counts().head(15)
                fig = px.bar(
                    x=prod_freq.values,
                    y=[n[:50] + '...' if len(n) > 50 else n for n in prod_freq.index],
                    orientation='h', labels={'x': 'Frequency', 'y': ''},
                    color=prod_freq.values, color_continuous_scale='Viridis')
                fig.update_layout(height=500, showlegend=False,
                                  margin=dict(l=20, r=20, t=10, b=20))
                st.plotly_chart(fig, use_container_width=True)

            # Treemap
            top_prods = _with_notes.groupby(notes_col)['_amount'].sum().sort_values(ascending=False).head(20)
            if len(top_prods) > 0:
                tree_data = pd.DataFrame({
                    'product': [p[:50] + '...' if len(p) > 50 else p for p in top_prods.index],
                    'category': 'Sales',
                    'amount': top_prods.values
                })
                fig = px.treemap(tree_data, path=['category', 'product'], values='amount',
                                 title='Product Distribution', color='amount',
                                 color_continuous_scale='Viridis', height=500)
                st.plotly_chart(fig, use_container_width=True)

            # Product trends (top 5)
            if _with_notes['_date'].notna().any():
                top5_prods = _with_notes.groupby(notes_col)['_amount'].sum().sort_values(ascending=False).head(5).index
                trend_data = []
                _with_notes['_month'] = _with_notes['_date'].dt.to_period('M')
                for p in top5_prods:
                    p_trend = _with_notes[_with_notes[notes_col] == p].groupby('_month')['_amount'].sum()
                    for m, a in p_trend.items():
                        trend_data.append({
                            'product': p[:30] + '...' if len(p) > 30 else p,
                            'month': str(m), 'amount': a
                        })
                if trend_data:
                    fig = px.line(pd.DataFrame(trend_data), x='month', y='amount', color='product',
                                 title='Top 5 Product Trends',
                                 labels={'amount': 'Revenue', 'month': ''})
                    fig.update_layout(height=400, margin=dict(l=20, r=20, t=40, b=20))
                    st.plotly_chart(fig, use_container_width=True)

            # Searchable product table
            with st.expander("Product Details Table"):
                prod_search = st.text_input("Search products", placeholder="Product name...",
                                            key="prod_search")
                prod_table = _with_notes.groupby(notes_col).agg({
                    '_amount': ['sum', 'count', 'mean'],
                    '_customer': 'nunique'
                }).round(0).reset_index()
                prod_table.columns = ['Product', 'Total Revenue', 'Count', 'Avg Revenue', 'Customers']
                prod_table = prod_table.sort_values('Total Revenue', ascending=False)

                if prod_search:
                    prod_table = prod_table[prod_table['Product'].str.contains(prod_search, case=False, na=False)]

                st.dataframe(prod_table.head(50), use_container_width=True, hide_index=True,
                             column_config={
                                 'Total Revenue': st.column_config.NumberColumn(format="%.0f"),
                                 'Avg Revenue': st.column_config.NumberColumn(format="%.0f"),
                             })
else:
    st.info("No sales data available for the selected period")

# ---------------------------------------------------------------------------
# Section 3: Risk & Aging
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown("### Risk & Aging")

aging_left, aging_right = st.columns(2)

with aging_left:
    st.markdown("#### Receivables Aging")
    ar_aging = ar_aging_buckets(recon_ar, invoices_out)
    if not ar_aging.empty and ar_aging['amount'].sum() > 0:
        fig = px.bar(ar_aging, x='bucket', y='amount',
                     text='count', title='',
                     labels={'bucket': '', 'amount': 'Amount', 'count': 'Invoices'})
        fig.update_traces(marker_color='#3B82F6', textposition='outside')
        fig.update_layout(height=350, margin=dict(l=20, r=20, t=10, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No AR aging data")

with aging_right:
    st.markdown("#### Payables Aging")
    ap_aging = ap_aging_buckets(invoices_in)
    if not ap_aging.empty and ap_aging['amount'].sum() > 0:
        fig = px.bar(ap_aging, x='bucket', y='amount',
                     text='count', title='',
                     labels={'bucket': '', 'amount': 'Amount', 'count': 'Invoices'})
        fig.update_traces(marker_color='#F59E0B', textposition='outside')
        fig.update_layout(height=350, margin=dict(l=20, r=20, t=10, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No AP aging data")

# ---------------------------------------------------------------------------
# Section 4: Counterparty Quick View + Risk Analysis
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown("### Counterparty Overview")

search = st.text_input("Search counterparty", placeholder="Name or INN...")

cp_df = counterparty_summary(invoices_out, invoices_in, recon_ar, recon_ap, start_dt, end_dt)

if not cp_df.empty:
    display_df = cp_df.copy()
    if search:
        mask = (display_df['name'].str.contains(search, case=False, na=False) |
                display_df['inn'].astype(str).str.contains(search, na=False))
        display_df = display_df[mask]

    st.dataframe(
        display_df[['name', 'inn', 'type', 'volume', 'invoice_count', 'outstanding']].rename(columns={
            'name': 'Counterparty',
            'inn': 'INN',
            'type': 'Type',
            'volume': 'Total Volume',
            'invoice_count': 'Invoices',
            'outstanding': 'Outstanding',
        }),
        use_container_width=True,
        hide_index=True,
        column_config={
            'Total Volume': st.column_config.NumberColumn(format="%.0f"),
            'Outstanding': st.column_config.NumberColumn(format="%.0f"),
        }
    )
    st.caption(f"Showing {len(display_df)} counterparties")

    # --- Select & Analyze ---
    if not display_df.empty:
        options = [f"{row['name']}  ({row['inn']})" for _, row in display_df.iterrows()]
        inn_map = {f"{row['name']}  ({row['inn']})": row['inn'] for _, row in display_df.iterrows()}
        name_map = {f"{row['name']}  ({row['inn']})": row['name'] for _, row in display_df.iterrows()}

        selected = st.selectbox("Select counterparty to analyze", options=[""] + options,
                                index=0, key="cp_select")

        if selected and st.button("Analyze", type="primary"):
            selected_inn = str(inn_map[selected]).replace('.0', '').strip()
            selected_name = name_map[selected]

            with st.spinner(f"Running risk analysis for {selected_name}..."):
                # Determine lookback period (same as Risk Engine page)
                lookback = calculate_counterparty_lookback_period(
                    str(user_id), selected_inn, invoice_type='OUT'
                )

                # Fetch ALL invoices and payments (same as Risk Engine page)
                invoices_df, payments_df = get_all_invoices_and_payments(
                    str(user_id), invoice_type='OUT', months_back=lookback
                )

                # Clean INNs in dataframes (same as Risk Engine page)
                if not invoices_df.empty and 'buyer_inn' in invoices_df.columns:
                    invoices_df['buyer_inn'] = invoices_df['buyer_inn'].astype(str).str.replace('.0', '', regex=False).str.strip()
                if not payments_df.empty and 'counterparty_inn' in payments_df.columns:
                    payments_df['counterparty_inn'] = payments_df['counterparty_inn'].astype(str).str.replace('.0', '', regex=False).str.strip()

                # Run Risk Engine on full dataset
                config = load_risk_config()
                engine = RiskEngine(config=config, user_id=str(user_id))
                components = engine.reconstruct_invoice_components(
                    invoices_df, payments_df, invoice_type='OUT'
                )
                risk = engine.calculate_counterparty_risk(selected_inn, components)

                # Filter components for this counterparty only (for display)
                components = [
                    c for c in components
                    if str(c.get('counterparty_inn', '')).replace('.0', '').strip() == selected_inn
                ]

            # Store in session state so it persists
            st.session_state['risk_result'] = risk
            st.session_state['risk_cp_name'] = selected_name
            st.session_state['risk_cp_inn'] = selected_inn
            st.session_state['risk_components'] = components

    # --- Display risk results if available ---
    if st.session_state.get('risk_result'):
        risk = st.session_state['risk_result']
        cp_name = st.session_state.get('risk_cp_name', '')
        cp_inn = st.session_state.get('risk_cp_inn', '')
        components = st.session_state.get('risk_components', [])

        st.markdown("---")
        st.markdown(f"### Risk Analysis: {cp_name}")
        st.caption(f"INN: {cp_inn}")

        # ===== TABS =====
        tab_overview, tab_components, tab_aging = st.tabs(["Overview", "Components", "Aging Dashboard"])

        # ----------------------------------------------------------------
        # TAB 1: Overview
        # ----------------------------------------------------------------
        with tab_overview:
            # Rating badge
            rating = risk.get('rating', 'N/A')
            color_map = {'A': '#10B981', 'B': '#3B82F6', 'C': '#F59E0B', 'D': '#EF4444'}
            badge_color = color_map.get(rating, '#6B7280')

            rc1, rc2, rc3, rc4, rc5 = st.columns(5)
            with rc1:
                st.markdown(f"""
                <div style="text-align:center; padding:12px; background:{badge_color}15; border-radius:8px; border:2px solid {badge_color};">
                    <div style="font-size:36px; font-weight:bold; color:{badge_color};">{rating}</div>
                    <div style="font-size:12px; color:#6B7280;">Risk Rating</div>
                </div>
                """, unsafe_allow_html=True)
            with rc2:
                st.metric("PD (Prob. of Default)", f"{risk.get('pd', 0):.2%}")
            with rc3:
                st.metric("LGD (Loss Given Default)", f"{risk.get('lgd', 0):.2%}")
            with rc4:
                st.metric("EAD (Exposure)", format_currency(risk.get('ead_current', 0)))
            with rc5:
                st.metric("Expected Loss", format_currency(risk.get('expected_loss', 0)))

            rl1, rl2, rl3 = st.columns(3)
            with rl1:
                st.metric("Recommended Credit Limit", format_currency(risk.get('recommended_limit', 0)))
            with rl2:
                features = risk.get('behavioral_features', {})
                st.metric("Avg Days Past Due", f"{features.get('weighted_avg_dpd', 0):.0f} days")
            with rl3:
                st.metric("Late Payment Rate", f"{features.get('late_payment_rate', 0):.0%}")

            # Justification
            justification = risk.get('justification', {})
            if justification:
                with st.expander("Risk Justification"):
                    if justification.get('pd'):
                        st.markdown(f"**PD:** {justification['pd']}")
                    if justification.get('lgd'):
                        st.markdown(f"**LGD:** {justification['lgd']}")
                    if justification.get('ead'):
                        st.markdown(f"**EAD:** {justification['ead']}")

            # Payment behavior charts
            if components:
                comp_df = pd.DataFrame(components)
                st.markdown("#### Payment Behavior")

                beh1, beh2 = st.columns(2)
                with beh1:
                    type_summary = comp_df.groupby('component_type')['component_amount'].sum().reset_index()
                    type_summary.columns = ['type', 'amount']
                    color_discrete = {'paid': '#10B981', 'open': '#F59E0B', 'returned': '#EF4444'}
                    if not type_summary.empty:
                        fig = px.pie(type_summary, values='amount', names='type',
                                     title='Invoice Status Breakdown',
                                     color='type', color_discrete_map=color_discrete)
                        fig.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=20))
                        st.plotly_chart(fig, use_container_width=True)

                with beh2:
                    paid = comp_df[comp_df['component_type'] == 'paid']
                    if not paid.empty:
                        fig = px.histogram(paid, x='dpd', nbins=20,
                                           title='Days Past Due Distribution (Paid)',
                                           labels={'dpd': 'Days Past Due', 'count': 'Invoices'})
                        fig.update_traces(marker_color='#3B82F6')
                        fig.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=20))
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No paid invoices to show DPD distribution")

                # Financial Summary
                st.markdown("#### Financial Summary")
                total_invoiced = comp_df[comp_df['component_type'] != 'returned']['component_amount'].sum()
                paid_total = comp_df[comp_df['component_type'] == 'paid']['component_amount'].sum()
                returned_total = comp_df[comp_df['component_type'] == 'returned']['component_amount'].sum()
                unpaid_total = comp_df[comp_df['component_type'] == 'open']['component_amount'].sum()

                if total_invoiced > 0:
                    fs1, fs2, fs3, fs4 = st.columns(4)
                    with fs1:
                        st.metric("Total Invoiced", f"{total_invoiced:,.0f}")
                    with fs2:
                        st.metric("Total Paid", f"{paid_total:,.0f}",
                                  delta=f"{paid_total/total_invoiced*100:.1f}%")
                    with fs3:
                        st.metric("Total Returned", f"{returned_total:,.0f}",
                                  delta=f"{returned_total/total_invoiced*100:.1f}%", delta_color="inverse")
                    with fs4:
                        st.metric("Total Unpaid", f"{unpaid_total:,.0f}",
                                  delta=f"{unpaid_total/total_invoiced*100:.1f}%", delta_color="inverse")

                # Payment Behavior Insights
                paid_df = comp_df[comp_df['component_type'] == 'paid']
                if not paid_df.empty:
                    st.markdown("#### Payment Behavior Insights")
                    pi1, pi2, pi3 = st.columns(3)
                    with pi1:
                        st.metric("Average DPD (Paid)", f"{paid_df['dpd'].mean():.0f} days")
                    with pi2:
                        on_time = (paid_df['dpd'] <= 30).sum() / len(paid_df) * 100
                        st.metric("On-Time Rate (<=30 DPD)", f"{on_time:.1f}%")
                    with pi3:
                        late_90 = (paid_df['dpd'] > 90).sum()
                        st.metric("Payments >90 DPD", f"{late_90}")

        # ----------------------------------------------------------------
        # TAB 2: Components
        # ----------------------------------------------------------------
        with tab_components:
            if components:
                comp_df = pd.DataFrame(components)
                total_invoiced = comp_df[comp_df['component_type'] != 'returned']['component_amount'].sum()

                # Detailed Statistics
                st.markdown("#### Detailed Statistics")
                stat1, stat2 = st.columns(2)

                with stat1:
                    st.markdown("**By Component Type:**")
                    comp_summary = comp_df.groupby('component_type').agg({
                        'component_amount': ['sum', 'count', 'mean'],
                        'dpd': ['mean', 'max']
                    }).round(0)
                    comp_summary.columns = ['Total Amount', 'Count', 'Avg Amount', 'Avg DPD', 'Max DPD']
                    comp_summary.index = comp_summary.index.map({
                        'paid': 'Paid', 'returned': 'Returned', 'open': 'Unpaid'
                    })
                    st.dataframe(comp_summary, use_container_width=True)

                with stat2:
                    st.markdown("**By Aging Bucket:**")
                    aging_summary_df = None
                    if 'aging_bucket' in comp_df.columns:
                        aging_summary_df = comp_df.groupby('aging_bucket').agg({
                            'component_amount': ['sum', 'count']
                        }).round(0)
                        aging_summary_df.columns = ['Total Amount', 'Count']
                        if total_invoiced > 0:
                            aging_summary_df['% of Total'] = (aging_summary_df['Total Amount'] / total_invoiced * 100).round(1)
                        st.dataframe(aging_summary_df, use_container_width=True)

                # Invoice Component Breakdown
                st.markdown("#### Invoice Component Breakdown")

                df = comp_df.copy()
                df['component_amount'] = df['component_amount'].astype(float)

                # Sort: chronological, open items at end
                df['resolution_date_sort'] = pd.to_datetime(df['resolution_date'], errors='coerce')
                df['is_open'] = df['component_type'] == 'open'
                df = df.sort_values(['is_open', 'resolution_date_sort']).reset_index(drop=True)
                df = df.drop(['resolution_date_sort', 'is_open'], axis=1)

                df['sequence'] = df.groupby('invoice_number').cumcount() + 1
                df['total_in_group'] = df.groupby('invoice_number')['invoice_number'].transform('count')

                def format_match(row):
                    method = row.get('payment_method', '—')
                    contract = row.get('contract_number', '')
                    if method == 'contract_match' and contract:
                        return f"Contract ({contract})"
                    elif method == 'contract_match':
                        return "Contract"
                    elif method == 'fifo':
                        return "FIFO"
                    return "—"

                def generate_notes(row):
                    ct = row['component_type']
                    dpd = row['dpd']
                    if ct == 'returned':
                        return "Severe return delay" if dpd > 180 else "Return with delay" if dpd > 90 else "Normal return"
                    elif ct == 'paid':
                        if dpd < 0: return "Early payment"
                        if dpd == 0: return "On time"
                        if dpd <= 30: return "Minor delay"
                        if dpd <= 60: return "Late, cured"
                        if dpd <= 90: return "Late payment"
                        return "Very late payment"
                    else:
                        if dpd > 180: return "Major delinquency"
                        if dpd > 90: return "Severe delay"
                        if dpd > 60: return "Late"
                        if dpd > 30: return "Moderate delay"
                        return "Recently due"

                display_comp_df = pd.DataFrame({
                    'Invoice #': pd.to_numeric(df['invoice_number'], errors='coerce'),
                    'Invoice Date': df['invoice_date'].apply(
                        lambda x: x.strftime('%d.%m.%Y') if pd.notnull(x) else '—'),
                    'Part': df.apply(lambda r: f"{int(r['sequence'])}/{int(r['total_in_group'])}", axis=1),
                    'Component': df['component_type'].map({
                        'paid': '✅ Paid', 'returned': '↩️ Returned', 'open': '⏳ Unpaid'}),
                    'Amount': df['component_amount'].apply(lambda x: f"{x:,.0f}"),
                    'Resolution Date': df.apply(
                        lambda r: r['resolution_date'].strftime('%d.%m.%Y')
                        if pd.notnull(r['resolution_date']) and r['component_type'] != 'open'
                        else '—', axis=1),
                    'Due Date': df['due_date'].apply(
                        lambda x: x.strftime('%d.%m.%Y') if pd.notnull(x) else '—'),
                    'DPD': df['dpd'].apply(lambda x: f"{int(x)}"),
                    'Aging Bucket': df.get('aging_bucket', pd.Series(['—'] * len(df))),
                    'Match Method': df.apply(format_match, axis=1),
                    'Notes': df.apply(generate_notes, axis=1),
                })

                # Filters
                with st.expander("Filter Options", expanded=False):
                    fc1, fc2 = st.columns(2)
                    with fc1:
                        comp_filter = st.multiselect(
                            "Component Type",
                            options=['✅ Paid', '↩️ Returned', '⏳ Unpaid'],
                            default=['✅ Paid', '↩️ Returned', '⏳ Unpaid'],
                            key="an_comp_filter")
                    with fc2:
                        bucket_opts = sorted(df['aging_bucket'].dropna().unique().tolist()) if 'aging_bucket' in df.columns else []
                        bucket_filter = st.multiselect(
                            "Aging Bucket", options=bucket_opts, default=bucket_opts,
                            key="an_bucket_filter")

                filtered_comp_df = display_comp_df[
                    display_comp_df['Component'].isin(comp_filter) &
                    display_comp_df['Aging Bucket'].isin(bucket_filter)
                ]

                # Color-coded table
                def highlight_row(row):
                    dpd = int(row['DPD'])
                    comp = row['Component']
                    if '↩️' in comp:
                        return ['background-color: #ffc107'] * len(row) if dpd <= 180 else ['background-color: #dc3545; color: white'] * len(row)
                    elif '⏳' in comp:
                        if dpd > 180: return ['background-color: #dc3545; color: white'] * len(row)
                        if dpd > 90: return ['background-color: #fd7e14; color: white'] * len(row)
                        if dpd > 60: return ['background-color: #ffc107'] * len(row)
                        return ['background-color: #fff3cd'] * len(row)
                    elif '✅' in comp:
                        if dpd > 90: return ['background-color: #f8d7da'] * len(row)
                        if dpd > 60: return ['background-color: #fff3cd'] * len(row)
                        return ['background-color: #d4edda'] * len(row)
                    return [''] * len(row)

                styled = filtered_comp_df.style.apply(highlight_row, axis=1)
                st.dataframe(styled, use_container_width=True, hide_index=True, height=500)

                lc1, lc2 = st.columns(2)
                with lc1:
                    st.markdown("""
                    **Color Legend:**
                    - 🟢 Green: Paid on time (<=60 DPD)
                    - 🟡 Yellow: Moderate delay (61-90 DPD)
                    - 🟠 Orange: Severe unpaid (91-180 DPD)
                    - 🔴 Red: Critical (>180 DPD)
                    """)
                with lc2:
                    st.markdown("""
                    **Match Method:**
                    - Contract: Matched by contract number
                    - FIFO: First-In-First-Out allocation
                    - —: Not applicable
                    """)

                # Export
                st.divider()
                ex1, ex2 = st.columns(2)
                with ex1:
                    csv = filtered_comp_df.to_csv(index=False)
                    st.download_button("Export CSV", csv,
                                       file_name=f"components_{cp_inn}.csv",
                                       mime="text/csv", use_container_width=True)
                with ex2:
                    from io import BytesIO
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        filtered_comp_df.to_excel(writer, sheet_name='Components', index=False)
                        comp_summary.to_excel(writer, sheet_name='Summary')
                        if aging_summary_df is not None:
                            aging_summary_df.to_excel(writer, sheet_name='Aging')
                    st.download_button("Export Excel", output.getvalue(),
                                       file_name=f"analysis_{cp_inn}.xlsx",
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                       use_container_width=True)
            else:
                st.info("No component data available")

        # ----------------------------------------------------------------
        # TAB 3: Aging Dashboard
        # ----------------------------------------------------------------
        with tab_aging:
            if components:
                comp_df = pd.DataFrame(components)
                as_of = date.today()

                daily_series_list = []
                daily_summaries = []

                for inv_num in comp_df['invoice_number'].unique():
                    inv_components = [c for c in components if str(c.get('invoice_number')) == str(inv_num)]
                    if not inv_components:
                        continue

                    first = inv_components[0]
                    inv_date = first.get('invoice_date')
                    inv_total = sum(
                        c['component_amount'] for c in inv_components if c['component_type'] != 'returned'
                    )
                    if inv_total <= 0:
                        continue

                    if isinstance(inv_date, pd.Timestamp):
                        inv_date = inv_date.date()
                    elif isinstance(inv_date, datetime):
                        inv_date = inv_date.date()
                    if inv_date is None:
                        continue

                    events_by_date = {}
                    for comp in inv_components:
                        ct = comp.get('component_type')
                        amt = float(comp.get('component_amount', 0) or 0)
                        cd = comp.get('resolution_date')
                        if pd.isna(cd) or amt == 0:
                            continue
                        if isinstance(cd, pd.Timestamp):
                            cd = cd.date()
                        elif isinstance(cd, datetime):
                            cd = cd.date()
                        if cd < inv_date:
                            continue

                        if ct == 'paid':
                            delta = -amt
                        elif ct == 'returned':
                            delta = amt
                        else:
                            continue
                        events_by_date.setdefault(cd, []).append({'delta': delta, 'amount': amt, 'type': ct})

                    max_event_date = max(events_by_date.keys()) if events_by_date else inv_date
                    net_change = sum(e['delta'] for evts in events_by_date.values() for e in evts)
                    projected = max(inv_total + net_change, 0)
                    end = max_event_date
                    if projected > 0.01:
                        end = max(end, as_of)

                    records = []
                    balance = inv_total
                    cur = inv_date
                    day_n = 1
                    resolved_on = None

                    while cur <= end:
                        opening = balance
                        evts = events_by_date.get(cur, [])
                        change = sum(e['delta'] for e in evts)
                        balance = max(opening + change, 0)
                        records.append({
                            'invoice_number': str(inv_num),
                            'day': day_n, 'date': cur,
                            'opening_balance': opening, 'change': change,
                            'closing_balance': balance
                        })
                        if balance <= 0.01:
                            resolved_on = cur
                            break
                        cur += timedelta(days=1)
                        day_n += 1
                        if day_n > 2000:
                            break

                    if records:
                        daily_series_list.append(pd.DataFrame(records))
                        daily_summaries.append({
                            'invoice': str(inv_num),
                            'total': inv_total,
                            'issued': inv_date,
                            'closed': resolved_on,
                            'days': len(records),
                            'outstanding': balance,
                        })

                if daily_series_list:
                    all_daily = pd.concat(daily_series_list, ignore_index=True)
                    all_daily['date'] = pd.to_datetime(all_daily['date'])
                    agg = all_daily.groupby('date').agg({
                        'opening_balance': 'sum', 'change': 'sum', 'closing_balance': 'sum'
                    }).reset_index().sort_values('date')
                    agg['day'] = range(1, len(agg) + 1)

                    # Outstanding balance chart
                    st.markdown("#### Outstanding Balance Timeline")
                    fig = px.line(agg, x='date', y='closing_balance', markers=True,
                                  labels={'closing_balance': 'Outstanding Balance', 'date': 'Date'})
                    fig.update_layout(showlegend=False, height=350,
                                      margin=dict(l=20, r=20, t=20, b=20))
                    st.plotly_chart(fig, use_container_width=True)

                    # Invoice snapshots
                    if daily_summaries:
                        st.markdown("#### Invoice Snapshots")
                        for s in daily_summaries:
                            closed_str = s['closed'].strftime('%d.%m.%Y') if s['closed'] else "Open"
                            sc1, sc2, sc3, sc4 = st.columns(4)
                            with sc1:
                                st.metric(f"Invoice {s['invoice']}", f"{s['total']:,.0f}")
                            with sc2:
                                st.metric("Outstanding", f"{s['outstanding']:,.0f}")
                            with sc3:
                                st.metric("Days Tracked", s['days'])
                            with sc4:
                                st.metric("Status", "Closed" if s['closed'] else "Open", closed_str)

                    # Daily table
                    st.markdown("#### Daily Aging Table")
                    tbl = agg.copy()
                    tbl['date'] = tbl['date'].dt.strftime('%d.%m.%Y')
                    st.dataframe(
                        tbl[['day', 'date', 'opening_balance', 'change', 'closing_balance']].rename(columns={
                            'day': 'Day', 'date': 'Date', 'opening_balance': 'Opening',
                            'change': 'Change', 'closing_balance': 'Closing'
                        }),
                        use_container_width=True, height=420
                    )
                else:
                    st.info("No daily aging data available for this counterparty")
            else:
                st.info("No component data available")

else:
    st.info("No counterparty data available")
