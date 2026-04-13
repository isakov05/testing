"""
Invoice Analytics — standalone page that works purely with invoice data.
No bank statements or reconciliation required.
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date, timedelta

from auth.db_authenticator import protect_page
from utils.db_operations import load_user_invoices, load_user_bank_transactions
from utils.db_helper import get_db_engine
from utils.risk_engine import RiskEngine, load_risk_config
from utils.risk_queries import get_all_invoices_and_payments, calculate_counterparty_lookback_period

protect_page()

# ═══════════════════════════════════════════════════════════════════════════
# Helper functions
# ═══════════════════════════════════════════════════════════════════════════

def fmt(amount, suffix=" UZS"):
    """Format number with smart abbreviation."""
    if amount is None or pd.isna(amount):
        return "0" + suffix
    try:
        if abs(amount) >= 1e9:
            return f"{amount/1e9:,.1f}B{suffix}"
        if abs(amount) >= 1e6:
            return f"{amount/1e6:,.1f}M{suffix}"
        return f"{amount:,.0f}{suffix}"
    except (ValueError, TypeError):
        return str(amount)


def signed_only(df):
    """Filter to signed invoices only."""
    if df is None or df.empty:
        return pd.DataFrame()
    if 'Status' in df.columns:
        return df[df['Status'].isin(['Подписан', 'Signed', 'signed'])]
    return df


def in_range(df, start, end):
    """Filter by date range."""
    if df is None or df.empty or 'Document Date' not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df['Document Date'] = pd.to_datetime(df['Document Date'], errors='coerce')
    return df[(df['Document Date'] >= pd.Timestamp(start)) & (df['Document Date'] <= pd.Timestamp(end))]


AMT_COL = 'Supply Value (incl. VAT)'

# ═══════════════════════════════════════════════════════════════════════════
# Page header
# ═══════════════════════════════════════════════════════════════════════════
st.title("Invoice Analytics")
st.caption("Analysis based on invoice data")

# Date range
c1, c2, c3 = st.columns([1, 1, 3])
with c1:
    date_start = st.date_input("From", value=datetime.now() - timedelta(days=365))
with c2:
    date_end = st.date_input("To", value=datetime.now())

start_dt = datetime.combine(date_start, datetime.min.time())
end_dt = datetime.combine(date_end, datetime.max.time())

# ═══════════════════════════════════════════════════════════════════════════
# Load data
# ═══════════════════════════════════════════════════════════════════════════
user_id = st.session_state.get('user_id')
if not user_id:
    st.warning("Please log in.")
    st.stop()

uid = str(user_id)

# Try loading from DB first
raw_out = load_user_invoices(uid, 'OUT')
raw_in = load_user_invoices(uid, 'IN')

# Fallback: check session state (upload page stores data there directly)
if raw_out.empty:
    ss_out = st.session_state.get('invoices_out_processed')
    if ss_out is not None and not ss_out.empty:
        raw_out = ss_out
        st.caption("Using invoice data from session (not yet in DB)")

if raw_in.empty:
    ss_in = st.session_state.get('invoices_in_processed')
    if ss_in is not None and not ss_in.empty:
        raw_in = ss_in
        st.caption("Using invoice data from session (not yet in DB)")

if raw_out.empty and raw_in.empty:
    st.info("No invoices found. Upload invoices first.")
    st.stop()

# Check if bank data exists
bank_txns = load_user_bank_transactions(uid)
has_bank_data = not bank_txns.empty

# Prepare filtered datasets
sales = signed_only(raw_out)
purchases = signed_only(raw_in)
sales_period = in_range(sales, start_dt, end_dt)
purchases_period = in_range(purchases, start_dt, end_dt)

# Previous period for comparison
period_days = (end_dt - start_dt).days
prev_start = start_dt - timedelta(days=period_days)
sales_prev = in_range(sales, prev_start, start_dt)
purchases_prev = in_range(purchases, prev_start, start_dt)

# ═══════════════════════════════════════════════════════════════════════════
# KPI calculations
# ═══════════════════════════════════════════════════════════════════════════
def safe_sum(df, col):
    if df is None or df.empty or col not in df.columns:
        return 0.0
    return pd.to_numeric(df[col], errors='coerce').fillna(0).sum()

revenue = safe_sum(sales_period, AMT_COL)
revenue_prev = safe_sum(sales_prev, AMT_COL)
rev_growth = ((revenue - revenue_prev) / revenue_prev * 100) if revenue_prev > 0 else 0

expenses = safe_sum(purchases_period, AMT_COL)
expenses_prev = safe_sum(purchases_prev, AMT_COL)
exp_growth = ((expenses - expenses_prev) / expenses_prev * 100) if expenses_prev > 0 else 0

gross_margin = (revenue - expenses) / revenue * 100 if revenue > 0 else 0
net_position = revenue - expenses

n_buyers = sales_period['Buyer (Tax ID or PINFL)'].nunique() if not sales_period.empty and 'Buyer (Tax ID or PINFL)' in sales_period.columns else 0
n_suppliers = purchases_period['Seller (Tax ID or PINFL)'].nunique() if not purchases_period.empty and 'Seller (Tax ID or PINFL)' in purchases_period.columns else 0
n_invoices_out = len(sales_period)
n_invoices_in = len(purchases_period)
avg_invoice = revenue / n_invoices_out if n_invoices_out > 0 else 0

# Concentration
if not sales_period.empty and 'Buyer (Tax ID or PINFL)' in sales_period.columns:
    sales_period_c = sales_period.copy()
    sales_period_c['_amt'] = pd.to_numeric(sales_period_c[AMT_COL], errors='coerce').fillna(0)
    by_buyer = sales_period_c.groupby('Buyer (Tax ID or PINFL)')['_amt'].sum().sort_values(ascending=False)
    top3_pct = by_buyer.head(3).sum() / by_buyer.sum() * 100 if by_buyer.sum() > 0 else 0
else:
    by_buyer = pd.Series(dtype=float)
    top3_pct = 0

# Overdue estimate (invoices older than 30 days)
if not sales.empty:
    _s = sales.copy()
    _s['Document Date'] = pd.to_datetime(_s['Document Date'], errors='coerce')
    _s['_days'] = (datetime.now() - _s['Document Date']).dt.days
    _s['_amt'] = pd.to_numeric(_s[AMT_COL], errors='coerce').fillna(0)
    overdue_df = _s[_s['_days'] > 30]
    overdue_total = overdue_df['_amt'].sum()
    overdue_count = len(overdue_df)
else:
    overdue_total = 0
    overdue_count = 0

# ═══════════════════════════════════════════════════════════════════════════
# Section 1: KPI Cards
# ═══════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.markdown("### Key Metrics")

k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("Revenue", fmt(revenue),
              delta=f"{rev_growth:+.1f}%" if revenue_prev > 0 else None)
with k2:
    st.metric("Expenses", fmt(expenses),
              delta=f"{exp_growth:+.1f}%" if expenses_prev > 0 else None,
              delta_color="inverse")
with k3:
    st.metric("Gross Margin", f"{gross_margin:.1f}%")
with k4:
    st.metric("Revenue Growth", f"{rev_growth:+.1f}%" if revenue_prev > 0 else "N/A")

k5, k6, k7, k8 = st.columns(4)
with k5:
    st.metric("Active Buyers", n_buyers)
with k6:
    st.metric("Active Suppliers", n_suppliers)
with k7:
    st.metric("Sales Invoices", n_invoices_out)
with k8:
    st.metric("Avg Invoice", fmt(avg_invoice))

k9, k10, k11, k12 = st.columns(4)
with k9:
    st.metric("Concentration (Top 3)", f"{top3_pct:.0f}%",
              help="% of revenue from top 3 clients")
with k10:
    st.metric("Overdue (>30d)", fmt(overdue_total),
              delta=f"{overdue_count} invoices", delta_color="inverse")
with k11:
    st.metric("Net Position", fmt(net_position))
with k12:
    st.metric("Purchase Invoices", n_invoices_in)

# ═══════════════════════════════════════════════════════════════════════════
# Section 2: Trends
# ═══════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.markdown("### Revenue Trends")

if not sales.empty:
    _t = sales.copy()
    _t['Document Date'] = pd.to_datetime(_t['Document Date'], errors='coerce')
    _t['_amt'] = pd.to_numeric(_t[AMT_COL], errors='coerce').fillna(0)
    _t = _t[_t['Document Date'].notna()]
    _t['_month'] = _t['Document Date'].dt.to_period('M').dt.to_timestamp()

    monthly = _t.groupby('_month').agg(
        revenue=('_amt', 'sum'),
        invoices=('_amt', 'count'),
        customers=('Buyer (Tax ID or PINFL)', 'nunique')
    ).reset_index()

    t1, t2 = st.columns(2)

    with t1:
        # Dual-axis: Revenue + Customers
        fig = go.Figure()
        fig.add_trace(go.Bar(x=monthly['_month'], y=monthly['revenue'],
                             name='Revenue', marker_color='#0B5C5F'))
        fig.add_trace(go.Scatter(x=monthly['_month'], y=monthly['customers'],
                                 mode='lines+markers', name='Customers',
                                 line=dict(color='#F59E0B', width=3), yaxis='y2'))
        fig.update_layout(
            title='Monthly Revenue & Customers',
            yaxis=dict(title='Revenue', side='left'),
            yaxis2=dict(title='Customers', side='right', overlaying='y'),
            height=380, margin=dict(l=20, r=20, t=40, b=20),
            legend=dict(orientation='h', y=1.12)
        )
        st.plotly_chart(fig, use_container_width=True)

    with t2:
        # Revenue + Invoice count
        fig = go.Figure()
        fig.add_trace(go.Bar(x=monthly['_month'], y=monthly['invoices'],
                             name='Invoice Count', marker_color='#3B82F6'))
        fig.add_trace(go.Scatter(x=monthly['_month'], y=monthly['revenue'],
                                 mode='lines+markers', name='Revenue',
                                 line=dict(color='#10B981', width=3), yaxis='y2'))
        fig.update_layout(
            title='Invoice Volume & Revenue',
            yaxis=dict(title='Invoice Count', side='left'),
            yaxis2=dict(title='Revenue', side='right', overlaying='y'),
            height=380, margin=dict(l=20, r=20, t=40, b=20),
            legend=dict(orientation='h', y=1.12)
        )
        st.plotly_chart(fig, use_container_width=True)

    # Expenses trend
    if not purchases.empty:
        _p = purchases.copy()
        _p['Document Date'] = pd.to_datetime(_p['Document Date'], errors='coerce')
        _p['_amt'] = pd.to_numeric(_p[AMT_COL], errors='coerce').fillna(0)
        _p = _p[_p['Document Date'].notna()]
        _p['_month'] = _p['Document Date'].dt.to_period('M').dt.to_timestamp()
        monthly_exp = _p.groupby('_month')['_amt'].sum().reset_index()
        monthly_exp.columns = ['_month', 'expenses']

        combined = monthly.merge(monthly_exp, on='_month', how='outer').fillna(0).sort_values('_month')
        combined['profit'] = combined['revenue'] - combined['expenses']

        fig = go.Figure()
        fig.add_trace(go.Bar(x=combined['_month'], y=combined['revenue'],
                             name='Revenue', marker_color='#10B981'))
        fig.add_trace(go.Bar(x=combined['_month'], y=combined['expenses'],
                             name='Expenses', marker_color='#EF4444'))
        fig.add_trace(go.Scatter(x=combined['_month'], y=combined['profit'],
                                 mode='lines+markers', name='Profit',
                                 line=dict(color='#0B5C5F', width=3)))
        fig.update_layout(
            title='Revenue vs Expenses vs Profit',
            barmode='group', height=380,
            margin=dict(l=20, r=20, t=40, b=20),
            legend=dict(orientation='h', y=1.12)
        )
        st.plotly_chart(fig, use_container_width=True)

else:
    st.info("No outgoing invoices to show trends")

# ═══════════════════════════════════════════════════════════════════════════
# Section 3: Counterparty Analysis
# ═══════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.markdown("### Counterparty Analysis")

cp1, cp2 = st.columns(2)

with cp1:
    # Top buyers
    if not sales_period.empty and 'Buyer (Name)' in sales_period.columns:
        st.markdown("#### Top Buyers")
        _b = sales_period.copy()
        _b['_amt'] = pd.to_numeric(_b[AMT_COL], errors='coerce').fillna(0)
        top_b = _b.groupby('Buyer (Name)')['_amt'].sum().sort_values(ascending=False).head(10)
        fig = px.bar(x=top_b.values, y=top_b.index, orientation='h',
                     labels={'x': 'Revenue', 'y': ''})
        fig.update_layout(yaxis={'categoryorder': 'total ascending'},
                          height=400, margin=dict(l=20, r=20, t=10, b=20), showlegend=False)
        fig.update_traces(marker_color='#0B5C5F')
        st.plotly_chart(fig, use_container_width=True)

with cp2:
    # Top suppliers
    if not purchases_period.empty and 'Seller (Name)' in purchases_period.columns:
        st.markdown("#### Top Suppliers")
        _s = purchases_period.copy()
        _s['_amt'] = pd.to_numeric(_s[AMT_COL], errors='coerce').fillna(0)
        top_s = _s.groupby('Seller (Name)')['_amt'].sum().sort_values(ascending=False).head(10)
        fig = px.bar(x=top_s.values, y=top_s.index, orientation='h',
                     labels={'x': 'Spend', 'y': ''})
        fig.update_layout(yaxis={'categoryorder': 'total ascending'},
                          height=400, margin=dict(l=20, r=20, t=10, b=20), showlegend=False)
        fig.update_traces(marker_color='#F59E0B')
        st.plotly_chart(fig, use_container_width=True)

# Pareto + Distribution
pa1, pa2 = st.columns(2)

with pa1:
    if len(by_buyer) > 0:
        st.markdown("#### Customer Concentration (Pareto)")
        cum_pct = (by_buyer / by_buyer.sum() * 100).cumsum()
        n = min(20, len(by_buyer))
        fig = go.Figure()
        fig.add_trace(go.Bar(x=list(range(n)), y=by_buyer.values[:n],
                             name='Sales', marker_color='#3B82F6'))
        fig.add_trace(go.Scatter(x=list(range(n)), y=cum_pct.values[:n],
                                 mode='lines+markers', name='Cumulative %',
                                 line=dict(color='#EF4444', width=2), yaxis='y2'))
        fig.update_layout(
            xaxis=dict(title='Customer Rank', tickvals=list(range(n)),
                       ticktext=[f"C{i+1}" for i in range(n)]),
            yaxis=dict(title='Sales', side='left'),
            yaxis2=dict(title='Cumulative %', side='right', overlaying='y', range=[0, 105]),
            height=400, margin=dict(l=20, r=20, t=10, b=20),
            legend=dict(orientation='h', y=1.1)
        )
        st.plotly_chart(fig, use_container_width=True)

with pa2:
    if not sales_period.empty:
        st.markdown("#### Invoice Size Distribution")
        _d = sales_period.copy()
        _d['_amt'] = pd.to_numeric(_d[AMT_COL], errors='coerce').fillna(0)
        fig = px.histogram(_d, x='_amt', nbins=30,
                           labels={'_amt': 'Invoice Amount'},
                           color_discrete_sequence=['#0B5C5F'])
        fig.update_layout(height=400, margin=dict(l=20, r=20, t=10, b=20), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

# Day of week + Performance
dw1, dw2 = st.columns(2)

with dw1:
    if not sales_period.empty:
        st.markdown("#### Invoicing by Day of Week")
        _w = sales_period.copy()
        _w['Document Date'] = pd.to_datetime(_w['Document Date'], errors='coerce')
        _w = _w[_w['Document Date'].notna()]
        _w['_dow'] = _w['Document Date'].dt.day_name()
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        freq = _w.groupby('_dow').size().reindex(day_order, fill_value=0)
        fig = px.bar(x=freq.index, y=freq.values, labels={'x': '', 'y': 'Invoices'},
                     color=freq.values, color_continuous_scale='Blues')
        fig.update_layout(height=350, margin=dict(l=20, r=20, t=10, b=20), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

with dw2:
    st.markdown("#### Performance Scorecard")
    scores = pd.DataFrame({
        'Metric': ['Revenue', 'Diversity', 'Volume', 'Concentration', 'Growth'],
        'Score': [
            85 if avg_invoice > 1e6 else 70 if avg_invoice > 1e5 else 55,
            85 if n_buyers > 50 else 70 if n_buyers > 20 else 55,
            90 if n_invoices_out > 100 else 75 if n_invoices_out > 30 else 55,
            85 if top3_pct < 50 else 60 if top3_pct < 70 else 40,
            85 if rev_growth > 5 else 65 if rev_growth >= 0 else 40,
        ]
    })
    fig = px.bar(scores, x='Metric', y='Score', color='Score',
                 color_continuous_scale='RdYlGn', range_color=[0, 100], text='Score')
    fig.update_traces(textposition='outside')
    fig.update_layout(height=350, margin=dict(l=20, r=20, t=10, b=20),
                      showlegend=False, yaxis_range=[0, 105])
    st.plotly_chart(fig, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════
# Section 4: Aging
# ═══════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.markdown("### Invoice Aging")

ag1, ag2 = st.columns(2)

buckets = [('0-30', 0, 30), ('31-60', 31, 60), ('61-90', 61, 90),
           ('91-180', 91, 180), ('180+', 181, 999999)]

with ag1:
    st.markdown("#### Sales Invoice Aging")
    if not sales.empty:
        _a = sales.copy()
        _a['Document Date'] = pd.to_datetime(_a['Document Date'], errors='coerce')
        _a['_days'] = (datetime.now() - _a['Document Date']).dt.days
        _a['_amt'] = pd.to_numeric(_a[AMT_COL], errors='coerce').fillna(0)
        rows = [{'bucket': n, 'amount': _a[(_a['_days'] >= lo) & (_a['_days'] <= hi)]['_amt'].sum(),
                 'count': len(_a[(_a['_days'] >= lo) & (_a['_days'] <= hi)])}
                for n, lo, hi in buckets]
        aging_df = pd.DataFrame(rows)
        if aging_df['amount'].sum() > 0:
            fig = px.bar(aging_df, x='bucket', y='amount', text='count',
                         labels={'bucket': '', 'amount': 'Amount'})
            fig.update_traces(marker_color='#3B82F6', textposition='outside')
            fig.update_layout(height=350, margin=dict(l=20, r=20, t=10, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data")
    else:
        st.info("No sales invoices")

with ag2:
    st.markdown("#### Purchase Invoice Aging")
    if not purchases.empty:
        _a = purchases.copy()
        _a['Document Date'] = pd.to_datetime(_a['Document Date'], errors='coerce')
        _a['_days'] = (datetime.now() - _a['Document Date']).dt.days
        _a['_amt'] = pd.to_numeric(_a[AMT_COL], errors='coerce').fillna(0)
        rows = [{'bucket': n, 'amount': _a[(_a['_days'] >= lo) & (_a['_days'] <= hi)]['_amt'].sum(),
                 'count': len(_a[(_a['_days'] >= lo) & (_a['_days'] <= hi)])}
                for n, lo, hi in buckets]
        aging_df = pd.DataFrame(rows)
        if aging_df['amount'].sum() > 0:
            fig = px.bar(aging_df, x='bucket', y='amount', text='count',
                         labels={'bucket': '', 'amount': 'Amount'})
            fig.update_traces(marker_color='#F59E0B', textposition='outside')
            fig.update_layout(height=350, margin=dict(l=20, r=20, t=10, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data")
    else:
        st.info("No purchase invoices")

# ═══════════════════════════════════════════════════════════════════════════
# Section 5: Product Explorer
# ═══════════════════════════════════════════════════════════════════════════
notes_col = 'Примечание к товару (работе, услуге)'
if not sales_period.empty and notes_col in sales_period.columns:
    _pn = sales_period[sales_period[notes_col].notna() & (sales_period[notes_col].str.strip() != '')].copy()
    if not _pn.empty:
        st.markdown("---")
        st.markdown("### Product Explorer")
        _pn['_amt'] = pd.to_numeric(_pn[AMT_COL], errors='coerce').fillna(0)

        pe1, pe2, pe3 = st.columns(3)
        with pe1:
            st.metric("Products", f"{_pn[notes_col].nunique():,}")
        with pe2:
            coverage = len(_pn) / len(sales_period) * 100
            st.metric("Coverage", f"{coverage:.0f}%")
        with pe3:
            st.metric("Product Revenue", fmt(_pn['_amt'].sum()))

        pr1, pr2 = st.columns(2)
        with pr1:
            st.markdown("**By Revenue**")
            top_rev = _pn.groupby(notes_col)['_amt'].sum().sort_values(ascending=False).head(15)
            fig = px.bar(x=top_rev.values,
                         y=[n[:50] + '...' if len(n) > 50 else n for n in top_rev.index],
                         orientation='h', labels={'x': 'Revenue', 'y': ''},
                         color=top_rev.values, color_continuous_scale='Oranges')
            fig.update_layout(height=500, showlegend=False, margin=dict(l=20, r=20, t=10, b=20))
            st.plotly_chart(fig, use_container_width=True)

        with pr2:
            st.markdown("**By Frequency**")
            top_freq = _pn[notes_col].value_counts().head(15)
            fig = px.bar(x=top_freq.values,
                         y=[n[:50] + '...' if len(n) > 50 else n for n in top_freq.index],
                         orientation='h', labels={'x': 'Count', 'y': ''},
                         color=top_freq.values, color_continuous_scale='Viridis')
            fig.update_layout(height=500, showlegend=False, margin=dict(l=20, r=20, t=10, b=20))
            st.plotly_chart(fig, use_container_width=True)

        # Treemap
        top20 = _pn.groupby(notes_col)['_amt'].sum().sort_values(ascending=False).head(20)
        if len(top20) > 1:
            tree = pd.DataFrame({
                'product': [p[:50] + '...' if len(p) > 50 else p for p in top20.index],
                'group': 'Sales', 'amount': top20.values
            })
            fig = px.treemap(tree, path=['group', 'product'], values='amount',
                             color='amount', color_continuous_scale='Viridis', height=450)
            st.plotly_chart(fig, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════
# Section 6: Counterparty Table + Risk Analysis
# ═══════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.markdown("### Counterparty Overview")

# Build counterparty table
cp_rows = []
if not sales_period.empty and 'Buyer (Name)' in sales_period.columns:
    _b = sales_period.copy()
    _b['_amt'] = pd.to_numeric(_b[AMT_COL], errors='coerce').fillna(0)
    buyer_agg = _b.groupby(['Buyer (Name)', 'Buyer (Tax ID or PINFL)']).agg(
        volume=('_amt', 'sum'), count=('_amt', 'count')).reset_index()
    buyer_agg.columns = ['name', 'inn', 'volume', 'invoices']
    buyer_agg['type'] = 'Buyer'
    cp_rows.append(buyer_agg)

if not purchases_period.empty and 'Seller (Name)' in purchases_period.columns:
    _s = purchases_period.copy()
    _s['_amt'] = pd.to_numeric(_s[AMT_COL], errors='coerce').fillna(0)
    seller_agg = _s.groupby(['Seller (Name)', 'Seller (Tax ID or PINFL)']).agg(
        volume=('_amt', 'sum'), count=('_amt', 'count')).reset_index()
    seller_agg.columns = ['name', 'inn', 'volume', 'invoices']
    seller_agg['type'] = 'Supplier'
    cp_rows.append(seller_agg)

if cp_rows:
    cp_df = pd.concat(cp_rows, ignore_index=True)
    cp_df = cp_df.groupby(['name', 'inn']).agg({
        'type': lambda x: 'Both' if len(set(x)) > 1 else x.iloc[0],
        'volume': 'sum', 'invoices': 'sum'
    }).reset_index().sort_values('volume', ascending=False)

    search = st.text_input("Search counterparty", placeholder="Name or INN...", key="cp_search")
    if search:
        mask = (cp_df['name'].str.contains(search, case=False, na=False) |
                cp_df['inn'].astype(str).str.contains(search, na=False))
        display_cp = cp_df[mask]
    else:
        display_cp = cp_df

    st.dataframe(
        display_cp.rename(columns={'name': 'Counterparty', 'inn': 'INN',
                                    'type': 'Type', 'volume': 'Volume', 'invoices': 'Invoices'}),
        use_container_width=True, hide_index=True,
        column_config={'Volume': st.column_config.NumberColumn(format="%.0f")}
    )
    st.caption(f"{len(display_cp)} counterparties")

    # --- Risk Analysis ---
    if not display_cp.empty:
        if has_bank_data:
            options = [f"{r['name']}  ({r['inn']})" for _, r in display_cp.iterrows()]
            inn_map = {f"{r['name']}  ({r['inn']})": r['inn'] for _, r in display_cp.iterrows()}
            name_map = {f"{r['name']}  ({r['inn']})": r['name'] for _, r in display_cp.iterrows()}

            selected = st.selectbox("Select counterparty to analyze", [""] + options, key="risk_cp")

            if selected and st.button("Analyze", type="primary", key="risk_btn"):
                sel_inn = str(inn_map[selected]).replace('.0', '').strip()
                sel_name = name_map[selected]

                with st.spinner(f"Analyzing {sel_name}..."):
                    lookback = calculate_counterparty_lookback_period(uid, sel_inn, 'OUT')
                    inv_df, pay_df = get_all_invoices_and_payments(uid, 'OUT', lookback)

                    if not inv_df.empty and 'buyer_inn' in inv_df.columns:
                        inv_df['buyer_inn'] = inv_df['buyer_inn'].astype(str).str.replace('.0', '', regex=False).str.strip()
                    if not pay_df.empty and 'counterparty_inn' in pay_df.columns:
                        pay_df['counterparty_inn'] = pay_df['counterparty_inn'].astype(str).str.replace('.0', '', regex=False).str.strip()

                    config = load_risk_config()
                    engine = RiskEngine(config, uid)
                    components = engine.reconstruct_invoice_components(inv_df, pay_df, 'OUT')
                    risk = engine.calculate_counterparty_risk(sel_inn, components)

                    cp_components = [c for c in components
                                     if str(c.get('counterparty_inn', '')).replace('.0', '').strip() == sel_inn]

                st.session_state['ia_risk'] = risk
                st.session_state['ia_risk_name'] = sel_name
                st.session_state['ia_risk_inn'] = sel_inn
                st.session_state['ia_risk_components'] = cp_components
        else:
            # No bank data — show blurred preview
            st.markdown("---")
            st.markdown("### Risk Analysis")

            # Blurred preview with overlay
            st.markdown("""
            <div style="position:relative;">
                <div style="filter:blur(6px); pointer-events:none; opacity:0.5;">
                    <div style="display:flex; gap:16px; margin-bottom:16px;">
                        <div style="flex:1; padding:16px; background:#f0fdf4; border-radius:8px; border:2px solid #10B981; text-align:center;">
                            <div style="font-size:36px; font-weight:bold; color:#10B981;">B</div>
                            <div style="font-size:12px; color:#6B7280;">Risk Rating</div>
                        </div>
                        <div style="flex:1; padding:16px; background:#f9fafb; border-radius:8px;">
                            <div style="font-size:12px; color:#6B7280;">PD (Prob. of Default)</div>
                            <div style="font-size:24px; font-weight:600;">3.50%</div>
                        </div>
                        <div style="flex:1; padding:16px; background:#f9fafb; border-radius:8px;">
                            <div style="font-size:12px; color:#6B7280;">LGD (Loss Given Default)</div>
                            <div style="font-size:24px; font-weight:600;">45.00%</div>
                        </div>
                        <div style="flex:1; padding:16px; background:#f9fafb; border-radius:8px;">
                            <div style="font-size:12px; color:#6B7280;">EAD (Exposure)</div>
                            <div style="font-size:24px; font-weight:600;">850.2M UZS</div>
                        </div>
                        <div style="flex:1; padding:16px; background:#f9fafb; border-radius:8px;">
                            <div style="font-size:12px; color:#6B7280;">Expected Loss</div>
                            <div style="font-size:24px; font-weight:600;">13.4M UZS</div>
                        </div>
                    </div>
                    <div style="display:flex; gap:16px; margin-bottom:16px;">
                        <div style="flex:1; padding:16px; background:#f9fafb; border-radius:8px;">
                            <div style="font-size:12px; color:#6B7280;">Credit Limit</div>
                            <div style="font-size:24px; font-weight:600;">120.5M UZS</div>
                        </div>
                        <div style="flex:1; padding:16px; background:#f9fafb; border-radius:8px;">
                            <div style="font-size:12px; color:#6B7280;">Avg Days Past Due</div>
                            <div style="font-size:24px; font-weight:600;">18 days</div>
                        </div>
                        <div style="flex:1; padding:16px; background:#f9fafb; border-radius:8px;">
                            <div style="font-size:12px; color:#6B7280;">Late Payment Rate</div>
                            <div style="font-size:24px; font-weight:600;">12%</div>
                        </div>
                    </div>
                    <div style="display:flex; gap:16px;">
                        <div style="flex:1; height:200px; background:#f0fdf4; border-radius:8px;"></div>
                        <div style="flex:1; height:200px; background:#eff6ff; border-radius:8px;"></div>
                    </div>
                </div>
                <div style="position:absolute; top:50%; left:50%; transform:translate(-50%,-50%);
                            background:white; padding:24px 40px; border-radius:12px;
                            box-shadow:0 4px 24px rgba(0,0,0,0.15); text-align:center; z-index:10;">
                    <div style="font-size:32px; margin-bottom:8px;">🔒</div>
                    <div style="font-size:18px; font-weight:600; margin-bottom:8px;">Upload Bank Statements to Unlock</div>
                    <div style="font-size:14px; color:#6B7280; margin-bottom:16px;">
                        Risk ratings, credit limits, payment behavior,<br>and aging dashboard
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            st.page_link("pages/file_upload.py", label="Upload Bank Statements", icon="📂")

            # Clear any stale risk results
            if 'ia_risk' in st.session_state:
                del st.session_state['ia_risk']

    # Display risk results (only if bank data was available when analysis ran)
    if has_bank_data and st.session_state.get('ia_risk'):
        risk = st.session_state['ia_risk']
        r_name = st.session_state.get('ia_risk_name', '')
        r_inn = st.session_state.get('ia_risk_inn', '')
        r_comps = st.session_state.get('ia_risk_components', [])

        st.markdown("---")
        st.markdown(f"### Risk Analysis: {r_name}")
        st.caption(f"INN: {r_inn}")

        tab_ov, tab_comp, tab_aging = st.tabs(["Overview", "Components", "Aging"])

        # --- Overview Tab ---
        with tab_ov:
            rating = risk.get('rating', 'N/A')
            clr = {'A': '#10B981', 'B': '#3B82F6', 'C': '#F59E0B', 'D': '#EF4444'}.get(rating, '#6B7280')

            o1, o2, o3, o4, o5 = st.columns(5)
            with o1:
                st.markdown(f"""
                <div style="text-align:center; padding:12px; background:{clr}15; border-radius:8px; border:2px solid {clr};">
                    <div style="font-size:36px; font-weight:bold; color:{clr};">{rating}</div>
                    <div style="font-size:12px; color:#6B7280;">Rating</div>
                </div>""", unsafe_allow_html=True)
            with o2:
                st.metric("PD", f"{risk.get('pd', 0):.2%}")
            with o3:
                st.metric("LGD", f"{risk.get('lgd', 0):.2%}")
            with o4:
                st.metric("EAD", fmt(risk.get('ead_current', 0)))
            with o5:
                st.metric("Expected Loss", fmt(risk.get('expected_loss', 0)))

            ol1, ol2, ol3 = st.columns(3)
            with ol1:
                st.metric("Credit Limit", fmt(risk.get('recommended_limit', 0)))
            with ol2:
                feats = risk.get('behavioral_features', {})
                st.metric("Avg DPD", f"{feats.get('weighted_avg_dpd', 0):.0f} days")
            with ol3:
                st.metric("Late Rate", f"{feats.get('late_payment_rate', 0):.0%}")

            just = risk.get('justification', {})
            if just:
                with st.expander("Justification"):
                    for k, v in just.items():
                        st.markdown(f"**{k.upper()}:** {v}")

            if r_comps:
                cdf = pd.DataFrame(r_comps)
                bh1, bh2 = st.columns(2)
                with bh1:
                    ts = cdf.groupby('component_type')['component_amount'].sum().reset_index()
                    ts.columns = ['type', 'amount']
                    fig = px.pie(ts, values='amount', names='type', title='Status Breakdown',
                                 color='type', color_discrete_map={'paid': '#10B981', 'open': '#F59E0B', 'returned': '#EF4444'})
                    fig.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=20))
                    st.plotly_chart(fig, use_container_width=True)
                with bh2:
                    paid = cdf[cdf['component_type'] == 'paid']
                    if not paid.empty:
                        fig = px.histogram(paid, x='dpd', nbins=20, title='DPD Distribution (Paid)',
                                           labels={'dpd': 'Days Past Due'})
                        fig.update_traces(marker_color='#3B82F6')
                        fig.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=20))
                        st.plotly_chart(fig, use_container_width=True)

                # Financial summary
                total_inv = cdf[cdf['component_type'] != 'returned']['component_amount'].sum()
                if total_inv > 0:
                    f1, f2, f3, f4 = st.columns(4)
                    paid_t = cdf[cdf['component_type'] == 'paid']['component_amount'].sum()
                    ret_t = cdf[cdf['component_type'] == 'returned']['component_amount'].sum()
                    unpaid_t = cdf[cdf['component_type'] == 'open']['component_amount'].sum()
                    with f1:
                        st.metric("Invoiced", f"{total_inv:,.0f}")
                    with f2:
                        st.metric("Paid", f"{paid_t:,.0f}", delta=f"{paid_t/total_inv*100:.0f}%")
                    with f3:
                        st.metric("Returned", f"{ret_t:,.0f}", delta=f"{ret_t/total_inv*100:.0f}%", delta_color="inverse")
                    with f4:
                        st.metric("Unpaid", f"{unpaid_t:,.0f}", delta=f"{unpaid_t/total_inv*100:.0f}%", delta_color="inverse")

        # --- Components Tab ---
        with tab_comp:
            if r_comps:
                cdf = pd.DataFrame(r_comps)
                cdf['component_amount'] = cdf['component_amount'].astype(float)

                cdf['_sort'] = pd.to_datetime(cdf['resolution_date'], errors='coerce')
                cdf['_open'] = cdf['component_type'] == 'open'
                cdf = cdf.sort_values(['_open', '_sort']).reset_index(drop=True)
                cdf = cdf.drop(['_sort', '_open'], axis=1)

                cdf['seq'] = cdf.groupby('invoice_number').cumcount() + 1
                cdf['grp'] = cdf.groupby('invoice_number')['invoice_number'].transform('count')

                tbl = pd.DataFrame({
                    'Invoice': pd.to_numeric(cdf['invoice_number'], errors='coerce'),
                    'Date': cdf['invoice_date'].apply(lambda x: x.strftime('%d.%m.%Y') if pd.notnull(x) else '—'),
                    'Part': cdf.apply(lambda r: f"{int(r['seq'])}/{int(r['grp'])}", axis=1),
                    'Status': cdf['component_type'].map({'paid': '✅ Paid', 'returned': '↩️ Returned', 'open': '⏳ Unpaid'}),
                    'Amount': cdf['component_amount'].apply(lambda x: f"{x:,.0f}"),
                    'DPD': cdf['dpd'].apply(lambda x: f"{int(x)}"),
                    'Bucket': cdf.get('aging_bucket', '—'),
                    'Match': cdf.apply(lambda r: f"Contract ({r.get('contract_number', '')})" if r.get('payment_method') == 'contract_match' else 'FIFO' if r.get('payment_method') == 'fifo' else '—', axis=1),
                })

                def color_row(row):
                    d = int(row['DPD'])
                    s = row['Status']
                    if '↩️' in s:
                        return ['background-color: #ffc107'] * len(row) if d <= 180 else ['background-color: #dc3545; color: white'] * len(row)
                    elif '⏳' in s:
                        if d > 180: return ['background-color: #dc3545; color: white'] * len(row)
                        if d > 90: return ['background-color: #fd7e14; color: white'] * len(row)
                        return ['background-color: #fff3cd'] * len(row)
                    elif '✅' in s:
                        if d > 90: return ['background-color: #f8d7da'] * len(row)
                        return ['background-color: #d4edda'] * len(row)
                    return [''] * len(row)

                st.dataframe(tbl.style.apply(color_row, axis=1),
                             use_container_width=True, hide_index=True, height=500)

                csv = tbl.to_csv(index=False)
                st.download_button("Export CSV", csv, file_name=f"components_{r_inn}.csv",
                                   mime="text/csv")
            else:
                st.info("No components")

        # --- Aging Tab ---
        with tab_aging:
            if r_comps:
                cdf = pd.DataFrame(r_comps)
                series_list = []
                summaries = []

                for inv_num in cdf['invoice_number'].unique():
                    ic = [c for c in r_comps if str(c.get('invoice_number')) == str(inv_num)]
                    first = ic[0]
                    idate = first.get('invoice_date')
                    itotal = sum(c['component_amount'] for c in ic if c['component_type'] != 'returned')
                    if itotal <= 0:
                        continue
                    if isinstance(idate, pd.Timestamp):
                        idate = idate.date()
                    elif isinstance(idate, datetime):
                        idate = idate.date()
                    if idate is None:
                        continue

                    events = {}
                    for c in ic:
                        ct, amt = c.get('component_type'), float(c.get('component_amount', 0) or 0)
                        cd = c.get('resolution_date')
                        if pd.isna(cd) or amt == 0:
                            continue
                        if isinstance(cd, pd.Timestamp): cd = cd.date()
                        elif isinstance(cd, datetime): cd = cd.date()
                        if cd < idate:
                            continue
                        delta = -amt if ct == 'paid' else (amt if ct == 'returned' else 0)
                        if delta != 0:
                            events.setdefault(cd, []).append(delta)

                    end_d = max(events.keys()) if events else idate
                    net = sum(d for ds in events.values() for d in ds)
                    if max(itotal + net, 0) > 0.01:
                        end_d = max(end_d, date.today())

                    recs, bal, cur_d, dn, resolved = [], itotal, idate, 1, None
                    while cur_d <= end_d and dn <= 2000:
                        op = bal
                        ch = sum(events.get(cur_d, []))
                        bal = max(op + ch, 0)
                        recs.append({'inv': str(inv_num), 'day': dn, 'date': cur_d,
                                     'opening': op, 'change': ch, 'closing': bal})
                        if bal <= 0.01:
                            resolved = cur_d
                            break
                        cur_d += timedelta(days=1)
                        dn += 1

                    if recs:
                        series_list.append(pd.DataFrame(recs))
                        summaries.append({'inv': str(inv_num), 'total': itotal,
                                          'outstanding': bal, 'days': len(recs),
                                          'closed': resolved})

                if series_list:
                    ad = pd.concat(series_list, ignore_index=True)
                    ad['date'] = pd.to_datetime(ad['date'])
                    agg = ad.groupby('date').agg(
                        opening=('opening', 'sum'), change=('change', 'sum'), closing=('closing', 'sum')
                    ).reset_index().sort_values('date')
                    agg['day'] = range(1, len(agg) + 1)

                    fig = px.line(agg, x='date', y='closing', markers=True,
                                  labels={'closing': 'Outstanding', 'date': ''})
                    fig.update_layout(height=350, margin=dict(l=20, r=20, t=20, b=20), showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)

                    for s in summaries:
                        sc1, sc2, sc3, sc4 = st.columns(4)
                        with sc1: st.metric(f"Invoice {s['inv']}", f"{s['total']:,.0f}")
                        with sc2: st.metric("Outstanding", f"{s['outstanding']:,.0f}")
                        with sc3: st.metric("Days", s['days'])
                        with sc4: st.metric("Status", "Closed" if s['closed'] else "Open")

                    with st.expander("Daily Table"):
                        _t = agg.copy()
                        _t['date'] = _t['date'].dt.strftime('%d.%m.%Y')
                        st.dataframe(_t[['day', 'date', 'opening', 'change', 'closing']].rename(
                            columns={'day': 'Day', 'date': 'Date', 'opening': 'Opening',
                                     'change': 'Change', 'closing': 'Closing'}
                        ), use_container_width=True, height=400)
                else:
                    st.info("No aging data")
            else:
                st.info("No components")
else:
    st.info("No counterparty data available")
