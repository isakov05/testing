"""
Business Overview — concise summary page.
Sections: Key Metrics | Revenue Trends (merged) | Top Buyers & Suppliers | Product Analytics
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

from auth.db_authenticator import protect_page
from utils.session_loader import (
    load_integration_invoices_by_tin,
    load_integration_items_by_tin,
    get_user_company_tin,
    get_company_name,
    load_user_bank_transactions,
)

st.set_page_config(page_title="Business Overview", page_icon="📊", layout="wide")
protect_page()

# ── Helpers ────────────────────────────────────────────────────────────────

def fmt(amount, suffix=" UZS"):
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
    if df is None or df.empty:
        return pd.DataFrame()
    if 'Status' in df.columns:
        return df[df['Status'].isin(['Подписан', 'Signed', 'signed'])]
    return df


def in_range(df, start, end):
    if df is None or df.empty or 'Document Date' not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df['Document Date'] = pd.to_datetime(df['Document Date'], errors='coerce')
    return df[(df['Document Date'] >= pd.Timestamp(start)) & (df['Document Date'] <= pd.Timestamp(end))]


def safe_sum(df, col):
    if df is None or df.empty or col not in df.columns:
        return 0.0
    return pd.to_numeric(df[col], errors='coerce').fillna(0).sum()


AMT = 'Supply Value (incl. VAT)'

# ── Header ─────────────────────────────────────────────────────────────────

st.title("Business Overview")
st.caption("Key metrics, trends, and product insights at a glance")

c1, c2, _ = st.columns([1, 1, 3])
with c1:
    date_start = st.date_input("From", value=datetime.now() - timedelta(days=365))
with c2:
    date_end = st.date_input("To", value=datetime.now())

start_dt = datetime.combine(date_start, datetime.min.time())
end_dt = datetime.combine(date_end, datetime.max.time())

# ── Auth / data ─────────────────────────────────────────────────────────────

user_id = st.session_state.get('user_id')
if not user_id:
    st.warning("Please log in.")
    st.stop()

company_tin = get_user_company_tin(str(user_id))
if not company_tin:
    st.info("Upload invoice files on the **File Upload** page to get started.")
    st.stop()

st.caption(f"**{get_company_name(company_tin)}** (INN: {company_tin})")

raw_out = load_integration_invoices_by_tin(company_tin, 'OUT')
raw_in  = load_integration_invoices_by_tin(company_tin, 'IN')

if raw_out.empty and raw_in.empty:
    st.info("No invoices found. Upload invoice files on the **File Upload** page.")
    st.stop()

sales      = signed_only(raw_out)
purchases  = signed_only(raw_in)
sales_p    = in_range(sales, start_dt, end_dt)
purchases_p = in_range(purchases, start_dt, end_dt)

period_days  = max((end_dt - start_dt).days, 1)
prev_start   = start_dt - timedelta(days=period_days)
sales_prev   = in_range(sales, prev_start, start_dt)
purchases_prev = in_range(purchases, prev_start, start_dt)

# ── KPI calculations ────────────────────────────────────────────────────────

revenue      = safe_sum(sales_p, AMT)
revenue_prev = safe_sum(sales_prev, AMT)
rev_growth   = (revenue - revenue_prev) / revenue_prev * 100 if revenue_prev > 0 else 0

expenses      = safe_sum(purchases_p, AMT)
expenses_prev = safe_sum(purchases_prev, AMT)
exp_growth    = (expenses - expenses_prev) / expenses_prev * 100 if expenses_prev > 0 else 0

gross_margin  = (revenue - expenses) / revenue * 100 if revenue > 0 else 0
net_position  = revenue - expenses

n_buyers      = sales_p['Buyer (Tax ID or PINFL)'].nunique()     if not sales_p.empty and 'Buyer (Tax ID or PINFL)' in sales_p.columns else 0
n_suppliers   = purchases_p['Seller (Tax ID or PINFL)'].nunique() if not purchases_p.empty and 'Seller (Tax ID or PINFL)' in purchases_p.columns else 0
n_out         = len(sales_p)
n_in          = len(purchases_p)
avg_invoice   = revenue / n_out if n_out > 0 else 0

if not sales_p.empty and 'Buyer (Tax ID or PINFL)' in sales_p.columns:
    _sp = sales_p.copy()
    _sp['_amt'] = pd.to_numeric(_sp[AMT], errors='coerce').fillna(0)
    by_buyer = _sp.groupby('Buyer (Tax ID or PINFL)')['_amt'].sum().sort_values(ascending=False)
    top3_pct = by_buyer.head(3).sum() / by_buyer.sum() * 100 if by_buyer.sum() > 0 else 0
else:
    by_buyer = pd.Series(dtype=float)
    top3_pct = 0

if not sales.empty:
    _ov = sales.copy()
    _ov['Document Date'] = pd.to_datetime(_ov['Document Date'], errors='coerce')
    _ov['_days'] = (datetime.now() - _ov['Document Date']).dt.days
    _ov['_amt'] = pd.to_numeric(_ov[AMT], errors='coerce').fillna(0)
    overdue_total = _ov[_ov['_days'] > 30]['_amt'].sum()
    overdue_count = len(_ov[_ov['_days'] > 30])
else:
    overdue_total = overdue_count = 0

# ══════════════════════════════════════════════════════════════════════════════
# Section 1 — Key Metrics
# ══════════════════════════════════════════════════════════════════════════════

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
    st.metric("Net Position", fmt(net_position))

k5, k6, k7, k8 = st.columns(4)
with k5:
    st.metric("Active Buyers", n_buyers)
with k6:
    st.metric("Active Suppliers", n_suppliers)
with k7:
    st.metric("Sales Invoices", n_out)
with k8:
    st.metric("Purchase Invoices", n_in)

k9, k10, k11, k12 = st.columns(4)
with k9:
    st.metric("Revenue Growth", f"{rev_growth:+.1f}%" if revenue_prev > 0 else "N/A")
with k10:
    st.metric("Avg Invoice", fmt(avg_invoice))
with k11:
    st.metric("Concentration (Top 3)", f"{top3_pct:.0f}%",
              help="% of revenue from top 3 buyers")
with k12:
    st.metric("Overdue (>30d)", fmt(overdue_total),
              delta=f"{overdue_count} invoices", delta_color="inverse")

# ══════════════════════════════════════════════════════════════════════════════
# Section 2 — Revenue Trends (merged single chart)
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("---")
st.markdown("### Revenue Trends")

if not sales.empty:
    _t = sales.copy()
    _t['Document Date'] = pd.to_datetime(_t['Document Date'], errors='coerce')
    _t['_amt'] = pd.to_numeric(_t[AMT], errors='coerce').fillna(0)
    _t = _t[_t['Document Date'].notna()]
    _t['_month'] = _t['Document Date'].dt.to_period('M').dt.to_timestamp()
    monthly_rev = _t.groupby('_month').agg(
        revenue=('_amt', 'sum'),
        invoices=('_amt', 'count'),
        customers=('Buyer (Tax ID or PINFL)', 'nunique') if 'Buyer (Tax ID or PINFL)' in _t.columns else ('_amt', 'count')
    ).reset_index()

    if not purchases.empty:
        _p = purchases.copy()
        _p['Document Date'] = pd.to_datetime(_p['Document Date'], errors='coerce')
        _p['_amt'] = pd.to_numeric(_p[AMT], errors='coerce').fillna(0)
        _p = _p[_p['Document Date'].notna()]
        _p['_month'] = _p['Document Date'].dt.to_period('M').dt.to_timestamp()
        monthly_exp = _p.groupby('_month')['_amt'].sum().reset_index()
        monthly_exp.columns = ['_month', 'expenses']
        combined = monthly_rev.merge(monthly_exp, on='_month', how='outer').fillna(0).sort_values('_month')
    else:
        combined = monthly_rev.copy()
        combined['expenses'] = 0

    combined['profit'] = combined['revenue'] - combined['expenses']

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=combined['_month'], y=combined['revenue'],
        name='Revenue', marker_color='#10B981', opacity=0.85
    ))
    fig.add_trace(go.Bar(
        x=combined['_month'], y=combined['expenses'],
        name='Expenses', marker_color='#EF4444', opacity=0.85
    ))
    fig.add_trace(go.Scatter(
        x=combined['_month'], y=combined['profit'],
        mode='lines+markers', name='Profit',
        line=dict(color='#0B5C5F', width=3),
        marker=dict(size=6)
    ))
    if 'customers' in combined.columns:
        fig.add_trace(go.Scatter(
            x=combined['_month'], y=combined['customers'],
            mode='lines+markers', name='Customers',
            line=dict(color='#F59E0B', width=2, dash='dot'),
            marker=dict(size=5), yaxis='y2'
        ))

    fig.update_layout(
        barmode='group',
        yaxis=dict(title='Amount (UZS)'),
        yaxis2=dict(title='Customers', overlaying='y', side='right',
                    showgrid=False) if 'customers' in combined.columns else {},
        height=420,
        margin=dict(l=20, r=20, t=20, b=20),
        legend=dict(orientation='h', y=1.05),
        hovermode='x unified'
    )
    st.plotly_chart(fig, use_container_width=True)

    col_info1, col_info2, col_info3 = st.columns(3)
    with col_info1:
        best_month = combined.loc[combined['revenue'].idxmax(), '_month'] if combined['revenue'].max() > 0 else None
        st.caption(f"Peak revenue month: **{best_month.strftime('%b %Y') if best_month is not None else 'N/A'}**")
    with col_info2:
        avg_monthly_rev = combined['revenue'].mean()
        st.caption(f"Avg monthly revenue: **{fmt(avg_monthly_rev)}**")
    with col_info3:
        profitable_months = (combined['profit'] > 0).sum()
        st.caption(f"Profitable months: **{profitable_months} / {len(combined)}**")
else:
    st.info("No outgoing invoices to show trends.")

# ══════════════════════════════════════════════════════════════════════════════
# Section 3 — Top Buyers & Top Suppliers
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("---")
st.markdown("### Counterparties")

cp1, cp2 = st.columns(2)

with cp1:
    st.markdown("#### Top Buyers")
    if not sales_p.empty and 'Buyer (Name)' in sales_p.columns:
        _b = sales_p.copy()
        _b['_amt'] = pd.to_numeric(_b[AMT], errors='coerce').fillna(0)
        top_b = _b.groupby('Buyer (Name)')['_amt'].sum().sort_values(ascending=False).head(10)
        fig = px.bar(
            x=top_b.values,
            y=[n[:50] + '...' if len(str(n)) > 50 else str(n) for n in top_b.index],
            orientation='h',
            labels={'x': 'Revenue', 'y': ''},
            text=[fmt(v, '') for v in top_b.values]
        )
        fig.update_traces(marker_color='#0B5C5F', textposition='outside',
                          textfont_size=11)
        fig.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            height=420, margin=dict(l=10, r=60, t=10, b=20), showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No buyer data available for this period.")

with cp2:
    st.markdown("#### Top Suppliers")
    if not purchases_p.empty and 'Seller (Name)' in purchases_p.columns:
        _s = purchases_p.copy()
        _s['_amt'] = pd.to_numeric(_s[AMT], errors='coerce').fillna(0)
        top_s = _s.groupby('Seller (Name)')['_amt'].sum().sort_values(ascending=False).head(10)
        fig = px.bar(
            x=top_s.values,
            y=[n[:50] + '...' if len(str(n)) > 50 else str(n) for n in top_s.index],
            orientation='h',
            labels={'x': 'Spend', 'y': ''},
            text=[fmt(v, '') for v in top_s.values]
        )
        fig.update_traces(marker_color='#F59E0B', textposition='outside',
                          textfont_size=11)
        fig.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            height=420, margin=dict(l=10, r=60, t=10, b=20), showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No supplier data available for this period.")

# ══════════════════════════════════════════════════════════════════════════════
# Section 4 — Product Analytics
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("---")
st.markdown("### Product Analytics")

prod_dir = st.radio("Direction", ["Selling (OUT)", "Buying (IN)"], horizontal=True, key="bov_prod_dir")
prod_type = 'OUT' if 'OUT' in prod_dir else 'IN'

items_df = load_integration_items_by_tin(
    company_tin, prod_type,
    start_date=date_start, end_date=date_end
)

if items_df.empty:
    st.info(f"No product data for {prod_type} in this period. Upload CBU-format invoice files to enable product analytics.")
else:
    items_df = items_df.copy()
    items_df['final_sum']   = pd.to_numeric(items_df['final_sum'], errors='coerce').fillna(0)
    items_df['quantity']    = pd.to_numeric(items_df['quantity'], errors='coerce').fillna(0)
    items_df['catalog_name'] = items_df['catalog_name'].fillna('(Unknown)').astype(str).str.strip()
    items_df['catalog_code'] = items_df['catalog_code'].fillna('').astype(str)

    # KPIs
    pk1, pk2, pk3, pk4 = st.columns(4)
    with pk1:
        st.metric("Line Items", f"{len(items_df):,}")
    with pk2:
        st.metric("Unique Products", f"{items_df['catalog_name'].nunique():,}")
    with pk3:
        st.metric("Unique Categories", f"{items_df['catalog_code'].nunique():,}")
    with pk4:
        st.metric("Total Value", fmt(items_df['final_sum'].sum()))

    # ── Product bar chart + Category donut side by side ──
    prod_col, cat_col = st.columns(2)

    with prod_col:
        st.markdown("#### Top Products by Revenue")
        top_prod = (
            items_df.groupby('catalog_name')['final_sum'].sum()
            .sort_values(ascending=False).head(15)
        )
        fig = px.bar(
            x=top_prod.values,
            y=[n[:55] + '...' if len(n) > 55 else n for n in top_prod.index],
            orientation='h',
            labels={'x': 'Revenue', 'y': ''},
            color=top_prod.values,
            color_continuous_scale='Oranges'
        )
        fig.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            height=480, showlegend=False,
            margin=dict(l=10, r=20, t=10, b=20),
            coloraxis_showscale=False
        )
        st.plotly_chart(fig, use_container_width=True)

    with cat_col:
        st.markdown("#### Category Distribution (Top 5 + Others)")
        cat_rev = (
            items_df.groupby('catalog_name')['final_sum'].sum()
            .sort_values(ascending=False)
        )
        if len(cat_rev) > 5:
            top5 = cat_rev.head(5)
            others_val = cat_rev.iloc[5:].sum()
            pie_labels = list(top5.index) + ['Others']
            pie_values = list(top5.values) + [others_val]
        else:
            pie_labels = list(cat_rev.index)
            pie_values = list(cat_rev.values)

        pie_labels_short = [
            (lbl[:40] + '...') if len(lbl) > 40 else lbl
            for lbl in pie_labels
        ]

        fig = go.Figure(go.Pie(
            labels=pie_labels_short,
            values=pie_values,
            hole=0.42,
            textinfo='label+percent',
            textposition='outside',
            marker=dict(colors=px.colors.qualitative.Plotly)
        ))
        fig.update_layout(
            height=480,
            margin=dict(l=10, r=10, t=10, b=10),
            showlegend=True,
            legend=dict(orientation='v', x=1.02, y=0.5)
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Monthly trend for top 5 products ──
    if 'factura_date' in items_df.columns and items_df['factura_date'].notna().any():
        st.markdown("#### Top 5 Products — Monthly Trend")
        top5_names = (
            items_df.groupby('catalog_name')['final_sum'].sum()
            .sort_values(ascending=False).head(5).index.tolist()
        )
        trend_df = items_df[items_df['catalog_name'].isin(top5_names)].copy()
        trend_df['factura_date'] = pd.to_datetime(trend_df['factura_date'])
        trend_df['month'] = trend_df['factura_date'].dt.to_period('M').dt.to_timestamp()
        trend_agg = trend_df.groupby(['month', 'catalog_name'])['final_sum'].sum().reset_index()
        trend_agg['catalog_name'] = trend_agg['catalog_name'].apply(
            lambda x: x[:40] + '...' if len(x) > 40 else x
        )
        fig = px.line(trend_agg, x='month', y='final_sum', color='catalog_name',
                      markers=True, labels={'final_sum': 'Revenue', 'month': ''})
        fig.update_layout(
            height=380, margin=dict(l=20, r=20, t=10, b=20),
            legend=dict(orientation='h', yanchor='bottom', y=-0.35)
        )
        st.plotly_chart(fig, use_container_width=True)
