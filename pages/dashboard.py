"""
Analytics Dashboard — matches mockup layout with real data from integration.invoices.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date, timedelta

from auth.db_authenticator import protect_page
from utils.integration_loader import (
    load_raw_invoices,
    load_integration_items_by_tin,
    get_user_company_tin,
)
from utils.insights_engine import (
    fmt, calc_kpis, calc_revenue_expenses, monthly_trend_by_counterparty,
    calc_aging, top_counterparties, calc_client_dynamics, top_contracts,
    calc_velocity, top_products, explain_revenue_change,
    batch_score_readiness, generate_smart_alerts, generate_insights_banner,
)

protect_page()

# ═══════════════════════════════════════════════════════════════════
# Setup
# ═══════════════════════════════════════════════════════════════════
user_id = st.session_state.get('user_id')
if not user_id:
    st.warning("Please log in.")
    st.stop()

company_tin = get_user_company_tin(str(user_id))
if not company_tin:
    st.error("No company linked to your account.")
    st.stop()

# ─── Top Bar: Period + Direction Toggle ───
st.title("Analytics")

bar1, bar2, bar3, bar4 = st.columns([1, 1, 1, 2])
with bar1:
    period = st.selectbox("Period", ["Last month", "Last 3 months", "Last 6 months", "Last year"],
                          index=2, label_visibility="collapsed")
with bar2:
    d_start = st.date_input("From", value=datetime.now() - timedelta(days=180), label_visibility="collapsed")
with bar3:
    d_end = st.date_input("To", value=datetime.now(), label_visibility="collapsed")
with bar4:
    direction = st.radio("Direction", ["Buyer (IN)", "Supplier (OUT)"],
                         horizontal=True, label_visibility="collapsed")

start_dt = datetime.combine(d_start, datetime.min.time())
end_dt = datetime.combine(d_end, datetime.max.time())
period_days = (end_dt - start_dt).days
prev_start = start_dt - timedelta(days=period_days)

is_buyer = "IN" in direction
inv_type = 'IN' if is_buyer else 'OUT'
cp_tin_col = 'seller_tin' if is_buyer else 'buyer_tin'
cp_name_col = 'seller_name' if is_buyer else 'buyer_name'

# ═══════════════════════════════════════════════════════════════════
# Load Data
# ═══════════════════════════════════════════════════════════════════
@st.cache_data(ttl=120)
def load_data(tin, inv_type, start, end, prev_start):
    raw = load_raw_invoices(tin, inv_type)
    if raw.empty:
        return raw, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    raw['factura_date'] = pd.to_datetime(raw['factura_date'], errors='coerce')
    raw['delivery_sum_with_vat'] = pd.to_numeric(raw['delivery_sum_with_vat'], errors='coerce').fillna(0)
    cur = raw[(raw['factura_date'] >= pd.Timestamp(start)) & (raw['factura_date'] <= pd.Timestamp(end))]
    prev = raw[(raw['factura_date'] >= pd.Timestamp(prev_start)) & (raw['factura_date'] < pd.Timestamp(start))]
    return raw, cur, prev, raw

# Load both directions for revenue vs expenses
with st.spinner("Loading..."):
    raw_main, df_cur, df_prev, _ = load_data(company_tin, inv_type, start_dt, end_dt, prev_start)
    # Other direction for rev vs exp
    other_type = 'OUT' if is_buyer else 'IN'
    _, df_other_cur, df_other_prev, _ = load_data(company_tin, other_type, start_dt, end_dt, prev_start)

if df_cur.empty and df_prev.empty:
    st.info("No invoices found for this period and direction.")
    st.stop()

AMT = 'delivery_sum_with_vat'

# ═══════════════════════════════════════════════════════════════════
# Compute Everything
# ═══════════════════════════════════════════════════════════════════
kpis = calc_kpis(df_cur, df_prev, AMT, cp_tin_col)

rev_exp = calc_revenue_expenses(
    df_cur if not is_buyer else df_other_cur,
    df_cur if is_buyer else df_other_cur,
    df_prev if not is_buyer else df_other_prev,
    df_prev if is_buyer else df_other_prev,
    AMT
)

clients = calc_client_dynamics(df_cur, df_prev, cp_tin_col, cp_name_col, AMT)
aging = calc_aging(df_cur, 'factura_date', AMT)
top_cp = top_counterparties(df_cur, 7, cp_tin_col, cp_name_col, AMT)
velocity = calc_velocity(df_cur, df_prev, 'factura_date', AMT)
rev_attr = explain_revenue_change(df_cur, df_prev, cp_tin_col, cp_name_col, AMT)

# Readiness scoring
scored = batch_score_readiness(df_cur, cp_tin_col, AMT)
ready_invoices = scored[scored['readiness_grade'] == 'Ready'] if not scored.empty else pd.DataFrame()
readiness_summary = {
    'ready_count': len(ready_invoices),
    'ready_amount': ready_invoices[AMT].sum() if not ready_invoices.empty else 0,
    'medium_count': len(scored[scored['readiness_grade'] == 'Medium']) if not scored.empty else 0,
    'low_count': len(scored[scored['readiness_grade'] == 'Low']) if not scored.empty else 0,
    'none_count': len(scored[scored['readiness_grade'] == 'Not eligible']) if not scored.empty else 0,
    'top_candidate': f"#{ready_invoices.iloc[0].get('factura_no', 'N/A')} {ready_invoices.iloc[0].get(cp_name_col, '')[:30]}" if not ready_invoices.empty else 'N/A',
}

# Top counterparty for concentration
top_cp_name = top_cp.iloc[0]['name'][:40] if not top_cp.empty else 'N/A'

# Alerts
alerts = generate_smart_alerts(kpis, rev_exp, clients, aging,
                                kpis['concentration_top3'], top_cp_name)

# Insights banner
insights = generate_insights_banner(kpis, rev_attr, readiness_summary, clients,
                                     kpis['concentration_top3'], top_cp_name)

# ═══════════════════════════════════════════════════════════════════
# Section 1: Insights Banner
# ═══════════════════════════════════════════════════════════════════
color_map = {'green': '#10B981', 'yellow': '#F59E0B', 'red': '#EF4444', 'blue': '#3B82F6'}

if insights:
    insights_html = '<div style="background:linear-gradient(135deg,#F0FDF4,#ECFDF5,#FFF7ED);border:1px solid #D1FAE5;border-radius:12px;padding:20px 24px;">'
    insights_html += '<div style="font-size:13px;font-weight:600;color:#6B7280;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:12px;">Key Observations</div>'
    for ins in insights:
        c = color_map.get(ins['color'], '#6B7280')
        insights_html += f'<div style="display:flex;align-items:flex-start;gap:10px;margin-bottom:8px;font-size:14px;line-height:1.5;"><div style="width:8px;height:8px;border-radius:50%;background:{c};margin-top:6px;flex-shrink:0;"></div><div>{ins["text"]}</div></div>'
    insights_html += '</div>'
    st.markdown(insights_html, unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════
# Section 2: KPI Cards
# ═══════════════════════════════════════════════════════════════════
st.markdown("")
k1, k2, k3, k4, k5 = st.columns(5)
with k1:
    delta = f"{kpis['invoices_growth']:+.0f}%" if kpis['total_invoices_prev'] > 0 else None
    st.metric("Total Invoices", f"{kpis['total_invoices']:,}", delta=delta)
with k2:
    delta = f"{kpis['volume_growth']:+.0f}%" if kpis['total_volume_prev'] > 0 else None
    st.metric("Total Volume", fmt(kpis['total_volume']), delta=delta)
with k3:
    st.metric("Margin", f"{rev_exp['margin']:.1f}%",
              delta=f"{rev_exp['margin_change']:+.1f}%" if rev_exp['margin_prev'] > 0 else None)
with k4:
    st.metric("Counterparties", kpis['counterparties'],
              delta=f"+{kpis['new_counterparties']}" if kpis['new_counterparties'] > 0 else None)
with k5:
    st.metric("Concentration Top-3", f"{kpis['concentration_top3']:.0f}%",
              help="% of volume from top 3 counterparties")

# ═══════════════════════════════════════════════════════════════════
# Section 3: Revenue vs Expenses
# ═══════════════════════════════════════════════════════════════════
st.markdown("---")
re1, re2 = st.columns([3, 1])

with re1:
    st.markdown("#### Revenue & Expenses")
    fig = go.Figure()
    fig.add_trace(go.Bar(name='Revenue', x=['Revenue'], y=[rev_exp['revenue']],
                         marker_color='#10B981', text=[fmt(rev_exp['revenue'])], textposition='outside'))
    fig.add_trace(go.Bar(name='Expenses', x=['Expenses'], y=[rev_exp['expenses']],
                         marker_color='#EF4444', text=[fmt(rev_exp['expenses'])], textposition='outside'))
    fig.add_trace(go.Bar(name='Profit', x=['Profit'], y=[rev_exp['profit']],
                         marker_color='#0B5C5F', text=[fmt(rev_exp['profit'])], textposition='outside'))
    fig.update_layout(height=280, margin=dict(l=20, r=20, t=10, b=20), showlegend=False,
                      yaxis_title='', xaxis_title='')
    st.plotly_chart(fig, use_container_width=True)

with re2:
    st.markdown("")
    st.markdown("")
    margin_color = '#059669' if rev_exp['margin'] > 20 else '#D97706' if rev_exp['margin'] > 0 else '#DC2626'
    st.markdown(f"""
    <div style="text-align:center; padding:24px; background:#F0FDF4; border-radius:12px; margin-top:20px;">
        <div style="font-size:12px; color:#6B7280;">Margin</div>
        <div style="font-size:42px; font-weight:700; color:{margin_color};">{rev_exp['margin']:.1f}%</div>
    </div>
    """, unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════
# Section 4: Trend by Counterparty
# ═══════════════════════════════════════════════════════════════════
st.markdown("---")
st.markdown("#### Volume by Counterparty")

trend_data = monthly_trend_by_counterparty(df_cur, 5, AMT, cp_name_col, 'factura_date')
if not trend_data.empty:
    trend_data['counterparty'] = trend_data['counterparty'].apply(
        lambda x: x[:35] + '...' if len(str(x)) > 35 else x)
    fig = px.area(trend_data, x='month', y='amount', color='counterparty',
                  labels={'amount': 'Volume', 'month': '', 'counterparty': ''},
                  color_discrete_sequence=['#0B5C5F', '#EF4444', '#8B5CF6', '#F59E0B', '#93C5FD', '#D1D5DB'])
    fig.update_layout(height=350, margin=dict(l=20, r=20, t=10, b=20),
                      legend=dict(orientation='h', yanchor='bottom', y=-0.25))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No trend data available")

# ═══════════════════════════════════════════════════════════════════
# Section 5: Funnel + Top Counterparties
# ═══════════════════════════════════════════════════════════════════
st.markdown("---")
f1, f2 = st.columns(2)

with f1:
    st.markdown("#### Factoring Funnel")
    total = len(df_cur)
    # Factoring status breakdown (use actual data if available, else estimate)
    has_factoring = 'factoring_status' in df_cur.columns and df_cur['factoring_status'].notna().any()

    if has_factoring:
        submitted = len(df_cur[df_cur['factoring_status'].notna()])
        approved = len(df_cur[df_cur['factoring_status'] == 'approved'])
        financed = len(df_cur[df_cur['factoring_status'] == 'financed'])
    else:
        # Use readiness scores as proxy
        submitted = readiness_summary['ready_count'] + readiness_summary['medium_count']
        approved = readiness_summary['ready_count']
        financed = 0

    funnel_data = pd.DataFrame({
        'stage': ['All Invoices', 'Eligible', 'Ready for Factoring', 'Financed'],
        'count': [total, submitted + approved, approved, financed],
    })
    funnel_data['pct'] = (funnel_data['count'] / total * 100) if total > 0 else 0

    colors = ['#E5E7EB', '#93C5FD', '#0B5C5F', '#059669']
    fig = go.Figure()
    for i, row in funnel_data.iterrows():
        fig.add_trace(go.Bar(
            y=[row['stage']], x=[row['count']], orientation='h',
            marker_color=colors[i], text=[f"{row['count']} ({row['pct']:.0f}%)"],
            textposition='inside', showlegend=False
        ))
    fig.update_layout(height=250, margin=dict(l=20, r=20, t=10, b=20),
                      yaxis={'categoryorder': 'array', 'categoryarray': funnel_data['stage'].tolist()[::-1]},
                      barmode='stack', xaxis_title='')
    st.plotly_chart(fig, use_container_width=True)

    if not has_factoring:
        st.caption("Based on readiness scores (no factoring status data yet)")

with f2:
    st.markdown("#### Top Counterparties")
    if not top_cp.empty:
        top_cp['name_short'] = top_cp['name'].apply(lambda x: x[:30] + '...' if len(str(x)) > 30 else x)
        fig = px.bar(top_cp, x='volume', y='name_short', orientation='h',
                     labels={'volume': 'Volume', 'name_short': ''},
                     text=top_cp['volume'].apply(lambda x: fmt(x)),
                     color='volume', color_continuous_scale='Teal')
        fig.update_layout(height=300, margin=dict(l=20, r=20, t=10, b=20),
                          yaxis={'categoryorder': 'total ascending'}, showlegend=False,
                          coloraxis_showscale=False)
        fig.update_traces(textposition='outside')
        st.plotly_chart(fig, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════
# Section 6: Aging + Readiness
# ═══════════════════════════════════════════════════════════════════
st.markdown("---")
a1, a2 = st.columns(2)

with a1:
    st.markdown("#### Invoice Aging")
    if aging:
        aging_df = pd.DataFrame(aging)
        fig = go.Figure()
        for _, row in aging_df.iterrows():
            fig.add_trace(go.Bar(
                y=[row['bucket']], x=[row['amount']], orientation='h',
                marker_color=row['color'],
                text=[f"{row['pct']:.0f}% — {fmt(row['amount'])}"],
                textposition='inside', showlegend=False
            ))
        fig.update_layout(height=260, margin=dict(l=20, r=20, t=10, b=20),
                          yaxis={'categoryorder': 'array',
                                 'categoryarray': ['180+', '91-180', '61-90', '31-60', '0-30']},
                          xaxis_title='')
        st.plotly_chart(fig, use_container_width=True)

        overdue_pct = sum(a['pct'] for a in aging if a['bucket'] in ('91-180', '180+'))
        if overdue_pct > 5:
            st.warning(f"{overdue_pct:.0f}% of invoices are >90 days old")

with a2:
    st.markdown("#### Factoring Readiness")
    rc1, rc2, rc3, rc4 = st.columns(4)
    with rc1:
        st.metric("Ready", readiness_summary['ready_count'])
    with rc2:
        st.metric("Medium", readiness_summary['medium_count'])
    with rc3:
        st.metric("Low", readiness_summary['low_count'])
    with rc4:
        st.metric("N/A", readiness_summary['none_count'])

    if not ready_invoices.empty:
        st.success(f"**Top candidate:** {readiness_summary['top_candidate']} — {fmt(ready_invoices.iloc[0][AMT])}")
        if len(ready_invoices) > 1:
            st.info(f"**#2:** #{ready_invoices.iloc[1].get('factura_no', 'N/A')} — {fmt(ready_invoices.iloc[1][AMT])}")
    else:
        st.info("No invoices scored as 'Ready' in this period")

# ═══════════════════════════════════════════════════════════════════
# Section 7: Products + Client Dynamics
# ═══════════════════════════════════════════════════════════════════
st.markdown("---")
p1, p2 = st.columns(2)

with p1:
    st.markdown("#### Top Products")
    items = load_integration_items_by_tin(company_tin, inv_type,
                                          start_date=d_start, end_date=d_end)
    products = top_products(items, 7)
    if not products.empty:
        products['name_short'] = products['catalog_name'].apply(
            lambda x: x[:45] + '...' if len(str(x)) > 45 else x)
        fig = px.bar(products, x='revenue', y='name_short', orientation='h',
                     labels={'revenue': 'Revenue', 'name_short': ''},
                     text=products['revenue'].apply(lambda x: fmt(x)),
                     color='revenue', color_continuous_scale='Oranges')
        fig.update_layout(height=320, margin=dict(l=20, r=20, t=10, b=20),
                          yaxis={'categoryorder': 'total ascending'}, showlegend=False,
                          coloraxis_showscale=False)
        fig.update_traces(textposition='outside')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No product data for this period")

with p2:
    st.markdown("#### Client Dynamics")
    cd1, cd2 = st.columns(2)
    with cd1:
        st.metric("New Clients", clients['new_count'], delta=None)
        st.metric("Returning", clients['returning_count'])
    with cd2:
        st.metric("Churned", clients['churned_count'],
                  delta=f"-{clients['churned_count']}" if clients['churned_count'] > 0 else None,
                  delta_color="inverse")
        st.metric("Base Growth", f"{clients['growth_pct']:+.1f}%")

    if clients['new']:
        names = ', '.join(f"{c['name'][:25]} ({fmt(c['amount'])})" for c in clients['new'][:3])
        st.success(f"**New:** {names}")
    if clients['churned']:
        names = ', '.join(f"{c['name'][:25]} (was {fmt(c['amount'])})" for c in clients['churned'][:3])
        st.error(f"**Lost:** {names}")

# ═══════════════════════════════════════════════════════════════════
# Section 8: Contracts + Velocity
# ═══════════════════════════════════════════════════════════════════
st.markdown("---")
c1, c2 = st.columns(2)

with c1:
    st.markdown("#### Top Contracts")
    contracts = top_contracts(df_cur, 5, cp_name_col, AMT)
    if not contracts.empty:
        contracts['start_date'] = contracts['start_date'].dt.strftime('%Y-%m').fillna('—')
        contracts['volume'] = contracts['volume'].apply(lambda x: fmt(x))
        st.dataframe(
            contracts[['contract_no', 'counterparty', 'invoices', 'volume', 'start_date']].rename(columns={
                'contract_no': 'Contract', 'counterparty': 'Counterparty',
                'invoices': 'Invoices', 'volume': 'Volume', 'start_date': 'Since'
            }),
            use_container_width=True, hide_index=True
        )
    else:
        st.info("No contract data")

with c2:
    st.markdown("#### Invoice Velocity")
    v = velocity
    vm1, vm2 = st.columns(2)
    with vm1:
        delta = f"{v['inv_growth']:+.0f}%" if abs(v['inv_growth']) > 0.5 else None
        st.metric("Invoices / week", f"{v['invoices_per_week']:.1f}", delta=delta)
    with vm2:
        delta = f"{v['vol_growth']:+.0f}%" if abs(v['vol_growth']) > 0.5 else None
        st.metric("Volume / week", fmt(v['volume_per_week']), delta=delta)
    st.metric("Avg Invoice", fmt(v['avg_invoice']))

    # Weekly sparkline
    if not df_cur.empty:
        _w = df_cur.copy()
        _w['_week'] = _w['factura_date'].dt.isocalendar().week
        _w['_year'] = _w['factura_date'].dt.year
        weekly = _w.groupby(['_year', '_week'])[AMT].agg(['sum', 'count']).reset_index()
        if len(weekly) > 2:
            fig = go.Figure()
            fig.add_trace(go.Scatter(y=weekly['sum'], mode='lines+markers',
                                     line=dict(color='#0B5C5F', width=2),
                                     marker=dict(size=4)))
            fig.update_layout(height=80, margin=dict(l=0, r=0, t=0, b=0),
                              xaxis=dict(visible=False), yaxis=dict(visible=False),
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════
# Section 9: Alerts & Recommendations
# ═══════════════════════════════════════════════════════════════════
st.markdown("---")
st.markdown("#### Alerts & Recommendations")

if alerts:
    for alert in alerts:
        sev = alert['severity']
        if sev == 'red':
            st.error(f"**{alert['title']}**\n\n{alert['desc']}")
        elif sev == 'yellow':
            st.warning(f"**{alert['title']}**\n\n{alert['desc']}")
        else:
            st.success(f"**{alert['title']}**\n\n{alert['desc']}")
else:
    st.success("All indicators look healthy.")
