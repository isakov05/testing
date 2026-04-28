"""
Insights Engine — rule-based analytics for factoring dashboard.
Generates KPIs, alerts, readiness scores, churn detection, revenue attribution.
All from integration.invoices + integration.invoice_items, no LLM needed.
"""
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional


def fmt(amount, suffix=""):
    """Format number with smart abbreviation."""
    if amount is None or pd.isna(amount):
        return "0" + suffix
    try:
        amount = float(amount)
        if abs(amount) >= 1e9:
            return f"{amount/1e9:,.1f}B{suffix}"
        if abs(amount) >= 1e6:
            return f"{amount/1e6:,.1f}M{suffix}"
        if abs(amount) >= 1e3:
            return f"{amount/1e3:,.0f}K{suffix}"
        return f"{amount:,.0f}{suffix}"
    except (ValueError, TypeError):
        return str(amount)


# ═══════════════════════════════════════════════════════════════════
# KPI Calculations
# ═══════════════════════════════════════════════════════════════════

def calc_kpis(df_current: pd.DataFrame, df_previous: pd.DataFrame,
              amt_col: str = 'delivery_sum_with_vat',
              counterparty_col: str = 'seller_tin') -> Dict[str, Any]:
    """Calculate all KPI metrics with period comparison."""
    cur_total = df_current[amt_col].sum() if not df_current.empty else 0
    prev_total = df_previous[amt_col].sum() if not df_previous.empty else 0
    growth = ((cur_total - prev_total) / prev_total * 100) if prev_total > 0 else 0

    cur_count = len(df_current)
    prev_count = len(df_previous)
    count_growth = ((cur_count - prev_count) / prev_count * 100) if prev_count > 0 else 0

    cur_cp = df_current[counterparty_col].nunique() if not df_current.empty else 0
    prev_cp = df_previous[counterparty_col].nunique() if not df_previous.empty else 0
    new_cp = cur_cp - prev_cp

    # Concentration top 3
    if not df_current.empty:
        by_cp = df_current.groupby(counterparty_col)[amt_col].sum().sort_values(ascending=False)
        top3_pct = (by_cp.head(3).sum() / by_cp.sum() * 100) if by_cp.sum() > 0 else 0
    else:
        top3_pct = 0

    # Avg invoice
    avg_invoice = cur_total / cur_count if cur_count > 0 else 0

    return {
        'total_invoices': cur_count,
        'total_invoices_prev': prev_count,
        'invoices_growth': count_growth,
        'total_volume': cur_total,
        'total_volume_prev': prev_total,
        'volume_growth': growth,
        'counterparties': cur_cp,
        'new_counterparties': new_cp,
        'concentration_top3': top3_pct,
        'avg_invoice': avg_invoice,
    }


# ═══════════════════════════════════════════════════════════════════
# Revenue vs Expenses
# ═══════════════════════════════════════════════════════════════════

def calc_revenue_expenses(df_out: pd.DataFrame, df_in: pd.DataFrame,
                          df_out_prev: pd.DataFrame, df_in_prev: pd.DataFrame,
                          amt_col: str = 'delivery_sum_with_vat') -> Dict[str, Any]:
    """Calculate revenue, expenses, profit, margin with comparison."""
    revenue = df_out[amt_col].sum() if not df_out.empty else 0
    expenses = df_in[amt_col].sum() if not df_in.empty else 0
    profit = revenue - expenses
    margin = (profit / revenue * 100) if revenue > 0 else 0

    rev_prev = df_out_prev[amt_col].sum() if not df_out_prev.empty else 0
    exp_prev = df_in_prev[amt_col].sum() if not df_in_prev.empty else 0
    margin_prev = ((rev_prev - exp_prev) / rev_prev * 100) if rev_prev > 0 else 0

    return {
        'revenue': revenue,
        'expenses': expenses,
        'profit': profit,
        'margin': margin,
        'margin_prev': margin_prev,
        'margin_change': margin - margin_prev,
    }


# ═══════════════════════════════════════════════════════════════════
# Monthly Trend by Counterparty
# ═══════════════════════════════════════════════════════════════════

def monthly_trend_by_counterparty(df: pd.DataFrame, top_n: int = 5,
                                   amt_col: str = 'delivery_sum_with_vat',
                                   cp_col: str = 'seller_name',
                                   date_col: str = 'factura_date') -> pd.DataFrame:
    """Monthly volume stacked by top N counterparties + others."""
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df[amt_col] = pd.to_numeric(df[amt_col], errors='coerce').fillna(0)
    df = df[df[date_col].notna()]

    # Find top N
    top_cps = df.groupby(cp_col)[amt_col].sum().sort_values(ascending=False).head(top_n).index.tolist()

    df['_group'] = df[cp_col].apply(lambda x: x if x in top_cps else 'Others')
    df['_month'] = df[date_col].dt.to_period('M').dt.to_timestamp()

    pivot = df.groupby(['_month', '_group'])[amt_col].sum().reset_index()
    pivot.columns = ['month', 'counterparty', 'amount']
    return pivot


# ═══════════════════════════════════════════════════════════════════
# Aging Buckets
# ═══════════════════════════════════════════════════════════════════

def calc_aging(df: pd.DataFrame, date_col: str = 'factura_date',
               amt_col: str = 'delivery_sum_with_vat') -> List[Dict]:
    """Calculate aging buckets."""
    if df.empty:
        return []

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df[amt_col] = pd.to_numeric(df[amt_col], errors='coerce').fillna(0)
    df['_days'] = (datetime.now() - df[date_col]).dt.days
    total = df[amt_col].sum()

    buckets = [
        ('0-30', 0, 30, '#10B981'),
        ('31-60', 31, 60, '#3B82F6'),
        ('61-90', 61, 90, '#F59E0B'),
        ('91-180', 91, 180, '#EF4444'),
        ('180+', 181, 999999, '#991B1B'),
    ]

    result = []
    for name, lo, hi, color in buckets:
        subset = df[(df['_days'] >= lo) & (df['_days'] <= hi)]
        amount = subset[amt_col].sum()
        count = len(subset)
        pct = (amount / total * 100) if total > 0 else 0
        result.append({'bucket': name, 'amount': amount, 'count': count, 'pct': pct, 'color': color})

    return result


# ═══════════════════════════════════════════════════════════════════
# Top Counterparties
# ═══════════════════════════════════════════════════════════════════

def top_counterparties(df: pd.DataFrame, n: int = 7,
                       cp_col: str = 'seller_tin',
                       name_col: str = 'seller_name',
                       amt_col: str = 'delivery_sum_with_vat') -> pd.DataFrame:
    """Top N counterparties by volume."""
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df[amt_col] = pd.to_numeric(df[amt_col], errors='coerce').fillna(0)
    agg = df.groupby([cp_col, name_col]).agg(
        volume=(amt_col, 'sum'),
        count=(amt_col, 'count')
    ).reset_index().sort_values('volume', ascending=False).head(n)
    agg.columns = ['tin', 'name', 'volume', 'count']
    return agg


# ═══════════════════════════════════════════════════════════════════
# Client Dynamics (new, churned, returning)
# ═══════════════════════════════════════════════════════════════════

def calc_client_dynamics(df_current: pd.DataFrame, df_previous: pd.DataFrame,
                          cp_col: str = 'seller_tin',
                          name_col: str = 'seller_name',
                          amt_col: str = 'delivery_sum_with_vat') -> Dict[str, Any]:
    """Detect new, churned, and returning clients."""
    if df_current.empty:
        return {'new': [], 'churned': [], 'returning_count': 0, 'growth_pct': 0}

    cur_cps = set(df_current[cp_col].dropna().unique())
    prev_cps = set(df_previous[cp_col].dropna().unique()) if not df_previous.empty else set()

    new_tins = cur_cps - prev_cps
    churned_tins = prev_cps - cur_cps
    returning_tins = cur_cps & prev_cps

    # New clients with amounts
    new_list = []
    if new_tins:
        new_df = df_current[df_current[cp_col].isin(new_tins)]
        new_agg = new_df.groupby([cp_col, name_col])[amt_col].sum().reset_index()
        new_agg.columns = ['tin', 'name', 'amount']
        new_list = new_agg.sort_values('amount', ascending=False).head(5).to_dict('records')

    # Churned clients with last period amounts
    churned_list = []
    if churned_tins and not df_previous.empty:
        ch_df = df_previous[df_previous[cp_col].isin(churned_tins)]
        ch_agg = ch_df.groupby([cp_col, name_col])[amt_col].sum().reset_index()
        ch_agg.columns = ['tin', 'name', 'amount']
        churned_list = ch_agg.sort_values('amount', ascending=False).head(5).to_dict('records')

    growth = ((len(cur_cps) - len(prev_cps)) / len(prev_cps) * 100) if len(prev_cps) > 0 else 0

    return {
        'new': new_list,
        'new_count': len(new_tins),
        'churned': churned_list,
        'churned_count': len(churned_tins),
        'returning_count': len(returning_tins),
        'growth_pct': growth,
    }


# ═══════════════════════════════════════════════════════════════════
# Contract Analysis
# ═══════════════════════════════════════════════════════════════════

def top_contracts(df: pd.DataFrame, n: int = 5,
                  cp_name_col: str = 'seller_name',
                  amt_col: str = 'delivery_sum_with_vat') -> pd.DataFrame:
    """Top contracts by volume."""
    if df.empty or 'contract_no' not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df[amt_col] = pd.to_numeric(df[amt_col], errors='coerce').fillna(0)
    df['contract_date'] = pd.to_datetime(df['contract_date'], errors='coerce')

    agg = df.groupby('contract_no').agg(
        counterparty=(cp_name_col, 'first'),
        invoices=(amt_col, 'count'),
        volume=(amt_col, 'sum'),
        start_date=('contract_date', 'min'),
    ).reset_index().sort_values('volume', ascending=False).head(n)

    return agg


# ═══════════════════════════════════════════════════════════════════
# Invoice Velocity
# ═══════════════════════════════════════════════════════════════════

def calc_velocity(df: pd.DataFrame, df_prev: pd.DataFrame,
                  date_col: str = 'factura_date',
                  amt_col: str = 'delivery_sum_with_vat') -> Dict[str, Any]:
    """Calculate invoice frequency and volume per week."""
    if df.empty:
        return {'invoices_per_week': 0, 'volume_per_week': 0, 'avg_invoice': 0,
                'inv_growth': 0, 'vol_growth': 0}

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df[amt_col] = pd.to_numeric(df[amt_col], errors='coerce').fillna(0)
    df = df[df[date_col].notna()]

    days = max((df[date_col].max() - df[date_col].min()).days, 1)
    weeks = max(days / 7, 1)

    inv_per_week = len(df) / weeks
    vol_per_week = df[amt_col].sum() / weeks
    avg_inv = df[amt_col].mean()

    # Previous period
    if not df_prev.empty:
        df_prev = df_prev.copy()
        df_prev[date_col] = pd.to_datetime(df_prev[date_col], errors='coerce')
        df_prev[amt_col] = pd.to_numeric(df_prev[amt_col], errors='coerce').fillna(0)
        df_prev = df_prev[df_prev[date_col].notna()]
        days_p = max((df_prev[date_col].max() - df_prev[date_col].min()).days, 1)
        weeks_p = max(days_p / 7, 1)
        inv_prev = len(df_prev) / weeks_p
        vol_prev = df_prev[amt_col].sum() / weeks_p
    else:
        inv_prev = inv_per_week
        vol_prev = vol_per_week

    return {
        'invoices_per_week': inv_per_week,
        'volume_per_week': vol_per_week,
        'avg_invoice': avg_inv,
        'inv_growth': ((inv_per_week - inv_prev) / inv_prev * 100) if inv_prev > 0 else 0,
        'vol_growth': ((vol_per_week - vol_prev) / vol_prev * 100) if vol_prev > 0 else 0,
    }


# ═══════════════════════════════════════════════════════════════════
# Top Products (from invoice_items)
# ═══════════════════════════════════════════════════════════════════

def top_products(items_df: pd.DataFrame, n: int = 7) -> pd.DataFrame:
    """Top products by revenue."""
    if items_df.empty or 'catalog_name' not in items_df.columns:
        return pd.DataFrame()

    items_df = items_df.copy()
    items_df['final_sum'] = pd.to_numeric(items_df['final_sum'], errors='coerce').fillna(0)

    agg = items_df.groupby('catalog_name').agg(
        revenue=('final_sum', 'sum'),
        qty=('quantity', 'sum'),
        count=('final_sum', 'count')
    ).reset_index().sort_values('revenue', ascending=False).head(n)

    return agg


# ═══════════════════════════════════════════════════════════════════
# Revenue Attribution (explain changes)
# ═══════════════════════════════════════════════════════════════════

def explain_revenue_change(df_current: pd.DataFrame, df_previous: pd.DataFrame,
                            cp_col: str = 'seller_tin',
                            name_col: str = 'seller_name',
                            amt_col: str = 'delivery_sum_with_vat',
                            top_n: int = 3) -> Dict[str, Any]:
    """Attribute revenue change to specific counterparties."""
    if df_current.empty:
        return {'direction': 'flat', 'total_change': 0, 'contributors': [], 'pct_change': 0}

    cur = df_current.groupby([cp_col, name_col])[amt_col].sum().reset_index()
    cur.columns = ['tin', 'name', 'current']

    if not df_previous.empty:
        prev = df_previous.groupby([cp_col, name_col])[amt_col].sum().reset_index()
        prev.columns = ['tin', 'name', 'previous']
        merged = cur.merge(prev[['tin', 'previous']], on='tin', how='outer').fillna(0)
    else:
        merged = cur.copy()
        merged['previous'] = 0

    merged['change'] = merged['current'] - merged['previous']
    merged['abs_change'] = merged['change'].abs()

    total_change = merged['change'].sum()
    prev_total = merged['previous'].sum()
    pct = (total_change / prev_total * 100) if prev_total > 0 else 0
    direction = 'up' if total_change > 0 else ('down' if total_change < 0 else 'flat')

    # Top contributors (biggest absolute change)
    top = merged.sort_values('abs_change', ascending=False).head(top_n)
    contributors = []
    for _, r in top.iterrows():
        contributors.append({
            'name': r['name'] if r['name'] and r['name'] != '0' else r['tin'],
            'change': r['change'],
            'current': r['current'],
            'previous': r['previous'],
        })

    return {
        'direction': direction,
        'total_change': total_change,
        'pct_change': pct,
        'contributors': contributors,
    }


# ═══════════════════════════════════════════════════════════════════
# Factoring Readiness Scoring
# ═══════════════════════════════════════════════════════════════════

def score_factoring_readiness(invoice_row: pd.Series, buyer_stats: Dict,
                               portfolio_stats: Dict) -> Dict[str, Any]:
    """Score a single invoice for factoring readiness (0-100)."""
    score = 0
    reasons = []

    # Factor 1: Buyer reliability (30 pts)
    inv_count = buyer_stats.get('invoice_count', 0)
    if inv_count >= 50:
        f1 = 28
        reasons.append(f"Reliable buyer ({inv_count} invoices)")
    elif inv_count >= 20:
        f1 = 22
    elif inv_count >= 5:
        f1 = 12
    else:
        f1 = 4
        reasons.append(f"New buyer (only {inv_count} invoices)")
    score += f1

    # Factor 2: Amount normality (20 pts)
    amount = float(invoice_row.get('delivery_sum_with_vat', 0) or 0)
    avg = buyer_stats.get('avg_amount', 0)
    if avg > 0 and amount > 0:
        ratio = amount / avg
        if 0.3 <= ratio <= 3.0:
            f2 = 18
        elif 0.1 <= ratio <= 5.0:
            f2 = 10
            reasons.append(f"Amount is {ratio:.1f}x buyer's average")
        else:
            f2 = 3
            reasons.append(f"Amount is {ratio:.1f}x buyer's average — anomalous")
    else:
        f2 = 8
    score += f2

    # Factor 3: Contract strength (20 pts)
    contract = invoice_row.get('contract_no')
    contract_invoices = buyer_stats.get('contract_invoices', 0)
    if contract and str(contract).strip():
        f3 = 5
        if contract_invoices >= 10:
            f3 += 10
            reasons.append(f"Strong contract ({contract_invoices} invoices)")
        elif contract_invoices >= 3:
            f3 += 6
        else:
            f3 += 2
        # Contract age
        contract_date = buyer_stats.get('contract_start')
        if contract_date and (datetime.now() - pd.Timestamp(contract_date)).days > 180:
            f3 += 5
    else:
        f3 = 0
        reasons.append("No contract number")
    score += f3

    # Factor 4: Concentration safety (15 pts)
    buyer_share = buyer_stats.get('portfolio_share', 0)
    if buyer_share < 20:
        f4 = 15
    elif buyer_share < 40:
        f4 = 10
    elif buyer_share < 60:
        f4 = 5
        reasons.append(f"Concentration risk ({buyer_share:.0f}% of portfolio)")
    else:
        f4 = 2
        reasons.append(f"HIGH concentration ({buyer_share:.0f}% of portfolio)")
    score += f4

    # Factor 5: Freshness (15 pts)
    fdate = invoice_row.get('factura_date')
    if fdate:
        days_old = (datetime.now() - pd.Timestamp(fdate)).days
        if days_old <= 7:
            f5 = 15
        elif days_old <= 14:
            f5 = 12
        elif days_old <= 30:
            f5 = 9
        elif days_old <= 60:
            f5 = 5
        else:
            f5 = 1
            reasons.append(f"Invoice is {days_old} days old")
    else:
        f5 = 5
    score += f5

    # Grade
    if score >= 80:
        grade = 'Ready'
    elif score >= 50:
        grade = 'Medium'
    elif score >= 20:
        grade = 'Low'
    else:
        grade = 'Not eligible'

    return {'score': score, 'grade': grade, 'reasons': reasons}


def batch_score_readiness(df: pd.DataFrame,
                          cp_col: str = 'seller_tin',
                          amt_col: str = 'delivery_sum_with_vat') -> pd.DataFrame:
    """Score all invoices for factoring readiness."""
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df[amt_col] = pd.to_numeric(df[amt_col], errors='coerce').fillna(0)
    total_portfolio = df[amt_col].sum()

    # Precompute buyer stats
    buyer_stats_map = {}
    for tin, group in df.groupby(cp_col):
        buyer_stats_map[tin] = {
            'invoice_count': len(group),
            'avg_amount': group[amt_col].mean(),
            'portfolio_share': (group[amt_col].sum() / total_portfolio * 100) if total_portfolio > 0 else 0,
            'contract_invoices': group['contract_no'].notna().sum() if 'contract_no' in group.columns else 0,
            'contract_start': group['contract_date'].min() if 'contract_date' in group.columns else None,
        }

    portfolio_stats = {'total': total_portfolio}

    scores = []
    for idx, row in df.iterrows():
        tin = row.get(cp_col)
        bs = buyer_stats_map.get(tin, {})
        result = score_factoring_readiness(row, bs, portfolio_stats)
        scores.append(result['score'])

    df['readiness_score'] = scores
    df['readiness_grade'] = pd.cut(
        df['readiness_score'],
        bins=[-1, 19, 49, 79, 100],
        labels=['Not eligible', 'Low', 'Medium', 'Ready']
    )

    return df


# ═══════════════════════════════════════════════════════════════════
# Smart Alerts Generator
# ═══════════════════════════════════════════════════════════════════

def generate_smart_alerts(kpis: Dict, rev_exp: Dict, clients: Dict,
                           aging: List[Dict], concentration: float,
                           top_cp_name: str = '') -> List[Dict[str, str]]:
    """Generate prioritized alerts with recommendations."""
    alerts = []

    # Concentration
    if concentration > 50:
        alerts.append({
            'severity': 'red',
            'title': f'Concentration: {top_cp_name} = {concentration:.0f}% of portfolio',
            'desc': f'If {top_cp_name} delays payment, {fmt(kpis.get("total_volume", 0) * concentration / 100)} at risk. Diversify factoring across more counterparties.',
        })

    # Churn
    if clients.get('churned_count', 0) > 0:
        churned = clients['churned']
        names = ', '.join(c['name'][:30] for c in churned[:2])
        total_lost = sum(c['amount'] for c in churned)
        alerts.append({
            'severity': 'red' if total_lost > kpis.get('total_volume', 0) * 0.05 else 'yellow',
            'title': f'Churn: {clients["churned_count"]} client(s) stopped ordering',
            'desc': f'{names}. Lost volume: ~{fmt(total_lost)}/period.',
        })

    # Aging
    overdue_pct = sum(a['pct'] for a in aging if a['bucket'] in ('91-180', '180+'))
    if overdue_pct > 10:
        overdue_amt = sum(a['amount'] for a in aging if a['bucket'] in ('91-180', '180+'))
        alerts.append({
            'severity': 'yellow',
            'title': f'Aging: {overdue_pct:.0f}% of invoices are >90 days old',
            'desc': f'{fmt(overdue_amt)} in overdue invoices. Consider collection actions.',
        })

    # Revenue decline
    if kpis.get('volume_growth', 0) < -10:
        alerts.append({
            'severity': 'yellow',
            'title': f'Volume declined {abs(kpis["volume_growth"]):.0f}% vs previous period',
            'desc': 'Check if this is seasonal or structural.',
        })

    # Margin improvement
    if rev_exp.get('margin_change', 0) > 2:
        alerts.append({
            'severity': 'green',
            'title': f'Margin improved from {rev_exp["margin_prev"]:.0f}% to {rev_exp["margin"]:.0f}%',
            'desc': 'Cost optimization is working.',
        })

    # New clients
    if clients.get('new_count', 0) > 0:
        new_total = sum(c['amount'] for c in clients.get('new', []))
        alerts.append({
            'severity': 'green',
            'title': f'{clients["new_count"]} new client(s) this period (+{fmt(new_total)})',
            'desc': 'Consider adding to factoring after 3+ invoices.',
        })

    # Volume growth
    if kpis.get('volume_growth', 0) > 10:
        alerts.append({
            'severity': 'green',
            'title': f'Volume grew {kpis["volume_growth"]:.0f}% vs previous period',
            'desc': 'Business is expanding.',
        })

    return alerts


# ═══════════════════════════════════════════════════════════════════
# Insights Banner (top 3-4 auto-generated sentences)
# ═══════════════════════════════════════════════════════════════════

def generate_insights_banner(kpis: Dict, rev_attribution: Dict,
                              readiness_summary: Dict, clients: Dict,
                              concentration: float, top_cp_name: str) -> List[Dict]:
    """Generate 3-4 key insight bullets for the banner."""
    insights = []

    # Revenue change
    ra = rev_attribution
    if ra['direction'] == 'up' and ra['contributors']:
        top = ra['contributors'][0]
        insights.append({
            'color': 'green',
            'text': f'Volume grew <b>{ra["pct_change"]:.0f}%</b>. Main driver: <b>{top["name"][:40]}</b> (+{fmt(top["change"])}).'
        })
    elif ra['direction'] == 'down' and ra['contributors']:
        top = ra['contributors'][0]
        insights.append({
            'color': 'red',
            'text': f'Volume dropped <b>{abs(ra["pct_change"]):.0f}%</b>. Main cause: <b>{top["name"][:40]}</b> ({fmt(top["change"])}).'
        })

    # Readiness
    ready = readiness_summary.get('ready_count', 0)
    ready_amt = readiness_summary.get('ready_amount', 0)
    if ready > 0:
        insights.append({
            'color': 'yellow',
            'text': f'<b>{ready} invoices</b> ready for factoring (total: {fmt(ready_amt)}). Top candidate: {readiness_summary.get("top_candidate", "N/A")}.'
        })

    # Concentration
    if concentration > 40:
        insights.append({
            'color': 'red' if concentration > 60 else 'yellow',
            'text': f'Concentration <b>{"high" if concentration > 60 else "moderate"}</b>: {top_cp_name} = {concentration:.0f}% of portfolio.'
        })

    # Client dynamics
    if clients.get('new_count', 0) > 0 or clients.get('churned_count', 0) > 0:
        parts = []
        if clients['new_count'] > 0:
            new_amt = sum(c['amount'] for c in clients.get('new', []))
            parts.append(f'<b>{clients["new_count"]} new buyer(s)</b> (+{fmt(new_amt)})')
        if clients['churned_count'] > 0:
            parts.append(f'<b>{clients["churned_count"]} left</b>')
        insights.append({
            'color': 'blue',
            'text': '. '.join(parts) + '.'
        })

    return insights[:4]
