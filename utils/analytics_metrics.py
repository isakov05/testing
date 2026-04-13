"""
Analytics metrics calculations for the single-page business overview.
Computes KPIs, trends, aging, counterparty summaries from invoices, bank transactions, and reconciliation data.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List


def format_currency(amount, suffix=" UZS"):
    """Format number with thousands separator."""
    if amount is None or pd.isna(amount):
        return "0" + suffix
    try:
        if abs(amount) >= 1_000_000_000:
            return f"{amount / 1_000_000_000:,.1f}B{suffix}"
        if abs(amount) >= 1_000_000:
            return f"{amount / 1_000_000:,.1f}M{suffix}"
        return f"{amount:,.0f}{suffix}"
    except (ValueError, TypeError):
        return str(amount)


def safe_sum(df: pd.DataFrame, col: str) -> float:
    """Safely sum a column, returning 0 if missing or empty."""
    if df is None or df.empty or col not in df.columns:
        return 0.0
    return pd.to_numeric(df[col], errors='coerce').fillna(0).sum()


def safe_mean(df: pd.DataFrame, col: str) -> float:
    """Safely average a column, returning 0 if missing or empty."""
    if df is None or df.empty or col not in df.columns:
        return 0.0
    vals = pd.to_numeric(df[col], errors='coerce').dropna()
    return vals.mean() if len(vals) > 0 else 0.0


def filter_by_date_range(df: pd.DataFrame, date_col: str, start: datetime, end: datetime) -> pd.DataFrame:
    """Filter DataFrame by date range."""
    if df is None or df.empty or date_col not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    mask = (df[date_col] >= pd.Timestamp(start)) & (df[date_col] <= pd.Timestamp(end))
    return df[mask]


def filter_signed(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only signed/confirmed invoices."""
    if df is None or df.empty or 'Status' not in df.columns:
        return df if df is not None else pd.DataFrame()
    return df[df['Status'].isin(['Подписан', 'Signed', 'signed'])]


# ---------------------------------------------------------------------------
# KPI calculations
# ---------------------------------------------------------------------------

def calc_revenue(invoices_out: pd.DataFrame, start: datetime, end: datetime) -> Dict[str, Any]:
    """Total revenue from outgoing invoices in period."""
    df = filter_signed(invoices_out)
    current = filter_by_date_range(df, 'Document Date', start, end)
    period_len = (end - start).days
    prev_start = start - timedelta(days=period_len)
    previous = filter_by_date_range(df, 'Document Date', prev_start, start)

    cur_total = safe_sum(current, 'Supply Value (incl. VAT)')
    prev_total = safe_sum(previous, 'Supply Value (incl. VAT)')
    growth = ((cur_total - prev_total) / prev_total * 100) if prev_total > 0 else 0.0

    return {'value': cur_total, 'previous': prev_total, 'growth_pct': growth}


def calc_expenses(invoices_in: pd.DataFrame, start: datetime, end: datetime) -> Dict[str, Any]:
    """Total expenses from incoming invoices (purchases) in period."""
    df = filter_signed(invoices_in)
    current = filter_by_date_range(df, 'Document Date', start, end)
    period_len = (end - start).days
    prev_start = start - timedelta(days=period_len)
    previous = filter_by_date_range(df, 'Document Date', prev_start, start)

    cur_total = safe_sum(current, 'Supply Value (incl. VAT)')
    prev_total = safe_sum(previous, 'Supply Value (incl. VAT)')
    growth = ((cur_total - prev_total) / prev_total * 100) if prev_total > 0 else 0.0

    return {'value': cur_total, 'previous': prev_total, 'growth_pct': growth}


def calc_gross_profit_margin(revenue: float, expenses: float) -> float:
    """Gross profit margin percentage."""
    if revenue <= 0:
        return 0.0
    return (revenue - expenses) / revenue * 100


def calc_ar_total(recon_ar: pd.DataFrame, invoices_out: pd.DataFrame = None,
                   bank_txns: pd.DataFrame = None) -> float:
    """
    Total accounts receivable.
    Priority: reconciliation data, then invoices minus bank payments.
    Returns None if no reliable data source is available.
    """
    recon_total = safe_sum(recon_ar, 'Outstanding_Amount')
    if recon_total > 0:
        return recon_total

    # Estimate AR from invoices minus collections (only if bank data exists)
    if invoices_out is not None and not invoices_out.empty:
        if bank_txns is not None and not bank_txns.empty:
            df = filter_signed(invoices_out)
            invoiced = safe_sum(df, 'Supply Value (incl. VAT)')
            collected = safe_sum(bank_txns, 'Credit Turnover')
            return max(invoiced - collected, 0.0)

    return None


def calc_ap_total(recon_ap: pd.DataFrame, invoices_in: pd.DataFrame = None,
                   bank_txns: pd.DataFrame = None) -> float:
    """
    Total accounts payable.
    Priority: reconciliation data, then purchases minus bank payments.
    Returns None if no reliable data source is available.
    """
    recon_total = safe_sum(recon_ap, 'Outstanding_Amount')
    if recon_total > 0:
        return recon_total

    # Estimate AP from purchases minus payments (only if bank data exists)
    if invoices_in is not None and not invoices_in.empty:
        if bank_txns is not None and not bank_txns.empty:
            df = filter_signed(invoices_in)
            purchased = safe_sum(df, 'Supply Value (incl. VAT)')
            paid = safe_sum(bank_txns, 'Debit Turnover')
            return max(purchased - paid, 0.0)

    return None


def calc_net_position(ar: float, ap: float) -> float:
    """Working capital = AR - AP."""
    return ar - ap


def calc_current_ratio(ar: float, ap: float) -> float:
    """Current ratio = AR / AP."""
    if ap <= 0:
        return float('inf') if ar > 0 else 0.0
    return ar / ap


def calc_dso(invoices_out: pd.DataFrame, bank_txns: pd.DataFrame, start: datetime, end: datetime) -> float:
    """
    Days Sales Outstanding - average days to get paid.
    Simplified: (AR / Revenue) * days_in_period
    """
    df = filter_signed(invoices_out)
    period = filter_by_date_range(df, 'Document Date', start, end)
    revenue = safe_sum(period, 'Supply Value (incl. VAT)')
    if revenue <= 0:
        return 0.0

    # Use bank inflows as proxy for collections
    if bank_txns is not None and not bank_txns.empty:
        bank_period = filter_by_date_range(bank_txns, 'date', start, end)
        collections = safe_sum(bank_period, 'Credit Turnover')
    else:
        collections = revenue * 0.8  # assume 80% collected

    days_in_period = max((end - start).days, 1)
    ar_estimate = revenue - collections
    if ar_estimate < 0:
        ar_estimate = 0
    return (ar_estimate / revenue) * days_in_period if revenue > 0 else 0.0


def calc_dpo(invoices_in: pd.DataFrame, bank_txns: pd.DataFrame, start: datetime, end: datetime) -> float:
    """
    Days Payable Outstanding - average days to pay suppliers.
    Simplified: (AP / Purchases) * days_in_period
    """
    df = filter_signed(invoices_in)
    period = filter_by_date_range(df, 'Document Date', start, end)
    purchases = safe_sum(period, 'Supply Value (incl. VAT)')
    if purchases <= 0:
        return 0.0

    if bank_txns is not None and not bank_txns.empty:
        bank_period = filter_by_date_range(bank_txns, 'date', start, end)
        payments = safe_sum(bank_period, 'Debit Turnover')
    else:
        payments = purchases * 0.8

    days_in_period = max((end - start).days, 1)
    ap_estimate = purchases - payments
    if ap_estimate < 0:
        ap_estimate = 0
    return (ap_estimate / purchases) * days_in_period if purchases > 0 else 0.0


def calc_cash_conversion_cycle(dso: float, dpo: float) -> float:
    """Cash conversion cycle = DSO - DPO."""
    return dso - dpo


def calc_active_counterparties(invoices_out: pd.DataFrame, invoices_in: pd.DataFrame,
                                start: datetime, end: datetime) -> Dict[str, int]:
    """Count active buyers and suppliers in period."""
    out_df = filter_signed(invoices_out)
    in_df = filter_signed(invoices_in)

    out_period = filter_by_date_range(out_df, 'Document Date', start, end)
    in_period = filter_by_date_range(in_df, 'Document Date', start, end)

    buyers = out_period['Buyer (Tax ID or PINFL)'].nunique() if not out_period.empty and 'Buyer (Tax ID or PINFL)' in out_period.columns else 0
    suppliers = in_period['Seller (Tax ID or PINFL)'].nunique() if not in_period.empty and 'Seller (Tax ID or PINFL)' in in_period.columns else 0

    return {'buyers': buyers, 'suppliers': suppliers}


def calc_overdue_ar(invoices_out: pd.DataFrame, bank_txns: pd.DataFrame,
                     due_days: int = 30) -> Dict[str, Any]:
    """
    Estimate overdue AR: invoices past due_days that haven't been fully paid.
    Simplified approach using invoice age.
    """
    df = filter_signed(invoices_out)
    if df is None or df.empty:
        return {'total': 0.0, 'count': 0}

    df = df.copy()
    df['Document Date'] = pd.to_datetime(df['Document Date'], errors='coerce')
    df['days_old'] = (datetime.now() - df['Document Date']).dt.days
    overdue = df[df['days_old'] > due_days]

    return {
        'total': safe_sum(overdue, 'Supply Value (incl. VAT)'),
        'count': len(overdue)
    }


def calc_concentration_risk(invoices_out: pd.DataFrame, start: datetime, end: datetime,
                             top_n: int = 3) -> Dict[str, Any]:
    """
    Concentration risk: % of revenue from top N clients.
    """
    df = filter_signed(invoices_out)
    period = filter_by_date_range(df, 'Document Date', start, end)

    if period.empty or 'Buyer (Tax ID or PINFL)' not in period.columns:
        return {'top_n_pct': 0.0, 'top_n': top_n, 'total_buyers': 0}

    period['amt'] = pd.to_numeric(period['Supply Value (incl. VAT)'], errors='coerce').fillna(0)
    by_buyer = period.groupby('Buyer (Tax ID or PINFL)')['amt'].sum().sort_values(ascending=False)
    total = by_buyer.sum()
    top_sum = by_buyer.head(top_n).sum()

    return {
        'top_n_pct': (top_sum / total * 100) if total > 0 else 0.0,
        'top_n': top_n,
        'total_buyers': len(by_buyer)
    }


def calc_bank_activity(bank_txns: pd.DataFrame, start: datetime, end: datetime) -> Dict[str, Any]:
    """Bank transaction volume in period."""
    if bank_txns is None or bank_txns.empty:
        return {'total_txns': 0, 'inflow': 0.0, 'outflow': 0.0}

    period = filter_by_date_range(bank_txns, 'date', start, end)
    return {
        'total_txns': len(period),
        'inflow': safe_sum(period, 'Credit Turnover'),
        'outflow': safe_sum(period, 'Debit Turnover'),
    }


# ---------------------------------------------------------------------------
# Charts / aggregations
# ---------------------------------------------------------------------------

def monthly_revenue_trend(invoices_out: pd.DataFrame, months: int = 12) -> pd.DataFrame:
    """Monthly revenue trend for last N months."""
    df = filter_signed(invoices_out)
    if df is None or df.empty:
        return pd.DataFrame(columns=['month', 'revenue'])

    df = df.copy()
    df['Document Date'] = pd.to_datetime(df['Document Date'], errors='coerce')
    df['amt'] = pd.to_numeric(df['Supply Value (incl. VAT)'], errors='coerce').fillna(0)

    cutoff = datetime.now() - timedelta(days=months * 30)
    df = df[df['Document Date'] >= cutoff]

    df['month'] = df['Document Date'].dt.to_period('M').dt.to_timestamp()
    monthly = df.groupby('month')['amt'].sum().reset_index()
    monthly.columns = ['month', 'revenue']
    return monthly.sort_values('month')


def top_buyers(invoices_out: pd.DataFrame, start: datetime, end: datetime, n: int = 10) -> pd.DataFrame:
    """Top N buyers by revenue in period."""
    df = filter_signed(invoices_out)
    period = filter_by_date_range(df, 'Document Date', start, end)

    if period.empty:
        return pd.DataFrame(columns=['buyer', 'inn', 'total'])

    period['amt'] = pd.to_numeric(period['Supply Value (incl. VAT)'], errors='coerce').fillna(0)
    grouped = period.groupby(['Buyer (Name)', 'Buyer (Tax ID or PINFL)'])['amt'].sum().reset_index()
    grouped.columns = ['buyer', 'inn', 'total']
    return grouped.sort_values('total', ascending=False).head(n)


def top_suppliers(invoices_in: pd.DataFrame, start: datetime, end: datetime, n: int = 10) -> pd.DataFrame:
    """Top N suppliers by spend in period."""
    df = filter_signed(invoices_in)
    period = filter_by_date_range(df, 'Document Date', start, end)

    if period.empty:
        return pd.DataFrame(columns=['supplier', 'inn', 'total'])

    period['amt'] = pd.to_numeric(period['Supply Value (incl. VAT)'], errors='coerce').fillna(0)
    grouped = period.groupby(['Seller (Name)', 'Seller (Tax ID or PINFL)'])['amt'].sum().reset_index()
    grouped.columns = ['supplier', 'inn', 'total']
    return grouped.sort_values('total', ascending=False).head(n)


def ar_aging_buckets(recon_ar: pd.DataFrame, invoices_out: pd.DataFrame) -> pd.DataFrame:
    """
    AR aging breakdown by bucket.
    Uses invoice dates to estimate aging of outstanding amounts.
    """
    df = filter_signed(invoices_out)
    if df is None or df.empty:
        return pd.DataFrame(columns=['bucket', 'amount', 'count'])

    df = df.copy()
    df['Document Date'] = pd.to_datetime(df['Document Date'], errors='coerce')
    df['days_old'] = (datetime.now() - df['Document Date']).dt.days
    df['amt'] = pd.to_numeric(df['Supply Value (incl. VAT)'], errors='coerce').fillna(0)

    buckets = [
        ('0-30 days', 0, 30),
        ('31-60 days', 31, 60),
        ('61-90 days', 61, 90),
        ('91-180 days', 91, 180),
        ('180+ days', 181, 999999),
    ]

    rows = []
    for name, lo, hi in buckets:
        subset = df[(df['days_old'] >= lo) & (df['days_old'] <= hi)]
        rows.append({'bucket': name, 'amount': subset['amt'].sum(), 'count': len(subset)})

    return pd.DataFrame(rows)


def ap_aging_buckets(invoices_in: pd.DataFrame) -> pd.DataFrame:
    """AP aging breakdown by bucket."""
    df = filter_signed(invoices_in)
    if df is None or df.empty:
        return pd.DataFrame(columns=['bucket', 'amount', 'count'])

    df = df.copy()
    df['Document Date'] = pd.to_datetime(df['Document Date'], errors='coerce')
    df['days_old'] = (datetime.now() - df['Document Date']).dt.days
    df['amt'] = pd.to_numeric(df['Supply Value (incl. VAT)'], errors='coerce').fillna(0)

    buckets = [
        ('0-30 days', 0, 30),
        ('31-60 days', 31, 60),
        ('61-90 days', 61, 90),
        ('91-180 days', 91, 180),
        ('180+ days', 181, 999999),
    ]

    rows = []
    for name, lo, hi in buckets:
        subset = df[(df['days_old'] >= lo) & (df['days_old'] <= hi)]
        rows.append({'bucket': name, 'amount': subset['amt'].sum(), 'count': len(subset)})

    return pd.DataFrame(rows)


def counterparty_summary(invoices_out: pd.DataFrame, invoices_in: pd.DataFrame,
                          recon_ar: pd.DataFrame, recon_ap: pd.DataFrame,
                          start: datetime, end: datetime) -> pd.DataFrame:
    """
    Unified counterparty table: name, type (buyer/supplier/both), volume, outstanding, invoice count.
    """
    rows = []

    # Buyers
    out_df = filter_signed(invoices_out)
    out_period = filter_by_date_range(out_df, 'Document Date', start, end)
    if not out_period.empty and 'Buyer (Tax ID or PINFL)' in out_period.columns:
        out_period['amt'] = pd.to_numeric(out_period['Supply Value (incl. VAT)'], errors='coerce').fillna(0)
        buyer_agg = out_period.groupby(['Buyer (Name)', 'Buyer (Tax ID or PINFL)']).agg(
            volume=('amt', 'sum'),
            invoice_count=('amt', 'count')
        ).reset_index()
        buyer_agg.columns = ['name', 'inn', 'volume', 'invoice_count']
        buyer_agg['type'] = 'Buyer'
        rows.append(buyer_agg)

    # Suppliers
    in_df = filter_signed(invoices_in)
    in_period = filter_by_date_range(in_df, 'Document Date', start, end)
    if not in_period.empty and 'Seller (Tax ID or PINFL)' in in_period.columns:
        in_period['amt'] = pd.to_numeric(in_period['Supply Value (incl. VAT)'], errors='coerce').fillna(0)
        seller_agg = in_period.groupby(['Seller (Name)', 'Seller (Tax ID or PINFL)']).agg(
            volume=('amt', 'sum'),
            invoice_count=('amt', 'count')
        ).reset_index()
        seller_agg.columns = ['name', 'inn', 'volume', 'invoice_count']
        seller_agg['type'] = 'Supplier'
        rows.append(seller_agg)

    if not rows:
        return pd.DataFrame(columns=['name', 'inn', 'type', 'volume', 'invoice_count', 'outstanding'])

    result = pd.concat(rows, ignore_index=True)

    # Merge outstanding amounts from reconciliation
    if recon_ar is not None and not recon_ar.empty:
        ar_outstanding = recon_ar.groupby('Customer_INN')['Outstanding_Amount'].sum().reset_index()
        ar_outstanding.columns = ['inn', 'ar_outstanding']
        result = result.merge(ar_outstanding, on='inn', how='left')
    else:
        result['ar_outstanding'] = 0

    if recon_ap is not None and not recon_ap.empty:
        ap_outstanding = recon_ap.groupby('Customer_INN')['Outstanding_Amount'].sum().reset_index()
        ap_outstanding.columns = ['inn', 'ap_outstanding']
        result = result.merge(ap_outstanding, on='inn', how='left')
    else:
        result['ap_outstanding'] = 0

    result['ar_outstanding'] = result['ar_outstanding'].fillna(0)
    result['ap_outstanding'] = result['ap_outstanding'].fillna(0)
    result['outstanding'] = result['ar_outstanding'] + result['ap_outstanding']

    # Combine duplicates (same INN appearing as both buyer and supplier)
    combined = result.groupby(['name', 'inn']).agg({
        'type': lambda x: 'Both' if len(set(x)) > 1 else x.iloc[0],
        'volume': 'sum',
        'invoice_count': 'sum',
        'outstanding': 'max',
    }).reset_index()

    return combined.sort_values('volume', ascending=False)


def generate_alerts(revenue_data: Dict, ar_total: float, ap_total: float,
                     overdue_data: Dict, concentration: Dict,
                     dso: float, health_rating: Dict) -> List[Dict[str, str]]:
    """Generate actionable alerts for the business owner."""
    alerts = []

    # Overdue AR
    if overdue_data['count'] > 0:
        alerts.append({
            'severity': 'warning',
            'message': f"{overdue_data['count']} invoices overdue (total: {format_currency(overdue_data['total'], '')})"
        })

    # Concentration risk
    if concentration['top_n_pct'] > 60:
        alerts.append({
            'severity': 'warning',
            'message': f"Top {concentration['top_n']} clients account for {concentration['top_n_pct']:.0f}% of revenue"
        })

    # Revenue decline
    if revenue_data['growth_pct'] < -10:
        alerts.append({
            'severity': 'danger',
            'message': f"Revenue declined {abs(revenue_data['growth_pct']):.1f}% vs previous period"
        })
    elif revenue_data['growth_pct'] > 10:
        alerts.append({
            'severity': 'success',
            'message': f"Revenue grew {revenue_data['growth_pct']:.1f}% vs previous period"
        })

    # Cash position (only if AR/AP data available)
    if ar_total is not None and ap_total is not None:
        net = ar_total - ap_total
        if net < 0:
            alerts.append({
                'severity': 'danger',
                'message': f"Negative net position: you owe more than you're owed"
            })

    # High DSO
    if dso > 60:
        alerts.append({
            'severity': 'warning',
            'message': f"DSO is {dso:.0f} days - slow collections"
        })

    # Health rating
    if health_rating.get('rating') in ('C', 'C+', 'D'):
        alerts.append({
            'severity': 'danger',
            'message': f"Company health rating is {health_rating.get('rating')} - needs attention"
        })

    if not alerts:
        alerts.append({
            'severity': 'success',
            'message': 'All indicators look healthy'
        })

    return alerts
