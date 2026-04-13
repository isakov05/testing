"""
Dashboard metrics calculations for financial dashboard
Provides company health rating, outliers, legal cases, cash flow projection, and recent activity metrics
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import streamlit as st
from .db_helper import get_db_connection, get_db_engine
from .db_operations import load_user_invoices, load_user_bank_transactions


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def assign_health_rating_grade(score: float) -> str:
    """
    Convert numeric health score (0-100) to letter grade

    Args:
        score: Health score from 0-100 (higher is better)

    Returns:
        Letter grade: A+, A, B+, B, C+, C, or D
    """
    if score >= 90:
        return 'A+'
    elif score >= 80:
        return 'A'
    elif score >= 70:
        return 'B+'
    elif score >= 60:
        return 'B'
    elif score >= 50:
        return 'C+'
    elif score >= 40:
        return 'C'
    else:
        return 'D'


def get_aging_collection_factor(days_old: int) -> float:
    """
    Return collection probability adjustment based on invoice age
    Based on industry standard aging buckets

    Args:
        days_old: Age of invoice in days

    Returns:
        Collection factor (0.0 to 1.0)
    """
    if days_old <= 30:
        return 0.95  # 95% likely to collect current invoices
    elif days_old <= 60:
        return 0.80  # 80% for 30-60 days
    elif days_old <= 90:
        return 0.60  # 60% for 60-90 days
    else:
        return 0.30  # 30% for 90+ days


def clean_inn(inn_value) -> Optional[str]:
    """
    Clean and standardize INN value

    Args:
        inn_value: INN value (can be str, int, float, or None)

    Returns:
        Cleaned INN string or None
    """
    if inn_value is None or pd.isna(inn_value):
        return None

    inn_str = str(inn_value).replace('.0', '').strip()
    if inn_str.lower() == 'nan' or inn_str == '':
        return None

    return inn_str


def find_amount_column(df: pd.DataFrame) -> Optional[str]:
    """
    Find amount column in DataFrame (handles multiple possible names)

    Args:
        df: DataFrame to search

    Returns:
        Column name or None if not found
    """
    possible_names = [
        'Supply Value (incl. VAT)',
        'Total Amount',
        'total_amount',
        'amount'
    ]

    for name in possible_names:
        if name in df.columns:
            return name

    return None


def filter_signed_invoices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter DataFrame to only include signed/confirmed invoices

    Args:
        df: Invoice DataFrame

    Returns:
        Filtered DataFrame
    """
    if 'Status' not in df.columns:
        return df

    return df[df['Status'].isin(['Подписан', 'Signed', 'signed'])]


# ============================================================================
# MAIN METRIC FUNCTIONS
# ============================================================================

@st.cache_data(ttl=300)  # Cache for 5 minutes
def calculate_company_health_rating(user_id: str) -> Dict[str, Any]:
    """
    Calculate overall company health rating based on:
    - PD (Probability of Default) score
    - Recent legal cases count
    - Outlier transaction frequency

    Args:
        user_id: User identifier

    Returns:
        Dictionary with:
        - rating: Letter grade (A+ to D)
        - score: Numeric score (0-100)
        - pd_component: PD score contribution
        - legal_component: Legal cases penalty
        - outlier_component: Outlier transactions penalty
        - explanation: List of reasons for rating
        - trend: 'improving', 'stable', or 'declining'
    """
    try:
        # Step 1: Get base PD score (simplified - use average of invoice aging)
        # In production, this would come from the risk engine
        pd_score = get_simplified_pd_score(user_id)

        # Step 2: Get legal cases in last 30 days
        recent_cases_count = get_recent_legal_cases_count(user_id, days=30)
        legal_penalty = min(recent_cases_count * 5, 30)  # Max 30 points penalty

        # Step 3: Get outlier transactions in last 30 days
        outliers = get_recent_outliers(user_id, days=30)
        outlier_count = len(outliers['top_invoices']) + len(outliers['top_payments'])
        outlier_penalty = min(outlier_count * 2, 20)  # Max 20 points penalty

        # Step 4: Calculate composite score (100 = best, 0 = worst)
        base_score = 100 - pd_score  # Invert PD (lower PD = higher health)
        final_score = max(0, base_score - legal_penalty - outlier_penalty)

        # Step 5: Assign rating grade
        rating = assign_health_rating_grade(final_score)

        # Step 6: Generate explanation
        explanations = []
        if legal_penalty > 0:
            explanations.append(f"{recent_cases_count} legal case(s) in last 30 days (-{legal_penalty} points)")
        if outlier_penalty > 0:
            explanations.append(f"{outlier_count} outlier transaction(s) detected (-{outlier_penalty} points)")
        if pd_score > 50:
            explanations.append(f"High credit risk score indicates elevated default probability")
        elif pd_score < 20:
            explanations.append(f"Low credit risk score indicates strong payment behavior")

        # Step 7: Calculate trend (compare with 60 days ago)
        historical_score = calculate_historical_health_score(user_id, days_ago=60)
        score_change = final_score - historical_score

        if score_change > 5:
            trend = 'improving'
            explanations.append(f"Score improved by {score_change:.1f} points in last 60 days")
        elif score_change < -5:
            trend = 'declining'
            explanations.append(f"Score declined by {abs(score_change):.1f} points in last 60 days")
        else:
            trend = 'stable'

        return {
            'rating': rating,
            'score': final_score,
            'pd_component': pd_score,
            'legal_component': legal_penalty,
            'outlier_component': outlier_penalty,
            'explanation': explanations,
            'trend': trend,
            'historical_score': historical_score,
            'score_change': score_change
        }

    except Exception as e:
        print(f"Error calculating company health rating: {str(e)}")
        import traceback
        traceback.print_exc()

        # Return default values on error
        return {
            'rating': 'N/A',
            'score': 0,
            'pd_component': 0,
            'legal_component': 0,
            'outlier_component': 0,
            'explanation': [f"Error calculating rating: {str(e)}"],
            'trend': 'stable',
            'historical_score': 0,
            'score_change': 0
        }


def get_simplified_pd_score(user_id: str) -> float:
    """
    Calculate simplified PD score based on invoice aging
    Returns score from 0-100 (higher = more risky)

    Args:
        user_id: User identifier

    Returns:
        PD score (0-100)
    """
    try:
        invoices = load_user_invoices(user_id, invoice_type='OUT')
        if invoices.empty:
            return 50  # Neutral score if no data

        invoices = filter_signed_invoices(invoices)
        if invoices.empty:
            return 50

        # Calculate average days outstanding
        invoices['Document Date'] = pd.to_datetime(invoices['Document Date'])
        invoices['days_old'] = (datetime.now() - invoices['Document Date']).dt.days

        avg_days_old = invoices['days_old'].mean()

        # Convert to PD score (0-30 days = low risk, 90+ days = high risk)
        if avg_days_old <= 30:
            return 10  # Very low risk
        elif avg_days_old <= 60:
            return 25  # Low risk
        elif avg_days_old <= 90:
            return 50  # Medium risk
        else:
            return 75  # High risk

    except Exception as e:
        print(f"Error calculating PD score: {str(e)}")
        return 50  # Neutral score on error


def get_recent_legal_cases_count(user_id: str, days: int = 30) -> int:
    """
    Count recent legal cases filed in last N days

    Args:
        user_id: User identifier
        days: Number of days to look back

    Returns:
        Count of legal cases
    """
    try:
        engine = get_db_engine()
        cutoff_date = datetime.now() - timedelta(days=days)

        # Get user's INN first (simplified - would need to get from user table)
        # For now, return 0 as we don't have court cases linked to user
        query = """
            SELECT COUNT(*) as case_count
            FROM company_court_cases
            WHERE filing_date >= %(cutoff_date)s
        """

        result = pd.read_sql_query(query, engine, params={'cutoff_date': cutoff_date})
        return 0  # Simplified - return 0 for now

    except Exception as e:
        print(f"Error counting legal cases: {str(e)}")
        return 0


def calculate_historical_health_score(user_id: str, days_ago: int = 60) -> float:
    """
    Calculate health score from N days ago for trend analysis

    Args:
        user_id: User identifier
        days_ago: Number of days to look back

    Returns:
        Historical score (0-100)
    """
    try:
        # Simplified version - just return current score minus random variation
        # In production, this would query historical data
        current = get_simplified_pd_score(user_id)
        base_score = 100 - current

        # Return same score for now (stable trend)
        return base_score

    except Exception as e:
        print(f"Error calculating historical score: {str(e)}")
        return 50


@st.cache_data(ttl=300)
def get_recent_outliers(user_id: str, days: int = 30) -> Dict[str, pd.DataFrame]:
    """
    Identify outlier transactions using statistical methods (Z-score > 3 OR top 5)

    Args:
        user_id: User identifier
        days: Number of days to look back (default 30)

    Returns:
        Dictionary with:
        - top_invoices: DataFrame of top 5 largest invoices
        - top_payments: DataFrame of top 5 largest payments
        - recent_legal_cases: DataFrame of recent legal cases
    """
    try:
        cutoff_date = datetime.now() - timedelta(days=days)

        # Step 1: Get invoice outliers
        invoices = load_user_invoices(user_id)
        if not invoices.empty:
            invoices = filter_signed_invoices(invoices)
            invoices['Document Date'] = pd.to_datetime(invoices['Document Date'])
            recent_invoices = invoices[invoices['Document Date'] >= cutoff_date].copy()

            if not recent_invoices.empty:
                amount_col = find_amount_column(recent_invoices)
                if amount_col:
                    amounts = recent_invoices[amount_col]
                    mean = amounts.mean()
                    std = amounts.std()

                    # Calculate Z-score
                    if std > 0:
                        recent_invoices['z_score'] = (amounts - mean) / std
                    else:
                        recent_invoices['z_score'] = 0

                    # Get top 5 or outliers (Z > 3)
                    outlier_invoices = recent_invoices[
                        (recent_invoices['z_score'].abs() > 3) |
                        (recent_invoices[amount_col].rank(ascending=False) <= 5)
                    ].sort_values(amount_col, ascending=False).head(5)

                    # Select display columns
                    display_cols = ['Document Number', 'Document Date', 'Buyer (Name)', amount_col, 'z_score']
                    outlier_invoices = outlier_invoices[[col for col in display_cols if col in outlier_invoices.columns]]
                else:
                    outlier_invoices = pd.DataFrame()
            else:
                outlier_invoices = pd.DataFrame()
        else:
            outlier_invoices = pd.DataFrame()

        # Step 2: Get payment outliers
        payments = load_user_bank_transactions(user_id)
        if not payments.empty:
            payments['date'] = pd.to_datetime(payments['date'], errors='coerce')
            recent_payments = payments[payments['date'] >= cutoff_date].copy()

            if not recent_payments.empty:
                # Use absolute amount
                if 'amount' in recent_payments.columns:
                    recent_payments['abs_amount'] = recent_payments['amount'].abs()
                    amounts_pay = recent_payments['abs_amount']
                    mean_pay = amounts_pay.mean()
                    std_pay = amounts_pay.std()

                    # Calculate Z-score
                    if std_pay > 0:
                        recent_payments['z_score'] = (amounts_pay - mean_pay) / std_pay
                    else:
                        recent_payments['z_score'] = 0

                    # Get top 5 or outliers
                    outlier_payments = recent_payments[
                        (recent_payments['z_score'].abs() > 3) |
                        (recent_payments['abs_amount'].rank(ascending=False) <= 5)
                    ].sort_values('abs_amount', ascending=False).head(5)

                    # Select display columns
                    display_cols = ['date', 'Taxpayer ID (INN)', 'Account Name', 'amount', 'z_score']
                    outlier_payments = outlier_payments[[col for col in display_cols if col in outlier_payments.columns]]
                else:
                    outlier_payments = pd.DataFrame()
            else:
                outlier_payments = pd.DataFrame()
        else:
            outlier_payments = pd.DataFrame()

        # Step 3: Get recent legal cases (placeholder)
        recent_cases = pd.DataFrame()  # Would query company_court_cases table

        return {
            'top_invoices': outlier_invoices,
            'top_payments': outlier_payments,
            'recent_legal_cases': recent_cases
        }

    except Exception as e:
        print(f"Error getting outliers: {str(e)}")
        import traceback
        traceback.print_exc()

        return {
            'top_invoices': pd.DataFrame(),
            'top_payments': pd.DataFrame(),
            'recent_legal_cases': pd.DataFrame()
        }


@st.cache_data(ttl=300)
def get_counterparty_legal_cases(user_id: str, days: int = 30) -> pd.DataFrame:
    """
    Get all legal cases involving user's counterparties in the last N days

    Args:
        user_id: User identifier
        days: Number of days to look back (default 30)

    Returns:
        DataFrame with legal cases and counterparty information
    """
    try:
        # Step 1: Get all counterparty INNs
        counterparty_inns = get_all_counterparty_inns(user_id)

        if not counterparty_inns:
            return pd.DataFrame()

        # Step 2: Query legal cases
        engine = get_db_engine()
        cutoff_date = datetime.now() - timedelta(days=days)

        query = """
            SELECT
                cc.inn,
                cc.case_number,
                cc.case_type,
                cc.court_name,
                cc.plaintiff_name,
                cc.plaintiff_inn,
                cc.defendant_name,
                cc.defendant_inn,
                cc.filing_date,
                cc.claim_amount,
                cc.case_status,
                ci.company_name,
                ci.city,
                ci.region
            FROM company_court_cases cc
            LEFT JOIN company_info ci ON cc.inn = ci.inn
            WHERE cc.inn = ANY(%(inn_list)s)
              AND cc.filing_date >= %(cutoff_date)s
            ORDER BY cc.filing_date DESC
        """

        cases = pd.read_sql_query(query, engine, params={
            'inn_list': counterparty_inns,
            'cutoff_date': cutoff_date
        })

        # Step 3: Add role (plaintiff or defendant)
        if not cases.empty:
            def determine_role(row):
                if str(row['plaintiff_inn']) in counterparty_inns:
                    return 'Plaintiff'
                elif str(row['defendant_inn']) in counterparty_inns:
                    return 'Defendant'
                return 'Unknown'

            cases['role'] = cases.apply(determine_role, axis=1)

            # Step 4: Add exposure amount (simplified)
            cases['our_exposure'] = 0  # Would calculate from invoices

        return cases

    except Exception as e:
        print(f"Error getting counterparty legal cases: {str(e)}")
        import traceback
        traceback.print_exc()

        return pd.DataFrame()


def get_all_counterparty_inns(user_id: str) -> List[str]:
    """
    Get unique list of all counterparty INNs from invoices and bank transactions

    Args:
        user_id: User identifier

    Returns:
        List of unique INN strings
    """
    try:
        engine = get_db_engine()
        user_id = str(user_id)

        query = """
            SELECT DISTINCT buyer_inn AS inn FROM invoices
            WHERE user_id = %(user_id)s AND invoice_type = 'OUT' AND buyer_inn IS NOT NULL
            UNION
            SELECT DISTINCT seller_inn AS inn FROM invoices
            WHERE user_id = %(user_id)s AND invoice_type = 'IN' AND seller_inn IS NOT NULL
            UNION
            SELECT DISTINCT counterparty_inn AS inn FROM bank_transactions
            WHERE user_id = %(user_id)s AND counterparty_inn IS NOT NULL
        """

        result = pd.read_sql_query(query, engine, params={'user_id': user_id})

        # Clean INNs
        inns = result['inn'].apply(clean_inn).dropna().unique().tolist()

        return inns

    except Exception as e:
        print(f"Error getting counterparty INNs: {str(e)}")
        return []


@st.cache_data(ttl=300)
def calculate_cash_flow_projection(user_id: str, horizon_days: int = 30) -> Dict[str, Any]:
    """
    Project cash flow for next N days based on:
    - Expected inflow: Recent invoices + probability-weighted collections
    - Expected outflow: Days payable due

    Args:
        user_id: User identifier
        horizon_days: Number of days to project forward (default 30)

    Returns:
        Dictionary with:
        - expected_inflow: Projected cash inflow
        - expected_outflow: Projected cash outflow
        - net_cash_flow: Net cash flow (inflow - outflow)
        - confidence_level: 'high', 'medium', or 'low'
        - breakdown: Detailed calculation breakdown
    """
    try:
        # Step 1: Get invoices created in last 30 days (OUT = sales/AR)
        invoices_out = load_user_invoices(user_id, invoice_type='OUT')
        cutoff_date = datetime.now() - timedelta(days=30)

        if not invoices_out.empty:
            invoices_out = filter_signed_invoices(invoices_out)
            invoices_out['Document Date'] = pd.to_datetime(invoices_out['Document Date'])
            recent_invoices = invoices_out[invoices_out['Document Date'] >= cutoff_date].copy()

            amount_col = find_amount_column(recent_invoices)
            if amount_col and not recent_invoices.empty:
                total_invoiced = recent_invoices[amount_col].sum()

                # Step 2: Calculate probability-weighted collections
                weighted_collections = 0
                for _, invoice in recent_invoices.iterrows():
                    amount = invoice[amount_col]

                    # Get invoice age
                    days_old = (datetime.now() - invoice['Document Date']).days
                    aging_factor = get_aging_collection_factor(days_old)

                    # Simplified PD (would get per-customer in production)
                    collection_probability = 0.85  # 85% default collection rate

                    weighted_amount = amount * collection_probability * aging_factor
                    weighted_collections += weighted_amount
            else:
                total_invoiced = 0
                weighted_collections = 0
        else:
            total_invoiced = 0
            weighted_collections = 0

        # Step 3: Get payables due in next N days (IN = purchases/AP)
        invoices_in = load_user_invoices(user_id, invoice_type='IN')

        if not invoices_in.empty:
            invoices_in = filter_signed_invoices(invoices_in)
            invoices_in['Document Date'] = pd.to_datetime(invoices_in['Document Date'])

            # Assume 30-day payment terms
            invoices_in['due_date'] = invoices_in['Document Date'] + pd.Timedelta(days=30)
            future_date = datetime.now() + timedelta(days=horizon_days)

            amount_col = find_amount_column(invoices_in)
            if amount_col:
                ap_due = invoices_in[
                    (invoices_in['due_date'] >= datetime.now()) &
                    (invoices_in['due_date'] <= future_date)
                ][amount_col].sum()
            else:
                ap_due = 0
        else:
            ap_due = 0

        # Step 4: Calculate net cash flow
        net_cf = weighted_collections - ap_due

        # Step 5: Assess confidence level
        if not invoices_out.empty and len(recent_invoices) >= 30:
            confidence = 'high'
        elif not invoices_out.empty and len(recent_invoices) >= 10:
            confidence = 'medium'
        else:
            confidence = 'low'

        return {
            'expected_inflow': weighted_collections,
            'expected_outflow': ap_due,
            'net_cash_flow': net_cf,
            'confidence_level': confidence,
            'breakdown': {
                'invoiced_ar': total_invoiced,
                'weighted_collections': weighted_collections,
                'collection_adjustment': total_invoiced - weighted_collections,
                'ap_due': ap_due
            }
        }

    except Exception as e:
        print(f"Error calculating cash flow projection: {str(e)}")
        import traceback
        traceback.print_exc()

        return {
            'expected_inflow': 0,
            'expected_outflow': 0,
            'net_cash_flow': 0,
            'confidence_level': 'low',
            'breakdown': {
                'invoiced_ar': 0,
                'weighted_collections': 0,
                'collection_adjustment': 0,
                'ap_due': 0
            }
        }


@st.cache_data(ttl=300)
def get_recent_invoice_metrics(user_id: str, days: int = 7) -> Dict[str, Dict[str, Any]]:
    """
    Calculate invoice metrics for last N days

    Args:
        user_id: User identifier
        days: Number of days to look back (default 7)

    Returns:
        Dictionary with sales_invoices and purchase_invoices metrics
    """
    try:
        cutoff_date = datetime.now() - timedelta(days=days)
        prev_cutoff_date = cutoff_date - timedelta(days=days)

        # Sales (OUT) invoices
        invoices_out = load_user_invoices(user_id, invoice_type='OUT')
        if not invoices_out.empty:
            invoices_out = filter_signed_invoices(invoices_out)
            invoices_out['Document Date'] = pd.to_datetime(invoices_out['Document Date'])

            recent_out = invoices_out[invoices_out['Document Date'] >= cutoff_date]
            prev_out = invoices_out[
                (invoices_out['Document Date'] >= prev_cutoff_date) &
                (invoices_out['Document Date'] < cutoff_date)
            ]

            amount_col = find_amount_column(recent_out)
            if amount_col:
                sales_total = recent_out[amount_col].sum()
                sales_avg = recent_out[amount_col].mean() if len(recent_out) > 0 else 0

                prev_total = prev_out[amount_col].sum() if not prev_out.empty else 0
                if prev_total > 0:
                    trend = 'up' if sales_total > prev_total else 'down' if sales_total < prev_total else 'stable'
                else:
                    trend = 'stable'
            else:
                sales_total = 0
                sales_avg = 0
                trend = 'stable'

            sales_metrics = {
                'count': len(recent_out),
                'total_amount': sales_total,
                'avg_amount': sales_avg,
                'trend': trend
            }
        else:
            sales_metrics = {
                'count': 0,
                'total_amount': 0,
                'avg_amount': 0,
                'trend': 'stable'
            }

        # Purchase (IN) invoices
        invoices_in = load_user_invoices(user_id, invoice_type='IN')
        if not invoices_in.empty:
            invoices_in = filter_signed_invoices(invoices_in)
            invoices_in['Document Date'] = pd.to_datetime(invoices_in['Document Date'])

            recent_in = invoices_in[invoices_in['Document Date'] >= cutoff_date]
            prev_in = invoices_in[
                (invoices_in['Document Date'] >= prev_cutoff_date) &
                (invoices_in['Document Date'] < cutoff_date)
            ]

            amount_col = find_amount_column(recent_in)
            if amount_col:
                purch_total = recent_in[amount_col].sum()
                purch_avg = recent_in[amount_col].mean() if len(recent_in) > 0 else 0

                prev_total = prev_in[amount_col].sum() if not prev_in.empty else 0
                if prev_total > 0:
                    trend = 'up' if purch_total > prev_total else 'down' if purch_total < prev_total else 'stable'
                else:
                    trend = 'stable'
            else:
                purch_total = 0
                purch_avg = 0
                trend = 'stable'

            purchase_metrics = {
                'count': len(recent_in),
                'total_amount': purch_total,
                'avg_amount': purch_avg,
                'trend': trend
            }
        else:
            purchase_metrics = {
                'count': 0,
                'total_amount': 0,
                'avg_amount': 0,
                'trend': 'stable'
            }

        return {
            'sales_invoices': sales_metrics,
            'purchase_invoices': purchase_metrics
        }

    except Exception as e:
        print(f"Error getting invoice metrics: {str(e)}")
        import traceback
        traceback.print_exc()

        return {
            'sales_invoices': {'count': 0, 'total_amount': 0, 'avg_amount': 0, 'trend': 'stable'},
            'purchase_invoices': {'count': 0, 'total_amount': 0, 'avg_amount': 0, 'trend': 'stable'}
        }


@st.cache_data(ttl=300)
def get_recent_bank_metrics(user_id: str, days: int = 7) -> Dict[str, Dict[str, Any]]:
    """
    Calculate bank transaction metrics for last N days

    Args:
        user_id: User identifier
        days: Number of days to look back (default 7)

    Returns:
        Dictionary with payment_inflow and payment_outflow metrics
    """
    try:
        cutoff_date = datetime.now() - timedelta(days=days)

        bank_txns = load_user_bank_transactions(user_id)
        if bank_txns.empty:
            return {
                'payment_inflow': {'count': 0, 'total_amount': 0, 'avg_amount': 0},
                'payment_outflow': {'count': 0, 'total_amount': 0, 'avg_amount': 0}
            }

        bank_txns['date'] = pd.to_datetime(bank_txns['date'], errors='coerce')
        recent_txns = bank_txns[bank_txns['date'] >= cutoff_date]

        # Credit turnover (inflow)
        if 'Credit Turnover' in recent_txns.columns:
            inflow = recent_txns[recent_txns['Credit Turnover'] > 0]
            inflow_metrics = {
                'count': len(inflow),
                'total_amount': inflow['Credit Turnover'].sum(),
                'avg_amount': inflow['Credit Turnover'].mean() if len(inflow) > 0 else 0
            }
        else:
            inflow_metrics = {'count': 0, 'total_amount': 0, 'avg_amount': 0}

        # Debit turnover (outflow)
        if 'Debit Turnover' in recent_txns.columns:
            outflow = recent_txns[recent_txns['Debit Turnover'] > 0]
            outflow_metrics = {
                'count': len(outflow),
                'total_amount': outflow['Debit Turnover'].sum(),
                'avg_amount': outflow['Debit Turnover'].mean() if len(outflow) > 0 else 0
            }
        else:
            outflow_metrics = {'count': 0, 'total_amount': 0, 'avg_amount': 0}

        return {
            'payment_inflow': inflow_metrics,
            'payment_outflow': outflow_metrics
        }

    except Exception as e:
        print(f"Error getting bank metrics: {str(e)}")
        import traceback
        traceback.print_exc()

        return {
            'payment_inflow': {'count': 0, 'total_amount': 0, 'avg_amount': 0},
            'payment_outflow': {'count': 0, 'total_amount': 0, 'avg_amount': 0}
        }
