import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

from auth.db_authenticator import protect_page
from utils.dashboard_metrics import (
    calculate_company_health_rating,
    get_recent_outliers,
    get_counterparty_legal_cases,
    calculate_cash_flow_projection,
    get_recent_invoice_metrics,
    get_recent_bank_metrics
)
from translations import get_text

st.set_page_config(
    page_title="FLOTT Dashboard",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Protect this page - require authentication
protect_page()


def render_header():
    """Render dashboard header"""
    lang = st.session_state.get('language', 'en')
    st.title(get_text("dashboard_title", lang))
    st.caption(get_text("dashboard_subtitle", lang))


def render_company_health_card(user_id):
    """
    Render company health rating card with:
    - Large metric card (rating, score, progress bar)
    - Component breakdown (PD, legal penalty, outlier penalty)
    - Trend indicator (improving/stable/declining)
    - Expandable explanation section
    """
    lang = st.session_state.get('language', 'en')

    st.subheader("Company Health Rating")

    with st.spinner("Calculating health rating..."):
        health = calculate_company_health_rating(user_id)

    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        # Large rating display
        st.metric(
            "Health Rating",
            health['rating'],
            delta=health['trend'].capitalize() if health['trend'] != 'stable' else None,
            delta_color="normal" if health['trend'] == 'improving' else "inverse" if health['trend'] == 'declining' else "off"
        )

        # Score breakdown with progress bar
        st.progress(health['score'] / 100)
        st.caption(f"Score: {health['score']:.1f}/100")

    with col2:
        # Component breakdown
        st.write("**Components:**")
        st.caption(f"Base Score: {100 - health['pd_component']:.1f}")
        st.caption(f"Credit Risk: -{health['pd_component']:.1f}")
        if health['legal_component'] > 0:
            st.caption(f"Legal Cases: -{health['legal_component']:.1f}")
        if health['outlier_component'] > 0:
            st.caption(f"Outliers: -{health['outlier_component']:.1f}")

    with col3:
        # Trend indicator
        if health['trend'] == 'improving':
            st.success("📈 Improving")
            st.caption(f"+{health['score_change']:.1f} points")
        elif health['trend'] == 'declining':
            st.error("📉 Declining")
            st.caption(f"{health['score_change']:.1f} points")
        else:
            st.info("➡️ Stable")
            st.caption("No significant change")

    # Explanation
    if health['explanation']:
        with st.expander("Why did the rating change?"):
            for reason in health['explanation']:
                st.write(f"- {reason}")


def render_recent_outliers(user_id):
    """
    Render recent outliers section with:
    - Top 5 largest invoices (table)
    - Top 5 largest payments (table)
    - Recent legal cases (table)
    """
    lang = st.session_state.get('language', 'en')

    st.subheader("Recent Outliers (Last 30 Days)")

    with st.spinner("Loading outlier transactions..."):
        outliers = get_recent_outliers(user_id, days=30)

    col1, col2 = st.columns(2)

    with col1:
        st.write("**Top 5 Largest Invoices**")
        if not outliers['top_invoices'].empty:
            # Format for display
            display_df = outliers['top_invoices'].copy()

            # Format amounts
            if 'Supply Value (incl. VAT)' in display_df.columns:
                display_df['Amount'] = display_df['Supply Value (incl. VAT)'].apply(lambda x: f"{x:,.2f}")
                display_df = display_df.drop(columns=['Supply Value (incl. VAT)'])

            # Format Z-score
            if 'z_score' in display_df.columns:
                display_df['Z-Score'] = display_df['z_score'].apply(lambda x: f"{x:.2f}")
                display_df = display_df.drop(columns=['z_score'])

            # Format date
            if 'Document Date' in display_df.columns:
                display_df['Date'] = pd.to_datetime(display_df['Document Date']).dt.strftime('%Y-%m-%d')
                display_df = display_df.drop(columns=['Document Date'])

            st.dataframe(
                display_df,
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info("No large invoices detected in the last 30 days")

    with col2:
        st.write("**Top 5 Largest Payments**")
        if not outliers['top_payments'].empty:
            # Format for display
            display_df = outliers['top_payments'].copy()

            # Format amounts
            if 'amount' in display_df.columns:
                display_df['Amount'] = display_df['amount'].apply(lambda x: f"{x:,.2f}")
                display_df = display_df.drop(columns=['amount'])

            # Format Z-score
            if 'z_score' in display_df.columns:
                display_df['Z-Score'] = display_df['z_score'].apply(lambda x: f"{x:.2f}")
                display_df = display_df.drop(columns=['z_score'])

            # Format date
            if 'date' in display_df.columns:
                display_df['Date'] = pd.to_datetime(display_df['date']).dt.strftime('%Y-%m-%d')
                display_df = display_df.drop(columns=['date'])

            st.dataframe(
                display_df,
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info("No large payments detected in the last 30 days")

    # Recent legal cases
    if not outliers['recent_legal_cases'].empty:
        st.write("**Recent Legal Cases (Last 30 Days)**")
        st.dataframe(
            outliers['recent_legal_cases'],
            hide_index=True,
            use_container_width=True
        )


def render_counterparty_legal_cases(user_id):
    """
    Render counterparty legal cases section with:
    - Summary metrics (total cases, as plaintiff, as defendant)
    - Cases table with company, case#, type, date, amount, role, exposure
    """
    lang = st.session_state.get('language', 'en')

    st.subheader("Recent Counterparty Legal Cases (Last 30 Days)")

    with st.spinner("Loading counterparty legal cases..."):
        cases = get_counterparty_legal_cases(user_id, days=30)

    if cases.empty:
        st.success("✅ No recent legal cases involving your counterparties")
    else:
        # Summary metrics
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Cases", len(cases))

        with col2:
            plaintiff_count = len(cases[cases['role'] == 'Plaintiff']) if 'role' in cases.columns else 0
            st.metric("As Plaintiff", plaintiff_count)

        with col3:
            defendant_count = len(cases[cases['role'] == 'Defendant']) if 'role' in cases.columns else 0
            st.metric("As Defendant", defendant_count)

        # Cases table
        st.write("**Case Details:**")

        # Select columns to display
        display_cols = []
        for col in ['company_name', 'case_number', 'case_type', 'filing_date', 'claim_amount', 'role', 'our_exposure']:
            if col in cases.columns:
                display_cols.append(col)

        if display_cols:
            display_df = cases[display_cols].copy()

            # Format dates
            if 'filing_date' in display_df.columns:
                display_df['filing_date'] = pd.to_datetime(display_df['filing_date']).dt.strftime('%Y-%m-%d')

            # Format amounts
            for col in ['claim_amount', 'our_exposure']:
                if col in display_df.columns:
                    display_df[col] = display_df[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "N/A")

            st.dataframe(
                display_df,
                hide_index=True,
                use_container_width=True,
                column_config={
                    'company_name': st.column_config.TextColumn('Company', width='medium'),
                    'case_number': st.column_config.TextColumn('Case #', width='small'),
                    'case_type': st.column_config.TextColumn('Type', width='small'),
                    'filing_date': st.column_config.TextColumn('Filed', width='small'),
                    'claim_amount': st.column_config.TextColumn('Claim Amount', width='small'),
                    'role': st.column_config.TextColumn('Role', width='small'),
                    'our_exposure': st.column_config.TextColumn('Our Exposure', width='small')
                }
            )
        else:
            st.dataframe(cases, hide_index=True, use_container_width=True)


def render_cash_flow_projection(user_id):
    """
    Render cash flow projection card with:
    - Four metric cards (expected inflow, expected outflow, net CF, confidence)
    - Expandable breakdown (invoiced AR, weighted collections, AP due)
    - Formula explanation
    """
    lang = st.session_state.get('language', 'en')

    st.subheader("Cash Flow Projection (Next 30 Days)")

    with st.spinner("Calculating cash flow projection..."):
        cf = calculate_cash_flow_projection(user_id, horizon_days=30)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Expected Inflow", f"{cf['expected_inflow']:,.0f}")

    with col2:
        st.metric("Expected Outflow", f"{cf['expected_outflow']:,.0f}")

    with col3:
        delta_color = "normal" if cf['net_cash_flow'] >= 0 else "inverse"
        st.metric(
            "Net Cash Flow",
            f"{cf['net_cash_flow']:,.0f}",
            delta_color=delta_color
        )

    with col4:
        # Confidence indicator
        confidence_emoji = {
            'high': '🟢',
            'medium': '🟡',
            'low': '🔴'
        }
        emoji = confidence_emoji.get(cf['confidence_level'], '⚪')
        st.metric("Confidence", f"{emoji} {cf['confidence_level'].upper()}")

    # Breakdown
    with st.expander("📊 Calculation Breakdown"):
        breakdown = cf['breakdown']

        st.write("**Cash Inflow Calculation:**")
        st.write(f"- Invoiced AR (last 30 days): {breakdown['invoiced_ar']:,.0f}")
        st.write(f"- Collection adjustment (PD × Aging): -{breakdown['collection_adjustment']:,.0f}")
        st.write(f"- **Expected Collections: {breakdown['weighted_collections']:,.0f}**")

        st.write("")
        st.write("**Cash Outflow Calculation:**")
        st.write(f"- AP Due (next 30 days): {breakdown['ap_due']:,.0f}")

        st.write("")
        st.write("**Net Cash Flow:**")
        st.write(f"- Inflow - Outflow = **{cf['net_cash_flow']:,.0f}**")

        st.caption("""
        **Formula:**
        - Inflow = Σ(Invoice Amount × Collection Probability × Aging Factor)
        - Collection Probability = 1 - (Customer PD / 100)
        - Aging Factors: 0-30 days (95%), 31-60 (80%), 61-90 (60%), 90+ (30%)
        - Outflow = Sum of payables due in next 30 days
        """)


def render_recent_metrics(user_id):
    """
    Render recent activity metrics with:
    - Left column: Invoice activity (sales & purchase metrics)
    - Right column: Bank transaction activity (inflow & outflow metrics)
    """
    lang = st.session_state.get('language', 'en')

    st.subheader("Recent Activity (Last 7 Days)")

    col1, col2 = st.columns(2)

    with col1:
        st.write("### 📄 Invoice Activity")

        with st.spinner("Loading invoice metrics..."):
            invoice_metrics = get_recent_invoice_metrics(user_id, days=7)

        st.write("**Sales (OUT) Invoices:**")
        sales = invoice_metrics['sales_invoices']

        metric_col1, metric_col2, metric_col3 = st.columns(3)
        with metric_col1:
            st.metric("Count", sales['count'])
        with metric_col2:
            st.metric("Total", f"{sales['total_amount']:,.0f}")
        with metric_col3:
            trend_emoji = {'up': '📈', 'down': '📉', 'stable': '➡️'}
            emoji = trend_emoji.get(sales['trend'], '➡️')
            st.metric("Avg", f"{sales['avg_amount']:,.0f}", delta=f"{emoji} {sales['trend']}")

        st.write("")
        st.write("**Purchase (IN) Invoices:**")
        purch = invoice_metrics['purchase_invoices']

        metric_col1, metric_col2, metric_col3 = st.columns(3)
        with metric_col1:
            st.metric("Count", purch['count'])
        with metric_col2:
            st.metric("Total", f"{purch['total_amount']:,.0f}")
        with metric_col3:
            emoji = trend_emoji.get(purch['trend'], '➡️')
            st.metric("Avg", f"{purch['avg_amount']:,.0f}", delta=f"{emoji} {purch['trend']}")

    with col2:
        st.write("### 💰 Bank Transaction Activity")

        with st.spinner("Loading bank metrics..."):
            bank_metrics = get_recent_bank_metrics(user_id, days=7)

        st.write("**Payment Inflow (Credit):**")
        inflow = bank_metrics['payment_inflow']

        metric_col1, metric_col2, metric_col3 = st.columns(3)
        with metric_col1:
            st.metric("Count", inflow['count'])
        with metric_col2:
            st.metric("Total", f"{inflow['total_amount']:,.0f}")
        with metric_col3:
            st.metric("Avg", f"{inflow['avg_amount']:,.0f}")

        st.write("")
        st.write("**Payment Outflow (Debit):**")
        outflow = bank_metrics['payment_outflow']

        metric_col1, metric_col2, metric_col3 = st.columns(3)
        with metric_col1:
            st.metric("Count", outflow['count'])
        with metric_col2:
            st.metric("Total", f"{outflow['total_amount']:,.0f}")
        with metric_col3:
            st.metric("Avg", f"{outflow['avg_amount']:,.0f}")


def main():
    """Main dashboard function"""
    user_id = st.session_state.get('user_id')
    if not user_id:
        st.error("User not authenticated. Please log in.")
        return

    # Render header
    render_header()

    # Section 1: Company Health Rating
    render_company_health_card(user_id)
    st.divider()

    # Section 2: Recent Outliers
    render_recent_outliers(user_id)
    st.divider()

    # Section 3: Counterparty Legal Cases
    render_counterparty_legal_cases(user_id)
    st.divider()

    # Section 4: Cash Flow Projection
    render_cash_flow_projection(user_id)
    st.divider()

    # Section 5: Recent Metrics (Invoice + Bank Activity)
    render_recent_metrics(user_id)


if __name__ == "__main__":
    main()
