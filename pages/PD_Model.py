"""
PD/ECL Model - Credit Risk Analytics
=====================================
This page implements a complete Probability of Default (PD) and
Expected Credit Loss (ECL) calculation engine using roll-rate methodology.

Features:
- Upload monthly exposure data (Excel/CSV)
- Automatic roll-rate calculation
- PD estimation with calibration
- ECL computation
- Interactive visualizations
- Export results to Excel
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
import os

# Add parent directory to path to import risk_model
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import risk_model
from utils.db_operations import (
    get_monthly_aging_summary,
    get_customer_level_aging,
    get_aging_time_series,
    get_invoice_level_data_for_ecl,
    get_data_diagnostics,
    get_monthly_aging_with_payments
)


# Page configuration
st.set_page_config(
    page_title="PD/ECL Model",
    page_icon="📊",
    layout="wide"
)

st.title("📊 PD/ECL Credit Risk Model")
st.markdown("""
This tool calculates **Probability of Default (PD)** and **Expected Credit Loss (ECL)**
using the **roll-rate methodology** based on monthly exposure data across aging buckets.
""")

# Sidebar for parameters
st.sidebar.header("⚙️ Model Parameters")

# Initialize session state
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'exposure_df' not in st.session_state:
    st.session_state.exposure_df = None


# =============================================================================
# 1. DATA LOADING SECTION
# =============================================================================
st.header("1️⃣ Data Loading")

# Data source selection
data_source = st.radio(
    "Select Data Source",
    options=["📊 Load from Database", "📁 Upload File"],
    horizontal=True,
    help="Choose to load data from your database or upload a file"
)

if data_source == "📊 Load from Database":
    with st.expander("📊 Load Data from Database", expanded=True):
        st.markdown("""
        **Load aging data from your database:**
        - **Accounts Receivable (AR)**: Invoices with type='OUT' and status='Signed'
        - **Money IN**: Bank transactions (incoming payments)
        - Automatically calculates net exposure (AR - Payments) for accurate credit risk
        - Groups by month to create time series for roll-rate analysis
        """)

        # Get user_id from session state
        if 'user_id' not in st.session_state:
            st.warning("⚠️ Please log in to load data from database")
            st.session_state.data_loaded = False
        else:
            col1, col2 = st.columns([3, 1])

            with col1:
                # Option to choose data source
                db_data_type = st.selectbox(
                    "Select Data View",
                    options=[
                        "Net Exposure with Payments (AR - Money IN) - RECOMMENDED",
                        "Monthly Aging Summary (for Roll-Rate Analysis)",
                        "Time Series Snapshots (for Trend Analysis)",
                        "Customer-Level Analysis (for Segmentation)"
                    ],
                    help="Choose how to aggregate your invoice data"
                )

            with col2:
                load_button = st.button("🔄 Load Data", type="primary", use_container_width=True)

            if load_button:
                try:
                    user_id = st.session_state.user_id

                    with st.spinner("Loading data from database..."):
                        if db_data_type == "Net Exposure with Payments (AR - Money IN) - RECOMMENDED":
                            df = get_monthly_aging_with_payments(user_id)
                            st.session_state.data_source_type = "net_exposure_with_payments"

                        elif db_data_type == "Monthly Aging Summary (for Roll-Rate Analysis)":
                            df = get_monthly_aging_summary(user_id)
                            st.session_state.data_source_type = "monthly_summary"

                        elif db_data_type == "Time Series Snapshots (for Trend Analysis)":
                            df = get_aging_time_series(user_id)
                            st.session_state.data_source_type = "time_series"

                        else:  # Customer-Level Analysis
                            df = get_customer_level_aging(user_id)
                            st.session_state.data_source_type = "customer_level"
                            # For customer level, we need to reshape for PD model
                            st.info("📌 Customer-level data loaded. This view is for analysis - use Net Exposure with Payments for PD/ECL calculation.")

                    if df.empty:
                        st.warning("⚠️ No invoice data found in database. Please upload invoices first.")
                        st.session_state.data_loaded = False
                    else:
                        st.session_state.exposure_df = df
                        st.session_state.data_loaded = True
                        st.success(f"✅ Data loaded successfully! {len(df)} records found.")

                        # Display data preview
                        st.subheader("Data Preview")
                        st.dataframe(df.head(10), use_container_width=True)

                        # Display data summary
                        if st.session_state.data_source_type == "customer_level":
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Total Customers", len(df))
                            with col2:
                                st.metric("Total Exposure", f"{df['total_exposure'].sum():,.0f}")
                            with col3:
                                st.metric("Avg Exposure", f"{df['total_exposure'].mean():,.0f}")
                        else:
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Total Periods", len(df))
                            with col2:
                                if 'date' in df.columns:
                                    st.metric("Date Range",
                                             f"{df['date'].min().strftime('%Y-%m')} to {df['date'].max().strftime('%Y-%m')}")
                            with col3:
                                numeric_cols = df.select_dtypes(include=[np.number]).columns
                                st.metric("Buckets", len(numeric_cols))
                            with col4:
                                total_exp = df[['not_aged', 'dpd_0_30', 'dpd_31_60', 'default']].sum().sum() if all(col in df.columns for col in ['not_aged', 'dpd_0_30', 'dpd_31_60', 'default']) else 0
                                st.metric("Total Exposure", f"{total_exp:,.0f}")

                        # Add diagnostic information
                        with st.expander("🔍 Data Diagnostics", expanded=False):
                            st.markdown("**Understanding your data:**")

                            # Add button to show detailed diagnostics
                            if st.button("🔍 Run Detailed Diagnostics"):
                                with st.spinner("Analyzing invoice data..."):
                                    diag = get_data_diagnostics(user_id)

                                if diag:
                                    st.subheader("📊 Raw Data Analysis")

                                    col1, col2, col3, col4 = st.columns(4)
                                    with col1:
                                        st.metric("Total Invoices (OUT)", diag['total_invoices'])
                                    with col2:
                                        st.metric("Unique Customers", diag['total_customers'])
                                    with col3:
                                        st.metric("Distinct Months", diag['distinct_months'])
                                    with col4:
                                        st.metric("Total Value", f"{diag['total_exposure']:,.0f}")

                                    st.markdown("---")

                                    col1, col2 = st.columns(2)
                                    with col1:
                                        st.subheader("📋 By Invoice Status")
                                        status_df = pd.DataFrame(diag['by_status'])
                                        st.dataframe(
                                            status_df.style.format({
                                                'invoice_count': '{:,.0f}',
                                                'total_amount': '{:,.0f}'
                                            }),
                                            use_container_width=True,
                                            hide_index=True
                                        )

                                    with col2:
                                        st.subheader("✅ Filtered for PD Model")
                                        st.markdown(f"""
                                        **Filters Applied:**
                                        - Status = 'Подписан' or 'Signed'
                                        - Amount > 0
                                        - Amount IS NOT NULL

                                        **Results:**
                                        - **{diag['filtered_invoices']:,}** invoices included
                                        - **{diag['filtered_months']}** months with data
                                        - **{diag['filtered_exposure']:,.0f}** total exposure
                                        """)

                                        if diag['filtered_invoices'] < diag['total_invoices']:
                                            excluded = diag['total_invoices'] - diag['filtered_invoices']
                                            st.warning(f"⚠️ {excluded:,} invoices excluded due to filters")

                                    st.markdown("---")
                                    st.success(f"✅ Your {len(df)} records represent {diag['filtered_months']} months of aggregated data")

                            st.markdown("---")

                            if st.session_state.data_source_type == "net_exposure_with_payments":
                                st.info(f"""
                                📊 **What does {len(df)} records mean?**
                                - Each record represents a month-end snapshot
                                - {len(df)} records = {len(df)} months of data
                                - **Net Exposure = Invoices (AR) - Payments (Money IN)**

                                **How it works:**
                                1. For each month-end, calculate total invoices issued (type='OUT')
                                2. Match with incoming bank payments by customer INN
                                3. Compute net outstanding = invoiced - paid
                                4. Age the net outstanding based on oldest unpaid invoice

                                **Benefits:**
                                - More accurate credit risk assessment
                                - Accounts for partial payments
                                - Reflects actual collection performance
                                - Better PD/ECL estimates
                                """)

                                # Show month-by-month breakdown for net exposure
                                st.subheader("Monthly Net Exposure Breakdown")
                                month_summary = df.copy()
                                month_summary['total'] = month_summary[['not_aged', 'dpd_0_30', 'dpd_31_60', 'default']].sum(axis=1)
                                month_summary['month'] = month_summary['date'].dt.strftime('%Y-%m')

                                display_cols = ['month', 'not_aged', 'dpd_0_30', 'dpd_31_60', 'default', 'total']
                                st.dataframe(
                                    month_summary[display_cols].style.format({
                                        'not_aged': '{:,.0f}',
                                        'dpd_0_30': '{:,.0f}',
                                        'dpd_31_60': '{:,.0f}',
                                        'default': '{:,.0f}',
                                        'total': '{:,.0f}'
                                    }),
                                    use_container_width=True
                                )

                                # Show summary statistics
                                st.subheader("Summary Statistics")
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.metric("Average Monthly Net Exposure", f"{month_summary['total'].mean():,.0f}")
                                    st.metric("Max Monthly Net Exposure", f"{month_summary['total'].max():,.0f}")
                                with col2:
                                    st.metric("Min Monthly Net Exposure", f"{month_summary['total'].min():,.0f}")
                                    st.metric("Latest Month Net Exposure", f"{month_summary['total'].iloc[-1]:,.0f}")

                            elif st.session_state.data_source_type in ["monthly_summary", "time_series"]:
                                st.info(f"""
                                📊 **What does {len(df)} records mean?**
                                - The query groups invoices by MONTH
                                - {len(df)} records = {len(df)} months of data
                                - This is correct for roll-rate analysis

                                **How it works:**
                                ```sql
                                SELECT DATE_TRUNC('month', document_date) as date,
                                       SUM(amounts in each aging bucket)
                                FROM invoices
                                WHERE invoice_type = 'OUT' AND status = 'Signed'
                                GROUP BY DATE_TRUNC('month', document_date)
                                ```

                                **To see underlying invoices:**
                                - Go to the Accounts Receivable page
                                - Or use "Customer-Level Analysis" view above
                                """)

                                # Show month-by-month breakdown
                                st.subheader("Monthly Breakdown")
                                month_summary = df.copy()
                                month_summary['total'] = month_summary[['not_aged', 'dpd_0_30', 'dpd_31_60', 'default']].sum(axis=1)
                                month_summary['month'] = month_summary['date'].dt.strftime('%Y-%m')

                                display_cols = ['month', 'not_aged', 'dpd_0_30', 'dpd_31_60', 'default', 'total']
                                st.dataframe(
                                    month_summary[display_cols].style.format({
                                        'not_aged': '{:,.0f}',
                                        'dpd_0_30': '{:,.0f}',
                                        'dpd_31_60': '{:,.0f}',
                                        'default': '{:,.0f}',
                                        'total': '{:,.0f}'
                                    }),
                                    use_container_width=True
                                )

                                # Show summary statistics
                                st.subheader("Summary Statistics")
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.metric("Average Monthly Exposure", f"{month_summary['total'].mean():,.0f}")
                                    st.metric("Max Monthly Exposure", f"{month_summary['total'].max():,.0f}")
                                with col2:
                                    st.metric("Min Monthly Exposure", f"{month_summary['total'].min():,.0f}")
                                    st.metric("Total Across All Months", f"{month_summary['total'].sum():,.0f}")

                            elif st.session_state.data_source_type == "customer_level":
                                st.info(f"""
                                📊 **What does {len(df)} records mean?**
                                - Each record = 1 customer
                                - {len(df)} records = {len(df)} unique customers with outstanding invoices

                                **Exposure Distribution:**
                                - Top customer: {df['total_exposure'].max():,.0f}
                                - Average per customer: {df['total_exposure'].mean():,.0f}
                                - Median per customer: {df['total_exposure'].median():,.0f}
                                """)

                except Exception as e:
                    st.error(f"❌ Error loading data from database: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())
                    st.session_state.data_loaded = False

else:  # File Upload
    with st.expander("📁 Upload Exposure Data", expanded=True):
        st.markdown("""
        **Expected format:**
        - Column 1: Date/Month (e.g., "2022-01-01", "Jan 2022")
        - Subsequent columns: Exposure amounts per bucket
          - `not_aged` or `current`: Exposure with 0 DPD
          - `0-30` or `dpd_0_30`: Exposure with 1-30 days past due
          - `31-60` or `dpd_31_60`: Exposure with 31-60 days past due
          - `default` or `60+`: Exposure in default (>60 DPD)

        📝 **Example:**
        ```
        date       | not_aged | 0-30  | 31-60 | default
        2022-01-01 | 1000000  | 50000 | 20000 | 10000
        2022-02-01 | 1050000  | 55000 | 22000 | 11000
        ```
        """)

        uploaded_file = st.file_uploader(
            "Choose an Excel or CSV file",
            type=['xlsx', 'xls', 'csv'],
            help="Upload your monthly exposure data"
        )

        if uploaded_file is not None:
            try:
                # Load data using risk_model module
                with st.spinner("Loading data..."):
                    df = risk_model.load_data(uploaded_file)
                    st.session_state.exposure_df = df
                    st.session_state.data_loaded = True
                    st.session_state.data_source_type = "file_upload"

                st.success(f"✅ Data loaded successfully! {len(df)} months of data found.")

                # Display data preview
                st.subheader("Data Preview")
                st.dataframe(df.head(10), use_container_width=True)

                # Display data summary
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Months", len(df))
                with col2:
                    st.metric("Date Range",
                             f"{df['date'].min().strftime('%Y-%m')} to {df['date'].max().strftime('%Y-%m')}")
                with col3:
                    numeric_cols = df.select_dtypes(include=[np.number]).columns
                    st.metric("Buckets", len(numeric_cols))

            except Exception as e:
                st.error(f"❌ Error loading file: {str(e)}")
                st.session_state.data_loaded = False


# =============================================================================
# 2. EXPOSURE ANALYSIS
# =============================================================================
if st.session_state.data_loaded:
    st.header("2️⃣ Exposure Analysis")

    with st.expander("📈 Exposure Trends", expanded=True):
        df = st.session_state.exposure_df

        # Prepare data for visualization
        # Identify bucket columns
        bucket_cols = [col for col in df.columns if col not in ['date', 'total', 'total_exposure']]

        # Create stacked area chart
        fig = go.Figure()

        for col in bucket_cols:
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df[col],
                name=col.replace('_', ' ').title(),
                mode='lines',
                stackgroup='one',
                fillcolor=None
            ))

        fig.update_layout(
            title="Monthly Exposure by Aging Bucket",
            xaxis_title="Month",
            yaxis_title="Exposure Amount",
            hovermode='x unified',
            height=500
        )

        st.plotly_chart(fig, use_container_width=True)

        # Exposure statistics table
        st.subheader("Exposure Statistics")
        stats_df = df[bucket_cols].describe().T
        stats_df['total'] = df[bucket_cols].sum()
        st.dataframe(stats_df.style.format("{:,.0f}"), use_container_width=True)


# =============================================================================
# 3. ROLL-RATE CALCULATION
# =============================================================================
if st.session_state.data_loaded:
    st.header("3️⃣ Roll-Rate Calculation")

    with st.expander("🔄 Monthly Roll-Rates", expanded=True):
        try:
            with st.spinner("Calculating roll-rates..."):
                roll_rates_df = risk_model.compute_roll_rates(st.session_state.exposure_df)
                st.session_state.roll_rates_df = roll_rates_df

            st.success("✅ Roll-rates calculated successfully!")

            # Display roll-rates table
            st.subheader("Monthly Roll-Rates")
            display_df = roll_rates_df[['month', 'roll_rate_0_30', 'roll_rate_31_60',
                                        'exposure_0_30', 'exposure_31_60']].copy()
            display_df.columns = ['Month', '0-30 Roll-Rate', '31-60 Roll-Rate',
                                  '0-30 Exposure', '31-60 Exposure']

            # Format percentages and numbers
            st.dataframe(
                display_df.style.format({
                    '0-30 Roll-Rate': '{:.2%}',
                    '31-60 Roll-Rate': '{:.2%}',
                    '0-30 Exposure': '{:,.0f}',
                    '31-60 Exposure': '{:,.0f}'
                }),
                use_container_width=True
            )

            # Visualize roll-rates over time
            st.subheader("Roll-Rate Trends")

            fig = go.Figure()

            fig.add_trace(go.Scatter(
                x=roll_rates_df['date'],
                y=roll_rates_df['roll_rate_0_30'],
                name='0-30 → 31-60 Roll-Rate',
                mode='lines+markers',
                line=dict(color='blue', width=2)
            ))

            fig.add_trace(go.Scatter(
                x=roll_rates_df['date'],
                y=roll_rates_df['roll_rate_31_60'],
                name='31-60 → Default Roll-Rate',
                mode='lines+markers',
                line=dict(color='red', width=2)
            ))

            fig.update_layout(
                title="Monthly Roll-Rates Over Time",
                xaxis_title="Month",
                yaxis_title="Roll-Rate (%)",
                yaxis_tickformat='.0%',
                hovermode='x unified',
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)

            # Statistics
            col1, col2 = st.columns(2)
            with col1:
                st.metric(
                    "Avg 0-30 Roll-Rate",
                    f"{roll_rates_df['roll_rate_0_30'].mean():.2%}",
                    delta=f"Std: {roll_rates_df['roll_rate_0_30'].std():.2%}"
                )
            with col2:
                st.metric(
                    "Avg 31-60 Roll-Rate",
                    f"{roll_rates_df['roll_rate_31_60'].mean():.2%}",
                    delta=f"Std: {roll_rates_df['roll_rate_31_60'].std():.2%}"
                )

        except Exception as e:
            st.error(f"❌ Error calculating roll-rates: {str(e)}")


# =============================================================================
# 4. ANNUAL ROLL-RATES
# =============================================================================
if st.session_state.data_loaded and 'roll_rates_df' in st.session_state:
    st.header("4️⃣ Annual Roll-Rates")

    with st.expander("📅 Year Selection & Annual Averages", expanded=True):
        # Year selection
        available_years = sorted(st.session_state.roll_rates_df['date'].dt.year.unique())

        col1, col2 = st.columns([1, 3])
        with col1:
            selected_year = st.selectbox(
                "Select Year",
                options=['All Years'] + available_years,
                help="Choose a specific year or use all available data"
            )

        try:
            # Calculate annual roll-rates
            year_param = None if selected_year == 'All Years' else selected_year
            with st.spinner("Calculating annual averages..."):
                annual_rates = risk_model.compute_annual_roll_rates(
                    st.session_state.roll_rates_df,
                    year=year_param
                )
                st.session_state.annual_rates = annual_rates

            # Display results
            st.subheader("Annual Roll-Rate Summary")

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(
                    "Avg 0-30 Roll-Rate",
                    f"{annual_rates['avg_roll_0_30']:.2%}"
                )
            with col2:
                st.metric(
                    "Avg 31-60 Roll-Rate",
                    f"{annual_rates['avg_roll_31_60']:.2%}"
                )
            with col3:
                st.metric(
                    "Months Analyzed",
                    annual_rates['num_months']
                )

            # Show calculation details
            with st.expander("ℹ️ Calculation Details"):
                st.markdown(f"""
                **Period:** {annual_rates['year'] if annual_rates['year'] else 'All available data'}

                **Methodology:** Exposure-weighted average
                - Larger exposures have proportionally more impact on the average
                - Formula: Σ(roll_rate × exposure) / Σ(exposure)

                **Results:**
                - Average 0-30 Roll-Rate: {annual_rates['avg_roll_0_30']:.4%}
                - Average 31-60 Roll-Rate: {annual_rates['avg_roll_31_60']:.4%}
                - Based on {annual_rates['num_months']} months of data
                """)

        except Exception as e:
            st.error(f"❌ Error calculating annual roll-rates: {str(e)}")


# =============================================================================
# 5. PD CALCULATION
# =============================================================================
if st.session_state.data_loaded and 'annual_rates' in st.session_state:
    st.header("5️⃣ Probability of Default (PD)")

    with st.expander("🎯 PD Calibration & Calculation", expanded=True):
        st.markdown("""
        **PD Methodology:**
        - **Not Aged (0 DPD):** Calibrated based on expert judgment or historical data
        - **0-30 DPD:** Derived from roll-rate (0-30 → 31-60)
        - **31-60 DPD:** Derived from roll-rate (31-60 → default)
        - **Default:** 100% (already in default)
        """)

        # Parameter inputs
        col1, col2 = st.columns(2)
        with col1:
            pd_not_aged = st.number_input(
                "PD for Not Aged (%)",
                min_value=0.0,
                max_value=100.0,
                value=3.0,
                step=0.1,
                help="Calibrated PD for current/not aged exposure"
            ) / 100

        with col2:
            scaling_factor = st.number_input(
                "PD Scaling Factor",
                min_value=0.1,
                max_value=5.0,
                value=1.0,
                step=0.1,
                help="Multiplier to adjust PD estimates (1.0 = no adjustment)"
            )

        try:
            # Calculate PDs
            with st.spinner("Calculating PDs..."):
                pd_dict = risk_model.calculate_pd(
                    st.session_state.annual_rates,
                    pd_not_aged=pd_not_aged,
                    scaling_factor=scaling_factor
                )
                st.session_state.pd_dict = pd_dict

            st.success("✅ PDs calculated successfully!")

            # Display PD results
            st.subheader("PD Summary by Bucket")

            pd_summary = pd.DataFrame([
                {'Bucket': 'Not Aged (0 DPD)', 'PD': pd_dict['pd_not_aged'],
                 'Source': 'Calibrated'},
                {'Bucket': '0-30 DPD', 'PD': pd_dict['pd_0_30'],
                 'Source': f"Roll-rate × {scaling_factor}"},
                {'Bucket': '31-60 DPD', 'PD': pd_dict['pd_31_60'],
                 'Source': f"Roll-rate × {scaling_factor}"},
                {'Bucket': 'Default (60+ DPD)', 'PD': pd_dict['pd_default'],
                 'Source': 'Definition'},
            ])

            st.dataframe(
                pd_summary.style.format({'PD': '{:.2%}'}),
                use_container_width=True,
                hide_index=True
            )

            # Visualize PDs
            st.subheader("PD Distribution")

            fig = go.Figure(data=[
                go.Bar(
                    x=pd_summary['Bucket'],
                    y=pd_summary['PD'],
                    text=pd_summary['PD'].apply(lambda x: f'{x:.2%}'),
                    textposition='outside',
                    marker_color=['green', 'yellow', 'orange', 'red']
                )
            ])

            fig.update_layout(
                title="Probability of Default by Aging Bucket",
                xaxis_title="Aging Bucket",
                yaxis_title="PD (%)",
                yaxis_tickformat='.0%',
                height=400,
                showlegend=False
            )

            st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"❌ Error calculating PDs: {str(e)}")


# =============================================================================
# 6. ECL CALCULATION
# =============================================================================
if st.session_state.data_loaded and 'pd_dict' in st.session_state:
    st.header("6️⃣ Expected Credit Loss (ECL)")

    with st.expander("💰 ECL Calculation", expanded=True):
        st.markdown("""
        **ECL Formula:** ECL = Exposure × PD × LGD

        Where:
        - **Exposure:** Outstanding amount in each bucket
        - **PD:** Probability of Default (calculated above)
        - **LGD:** Loss Given Default (% of exposure lost if default occurs)
        """)

        # Parameters
        col1, col2 = st.columns(2)
        with col1:
            lgd = st.number_input(
                "LGD - Loss Given Default (%)",
                min_value=0.0,
                max_value=100.0,
                value=100.0,
                step=1.0,
                help="Percentage of exposure lost upon default"
            ) / 100

        with col2:
            use_latest = st.checkbox(
                "Use Latest Month Only",
                value=True,
                help="Calculate ECL for latest month vs. average across all months"
            )

        try:
            # Calculate ECL
            with st.spinner("Calculating ECL..."):
                summary_df, detailed_df = risk_model.calculate_ecl(
                    st.session_state.exposure_df,
                    st.session_state.pd_dict,
                    lgd=lgd,
                    use_latest_month=use_latest
                )
                st.session_state.ecl_summary = summary_df
                st.session_state.ecl_detailed = detailed_df

            st.success("✅ ECL calculated successfully!")

            # Display ECL Summary
            st.subheader("📊 ECL Summary")

            # Format and display
            display_summary = summary_df.copy()
            st.dataframe(
                display_summary.style.format({
                    'exposure': '{:,.0f}',
                    'pd': '{:.2%}',
                    'lgd': '{:.2%}',
                    'ecl_rate': '{:.2%}',
                    'ecl': '{:,.0f}'
                }),
                use_container_width=True,
                hide_index=True
            )

            # Key metrics
            total_row = summary_df[summary_df['bucket'] == 'TOTAL'].iloc[0]

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric(
                    "Total Exposure",
                    f"{total_row['exposure']:,.0f}"
                )
            with col2:
                st.metric(
                    "Weighted Avg PD",
                    f"{total_row['pd']:.2%}"
                )
            with col3:
                st.metric(
                    "ECL Rate",
                    f"{total_row['ecl_rate']:.2%}"
                )
            with col4:
                st.metric(
                    "Total ECL",
                    f"{total_row['ecl']:,.0f}"
                )

            # ECL Visualization
            st.subheader("ECL Distribution")

            # Pie chart
            fig = px.pie(
                summary_df[summary_df['bucket'] != 'TOTAL'],
                values='ecl',
                names='bucket',
                title='ECL by Aging Bucket',
                hole=0.4
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)

            # Bar chart comparing exposure vs ECL
            fig = go.Figure()

            non_total = summary_df[summary_df['bucket'] != 'TOTAL']

            fig.add_trace(go.Bar(
                name='Exposure',
                x=non_total['bucket'],
                y=non_total['exposure'],
                marker_color='lightblue'
            ))

            fig.add_trace(go.Bar(
                name='ECL',
                x=non_total['bucket'],
                y=non_total['ecl'],
                marker_color='darkred'
            ))

            fig.update_layout(
                title='Exposure vs. ECL by Bucket',
                xaxis_title='Aging Bucket',
                yaxis_title='Amount',
                barmode='group',
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)

            # Time series of ECL (if not using latest month only)
            if not use_latest and len(detailed_df) > 1:
                st.subheader("ECL Trend Over Time")

                fig = go.Figure()

                fig.add_trace(go.Scatter(
                    x=detailed_df['date'],
                    y=detailed_df['total_ecl'],
                    mode='lines+markers',
                    name='Total ECL',
                    line=dict(color='red', width=2)
                ))

                fig.update_layout(
                    title='Total ECL Over Time',
                    xaxis_title='Month',
                    yaxis_title='ECL Amount',
                    hovermode='x unified',
                    height=400
                )

                st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"❌ Error calculating ECL: {str(e)}")


# =============================================================================
# 7. EXPORT RESULTS
# =============================================================================
if st.session_state.data_loaded and 'ecl_summary' in st.session_state:
    st.header("7️⃣ Export Results")

    with st.expander("💾 Download Results", expanded=True):
        col1, col2 = st.columns(2)

        with col1:
            # Export to Excel
            try:
                excel_bytes = risk_model.export_to_excel(
                    st.session_state.ecl_summary,
                    st.session_state.ecl_detailed,
                    st.session_state.roll_rates_df,
                    st.session_state.annual_rates,
                    st.session_state.pd_dict
                )

                st.download_button(
                    label="📥 Download Excel Report",
                    data=excel_bytes,
                    file_name=f"ECL_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    help="Download complete ECL analysis with all calculations"
                )
            except Exception as e:
                st.error(f"Error creating Excel file: {str(e)}")

        with col2:
            # Export ECL summary to CSV
            csv = st.session_state.ecl_summary.to_csv(index=False)
            st.download_button(
                label="📥 Download ECL Summary (CSV)",
                data=csv,
                file_name=f"ECL_Summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                help="Download ECL summary table"
            )


# =============================================================================
# SIDEBAR HELP & INFO
# =============================================================================
with st.sidebar:
    st.markdown("---")
    st.subheader("📚 About This Model")

    with st.expander("ℹ️ Methodology"):
        st.markdown("""
        **Roll-Rate Method:**
        1. Calculate monthly migration rates between buckets
        2. Average roll-rates over observation period
        3. Use roll-rates as base PD estimates
        4. Apply calibration and scaling
        5. Calculate ECL = Exposure × PD × LGD

        **Key Assumptions:**
        - Historical patterns repeat
        - Roll-rates stable over time
        - LGD typically 100% for unsecured
        - PD calibrated with expert judgment
        """)

    with st.expander("🔧 Tips"):
        st.markdown("""
        **Best Practices:**
        - Use at least 12 months of data
        - Check for seasonality in roll-rates
        - Calibrate PD for current conditions
        - Validate results against actuals
        - Document assumptions

        **Common Issues:**
        - Missing months → gaps in calculations
        - Zero exposures → undefined roll-rates
        - High volatility → consider smoothing
        """)

    st.markdown("---")
    st.caption("PD/ECL Model v1.0 | Built with Streamlit")
