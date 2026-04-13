"""
Factoring Risk Engine - Streamlit UI

Interactive interface for calculating PD, LGD, EAD, Expected Loss,
and credit limits for counterparties (customers/suppliers).
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date, timedelta
from typing import Optional
import json

from translations import get_text
from utils.risk_engine import RiskEngine, load_risk_config
from utils.risk_queries import (
    get_all_counterparties,
    get_all_invoices_and_payments,
    get_invoices_with_payments,
    get_portfolio_summary,
    search_counterparties,
    calculate_counterparty_lookback_period
)
from utils.risk_utils import export_to_excel, format_risk_report
from utils.contract_terms import (
    load_contract_payment_terms,
    save_contract_payment_term,
    delete_contract_payment_term
)


# Page config
st.set_page_config(
    page_title="Risk Engine",
    page_icon="⚠️",
    layout="wide"
)

# Initialize session state
if 'risk_results' not in st.session_state:
    st.session_state.risk_results = {}
if 'selected_counterparty' not in st.session_state:
    st.session_state.selected_counterparty = None


def tr(key: str, lang: str, **kwargs) -> str:
    """Helper wrapper for translations."""
    return get_text(key, lang).format(**kwargs)


def main():
    lang = st.session_state.get('language', 'en')
    st.title(tr("risk_title", lang))
    st.markdown(tr("risk_subtitle", lang))
    st.caption(tr("risk_caption", lang))
    
    # Check authentication
    if 'user_id' not in st.session_state or st.session_state.get('user_id') is None:
        st.warning(tr("risk_login_required", lang))
        return
    
    user_id = st.session_state['user_id']
    
    # Load configuration
    config = load_risk_config()
    
    # Load contract payment terms from database
    try:
        db_contract_terms = load_contract_payment_terms(user_id)
        config['contract_payment_terms'] = db_contract_terms
    except:
        config['contract_payment_terms'] = {}
    
    # Configuration section - MOVED FROM SIDEBAR TO MAIN PAGE
    st.header(tr("risk_sidebar_config", lang))
    
    config_col1, config_col2, config_col3 = st.columns(3)
    
    with config_col1:
        # Analysis date
        as_of_date = st.date_input(
            tr("risk_analysis_date_label", lang),
            value=date.today(),
            help=tr("risk_analysis_date_help", lang)
        )
    
    with config_col2:
        # Analysis type
        analysis_type_options = ['AR', 'AP']
        analysis_type = st.selectbox(
            tr("risk_analysis_type_label", lang),
            options=analysis_type_options,
            format_func=lambda opt: tr("risk_analysis_type_ar", lang) if opt == 'AR' else tr("risk_analysis_type_ap", lang),
            help=tr("risk_analysis_type_help", lang)
        )
        invoice_type = 'OUT' if analysis_type == 'AR' else 'IN'
    
    with config_col3:
        # Lookback period - will be calculated dynamically if counterparty is selected
        default_lookback = 24
        if 'selected_counterparty' in st.session_state and st.session_state.selected_counterparty:
            # Calculate dynamic lookback period
            calculated_lookback = calculate_counterparty_lookback_period(
                user_id, st.session_state.selected_counterparty, invoice_type, as_of_date
            )
            default_lookback = calculated_lookback
        
        months_back = st.slider(
            tr("risk_lookback_label", lang),
            min_value=3,
            max_value=36,
            value=default_lookback,
            help=tr("risk_lookback_help", lang)
        )
        
        # Show calculated value if counterparty is selected
        if 'selected_counterparty' in st.session_state and st.session_state.selected_counterparty:
            calculated_lookback = calculate_counterparty_lookback_period(
                user_id, st.session_state.selected_counterparty, invoice_type, as_of_date
            )
            if calculated_lookback != months_back:
                st.caption(f"💡 Calculated: {calculated_lookback} months (based on latest invoice)")
    
    # PD Model weights - expander on main page
    with st.expander(tr("risk_pd_weights_title", lang)):
        st.write(tr("risk_pd_weights_description", lang))
        w1 = st.number_input(tr("risk_pd_weight_w1", lang), value=2.0, min_value=0.0, max_value=10.0, step=0.5)
        w2 = st.number_input(tr("risk_pd_weight_w2", lang), value=2.0, min_value=0.0, max_value=10.0, step=0.5)
        w3 = st.number_input(tr("risk_pd_weight_w3", lang), value=3.0, min_value=0.0, max_value=10.0, step=0.5)
        w4 = st.number_input(tr("risk_pd_weight_w4", lang), value=1.0, min_value=0.0, max_value=10.0, step=0.5)
        w5 = st.number_input(tr("risk_pd_weight_w5", lang), value=1.0, min_value=0.0, max_value=10.0, step=0.5)
        
        config['pd_weights']['default'] = [w1, w2, w3, w4, w5]
    
    # Risk parameters - expander on main page
    with st.expander(tr("risk_parameters_title", lang)):
        risk_cap = st.number_input(
            tr("risk_cap_label", lang),
            value=1000000,
            min_value=0,
            step=100000,
            help=tr("risk_cap_help", lang)
        )
        config['risk_cap_default'] = risk_cap
        
        st.divider()
        
        # Due date configuration by contract
        st.write(tr("risk_payment_terms_title", lang))
        default_due_days = st.number_input(
            tr("risk_default_due_days_label", lang),
            value=30,
            min_value=0,
            max_value=365,
            step=1,
            help=tr("risk_default_due_days_help", lang)
        )
        config['default_due_days'] = default_due_days
        
        # Contract-specific terms
        if st.checkbox(tr("risk_terms_checkbox_label", lang), help=tr("risk_terms_checkbox_help", lang)):
            # Load current terms from database
            db_terms = config.get('contract_payment_terms', {})
            
            # Initialize session state from database if not already set
            if 'contract_terms_config' not in st.session_state:
                st.session_state.contract_terms_config = db_terms.copy()
            
            # Input for adding new contract
            st.write(tr("risk_terms_add_new", lang))
            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                new_contract = st.text_input(
                    tr("risk_terms_contract_label", lang),
                    placeholder=tr("risk_terms_contract_placeholder", lang),
                    key="new_contract_input"
                )
            with col2:
                new_days = st.number_input(
                    tr("risk_terms_days_label", lang),
                    value=0,
                    min_value=0,
                    max_value=365,
                    step=1,
                    key="new_days_input"
                )
            with col3:
                st.write("")
                st.write("")
                if st.button(tr("risk_terms_add_button", lang), key="add_contract", type="primary"):
                    if new_contract:
                        # Save to database
                        if save_contract_payment_term(user_id, new_contract, new_days, st.session_state.get('username', user_id)):
                            st.session_state.contract_terms_config[new_contract] = new_days
                            st.success(tr("risk_terms_save_success", lang, contract=new_contract))
                            st.rerun()
                        else:
                            st.error(tr("risk_terms_save_error", lang))
            
            st.divider()
            
            # Show current contract terms
            if st.session_state.contract_terms_config:
                st.write(tr("risk_terms_current_title", lang))
                
                terms_to_delete = []
                terms_to_update = {}
                
                for contract, days in sorted(st.session_state.contract_terms_config.items()):
                    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
                    with col1:
                        st.text(f"📄 {contract}")
                    with col2:
                        updated_days = st.number_input(
                            tr("risk_terms_days_label", lang),
                            value=days,
                            min_value=0,
                            max_value=365,
                            step=1,
                            key=f"days_{contract}",
                            label_visibility="collapsed"
                        )
                        if updated_days != days:
                            terms_to_update[contract] = updated_days
                    with col3:
                        if st.button("💾", key=f"save_{contract}", help=tr("risk_terms_save_help", lang)):
                            if save_contract_payment_term(user_id, contract, updated_days, st.session_state.get('username', user_id)):
                                st.session_state.contract_terms_config[contract] = updated_days
                                st.success(tr("risk_terms_update_success", lang, contract=contract))
                                st.rerun()
                    with col4:
                        if st.button("🗑️", key=f"del_{contract}", help=tr("risk_terms_delete_help", lang)):
                            if delete_contract_payment_term(user_id, contract):
                                terms_to_delete.append(contract)
                                st.success(tr("risk_terms_delete_success", lang, contract=contract))
                                st.rerun()
                
                # Remove deleted contracts from session state
                for contract in terms_to_delete:
                    if contract in st.session_state.contract_terms_config:
                        del st.session_state.contract_terms_config[contract]
                
                st.caption(tr("risk_terms_count_caption", lang, count=len(st.session_state.contract_terms_config)))
            else:
                st.caption(tr("risk_terms_none_caption", lang))
            
            # Apply current terms to config
            config['contract_payment_terms'] = st.session_state.contract_terms_config
    
    st.divider()
    
    # Main tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        tr("risk_tab_portfolio", lang),
        tr("risk_tab_counterparty", lang),
        tr("risk_tab_components", lang),
        tr("risk_tab_batch", lang),
        tr("risk_tab_daily", lang)
    ])
    
    # TAB 1: Portfolio Overview
    with tab1:
        show_portfolio_overview(user_id, invoice_type, as_of_date, config, lang)
    
    # TAB 2: Counterparty Analysis
    with tab2:
        show_counterparty_analysis(user_id, invoice_type, as_of_date, months_back, config, lang)
    
    # TAB 3: Detailed Components
    with tab3:
        show_detailed_components(user_id, invoice_type, as_of_date, months_back, config, lang)
    
    # TAB 4: Batch Analysis
    with tab4:
        show_batch_analysis(user_id, invoice_type, as_of_date, months_back, config, lang)

    with tab5:
        show_daily_aging_dashboard(user_id, invoice_type, as_of_date, config, lang)


def show_portfolio_overview(user_id, invoice_type, as_of_date, config, lang):
    """Display portfolio-level risk overview."""
    st.header(tr("risk_portfolio_header", lang))
    
    # Get portfolio summary
    portfolio = get_portfolio_summary(user_id, invoice_type, as_of_date)
    
    if not portfolio:
        st.info(tr("risk_portfolio_no_data", lang))
        return
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            tr("risk_metric_total_counterparties", lang),
            f"{portfolio.get('unique_counterparties', 0):,}"
        )
    
    with col2:
        st.metric(
            tr("risk_metric_total_invoices", lang),
            f"{portfolio.get('total_invoices', 0):,}"
        )
    
    with col3:
        st.metric(
            tr("risk_metric_total_exposure", lang),
            f"{portfolio.get('total_exposure', 0):,.0f}"
        )
    
    with col4:
        st.metric(
            tr("risk_metric_avg_invoice", lang),
            f"{portfolio.get('avg_invoice_amount', 0):,.0f}"
        )
    
    st.divider()
    
    # Get all counterparties for analysis
    st.subheader(tr("risk_portfolio_run_analysis", lang))
    
    if st.button(tr("risk_portfolio_button_analyze", lang), type="primary"):
        with st.spinner(tr("risk_portfolio_spinner", lang)):
            results = run_portfolio_analysis(user_id, invoice_type, as_of_date, 12, config)
            st.session_state.risk_results = results
    
    # Display results if available
    if st.session_state.risk_results:
        results = st.session_state.risk_results
        
        if results:
            # Convert to DataFrame
            results_df = pd.DataFrame(list(results.values()))
            
            # Rating distribution
            st.subheader(tr("risk_portfolio_rating_distribution", lang))
            rating_dist = results_df['rating'].value_counts().sort_index()
            
            fig = px.bar(
                x=rating_dist.index,
                y=rating_dist.values,
                labels={
                    'x': tr("risk_axis_rating", lang),
                    'y': tr("risk_axis_counterparty_count", lang)
                },
                title=tr("risk_portfolio_distribution_title", lang)
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Risk metrics summary
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader(tr("risk_exposure_by_rating", lang))
                exposure_by_rating = results_df.groupby('rating')['ead_current'].sum().sort_index()
                
                fig = px.pie(
                    values=exposure_by_rating.values,
                    names=exposure_by_rating.index,
                    title=tr("risk_current_exposure_title", lang)
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader(tr("risk_expected_loss_by_rating", lang))
                el_by_rating = results_df.groupby('rating')['expected_loss'].sum().sort_index()
                
                fig = px.bar(
                    x=el_by_rating.index,
                    y=el_by_rating.values,
                    labels={
                        'x': tr("risk_axis_rating", lang),
                        'y': tr("risk_axis_expected_loss", lang)
                    },
                    title=tr("risk_expected_loss_distribution_title", lang)
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Top 10 exposures
            st.subheader(tr("risk_top_exposures_title", lang))
            top_exposures = results_df.nlargest(10, 'ead_current')[
                ['counterparty_inn', 'rating', 'pd', 'lgd', 'ead_current', 'expected_loss', 'recommended_limit']
            ]
            st.dataframe(top_exposures, use_container_width=True)
            
            # Export option
            st.divider()
            if st.button(tr("risk_export_portfolio_button", lang)):
                output_path = f"risk_portfolio_{invoice_type}_{as_of_date}.xlsx"
                if export_to_excel(list(results.values()), output_path):
                    st.success(tr("risk_export_portfolio_success", lang, path=output_path))
                    with open(output_path, 'rb') as f:
                        st.download_button(
                            tr("risk_download_excel_report", lang),
                            f.read(),
                            file_name=output_path,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )


def show_counterparty_analysis(user_id, invoice_type, as_of_date, months_back, config, lang):
    """Display single counterparty risk analysis."""
    t = lambda key, **kwargs: tr(key, lang, **kwargs)

    # Show analysis first if counterparty is already selected
    if 'selected_counterparty' in st.session_state and st.session_state.selected_counterparty:
        counterparty_inn = st.session_state.selected_counterparty
        counterparty_name = st.session_state.get('selected_counterparty_name', counterparty_inn)

        # Header with button in the same row
        col1, col2 = st.columns([3, 1])
        with col1:
            st.header(t("risk_counterparty_header"))
        with col2:
            st.write("")  # Spacing
            if st.button(t("risk_another_counterparty_button"), type="secondary"):
                del st.session_state.selected_counterparty
                if 'selected_counterparty_name' in st.session_state:
                    del st.session_state.selected_counterparty_name
                st.rerun()

        st.divider()
        st.subheader(t("risk_counterparty_selected", counterparty=counterparty_name))

        # Calculate and use dynamic lookback period for this counterparty
        calculated_lookback = calculate_counterparty_lookback_period(
            user_id, counterparty_inn, invoice_type, as_of_date
        )
        
        # Use calculated lookback period
        months_back = calculated_lookback
        
        st.info(
            f"📅 Using calculated lookback period: {calculated_lookback} months "
            f"(based on latest invoice date)"
        )
        
        with st.spinner(t("risk_counterparty_spinner")):
            risk_profile = analyze_single_counterparty(
                user_id, counterparty_inn, invoice_type, as_of_date, months_back, config, lang
            )
        
        if risk_profile and 'error' not in risk_profile:
            # Display risk card
            display_risk_card(risk_profile, lang)
            
            # Behavioral features
            st.subheader(t("risk_behavioral_features"))
            features = risk_profile.get('behavioral_features', {})
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(t("risk_metric_total_invoiced_12m"), f"{features.get('total_invoiced_12m', 0):,.0f}")
                st.metric(t("risk_metric_total_paid"), f"{features.get('total_paid_12m', 0):,.0f}")
            
            with col2:
                st.metric(t("risk_metric_total_returns"), f"{features.get('total_returns_12m', 0):,.0f}")
                st.metric(t("risk_metric_total_unpaid"), f"{features.get('total_unpaid_current', 0):,.0f}")
            
            with col3:
                st.metric(t("risk_metric_max_dpd_paid"), f"{features.get('max_dpd_paid', 0):.0f} {tr('risk_days_suffix', lang)}")
                st.metric(t("risk_metric_weighted_avg_dpd"), f"{features.get('weighted_avg_dpd', 0):.0f} {tr('risk_days_suffix', lang)}")
            
            with col4:
                st.metric(t("risk_metric_return_ratio"), f"{features.get('return_ratio', 0):.2%}")
                st.metric(t("risk_metric_late_payment_rate"), f"{features.get('late_payment_rate', 0):.2%}")
            
            # Delinquency breakdown
            st.subheader(t("risk_delinquency_header"))
            delinq_data = {
                '0-30 days': 1.0 - features.get('share_exposure_gt30', 0),
                '31-60 days': features.get('share_exposure_gt30', 0) - features.get('share_exposure_gt60', 0),
                '61-90 days': features.get('share_exposure_gt60', 0) - features.get('share_exposure_gt90', 0),
                '91-180 days': features.get('share_exposure_gt90', 0) - features.get('share_exposure_gt180', 0),
                '180+ days': features.get('share_exposure_gt180', 0)
            }
            
            fig = px.bar(
                x=list(delinq_data.keys()),
                y=list(delinq_data.values()),
                labels={'x': t("risk_axis_aging_bucket"), 'y': t("risk_axis_exposure_share")},
                title=t("risk_chart_aging_distribution")
            )
            fig.update_yaxes(tickformat=".0%")
            st.plotly_chart(fig, use_container_width=True)
            
            # Justification
            st.subheader(t("risk_justification_header"))
            justification = risk_profile.get('justification', {})
            
            st.write(t("risk_pd_label"))
            st.info(justification.get('pd', tr("risk_not_available", lang)))
            
            st.write(t("risk_lgd_label"))
            st.info(justification.get('lgd', tr("risk_not_available", lang)))
            
            st.write(t("risk_ead_label"))
            st.info(justification.get('ead', tr("risk_not_available", lang)))
            
            # Export single report
            st.divider()
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button(t("risk_export_json_button")):
                    json_report = format_risk_report(risk_profile, 'json')
                    st.download_button(
                        t("risk_download_json_button"),
                        json_report,
                        file_name=f"risk_report_{counterparty_inn}_{as_of_date}.json",
                        mime="application/json"
                    )
            
            with col2:
                if st.button(t("risk_export_text_button")):
                    text_report = format_risk_report(risk_profile, 'text')
                    st.download_button(
                        t("risk_download_text_button"),
                        text_report,
                        file_name=f"risk_report_{counterparty_inn}_{as_of_date}.txt",
                        mime="text/plain"
                    )
        else:
            st.error(risk_profile.get('error', tr("risk_counterparty_error_generic", lang)))

    else:
        # Search interface - only show when no counterparty is selected
        st.header(t("risk_counterparty_header"))
        st.markdown(t("risk_search_title"))
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            search_term = st.text_input(
                t("risk_search_input_label"),
                placeholder=t("risk_search_input_placeholder"),
                key="search_input"
            )
        
        with col2:
            st.write("")  # Spacing
            st.write("")
            search_button = st.button(t("risk_search_button"), type="primary")
        
        # Show search results - use session state to persist results
        if search_term and search_button:
            with st.spinner(t("risk_search_spinner")):
                results = search_counterparties(user_id, search_term, invoice_type)
                st.session_state.search_results = results
        
        # Display results if they exist in session state
        if 'search_results' in st.session_state:
            results = st.session_state.search_results
            # Handle both DataFrame and list types
            if isinstance(results, pd.DataFrame) and not results.empty:
                st.success(t("risk_search_success", count=len(results)))
                st.dataframe(results, use_container_width=True)

                # Select counterparty
                selected_inn = st.selectbox(
                    t("risk_search_select_label"),
                    options=results['counterparty_inn'].tolist(),
                    format_func=lambda x: f"{x} - {results[results['counterparty_inn']==x]['counterparty_name'].iloc[0]}",
                    key="inn_selector"
                )

                if st.button(t("risk_search_analyze_button"), type="primary", key="analyze_btn"):
                    # Clean the INN - remove .0 if present
                    clean_inn = str(selected_inn).replace('.0', '').strip()
                    st.session_state.selected_counterparty = clean_inn
                    # Store the name too for better display
                    st.session_state.selected_counterparty_name = results[results['counterparty_inn']==selected_inn]['counterparty_name'].iloc[0]
                    # Clear search results
                    if 'search_results' in st.session_state:
                        del st.session_state.search_results
                    st.rerun()
            elif isinstance(results, pd.DataFrame) and results.empty:
                st.info(t("risk_search_no_results"))
            else:
                # Handle unexpected types (e.g., list)
                st.warning(t("risk_search_unexpected_format"))
                if 'search_results' in st.session_state:
                    del st.session_state.search_results
        elif search_term and search_button:
            st.info(t("risk_search_results_placeholder"))


def show_detailed_components(user_id, invoice_type, as_of_date, months_back, config, lang):
    """Display detailed invoice component analysis."""
    t = lambda key, **kwargs: tr(key, lang, **kwargs)
    st.header(t("risk_components_header"))
    
    if not st.session_state.selected_counterparty:
        st.info(t("risk_components_select_prompt"))
        return
    
    if True:  # Keep indentation
        counterparty_inn = st.session_state.selected_counterparty
        counterparty_name = st.session_state.get('selected_counterparty_name', counterparty_inn)
        
        st.subheader(t("risk_components_subheader", name=counterparty_name, inn=counterparty_inn))
        
        # Calculate and use dynamic lookback period for this counterparty
        calculated_lookback = calculate_counterparty_lookback_period(
            user_id, counterparty_inn, invoice_type, as_of_date
        )
        print(f"Calculated lookback period: {calculated_lookback}")
        
        # Use calculated lookback period
        months_back = calculated_lookback
        
        st.caption(f"📅 Using calculated lookback period: {calculated_lookback} months")
        
        # Run analysis to get components
        with st.spinner(t("risk_components_spinner")):
            invoices_df, payments_df = get_all_invoices_and_payments(
                user_id, invoice_type, months_back, as_of_date
            )
            
            if not invoices_df.empty:
                engine = RiskEngine(config, user_id)
                components = engine.reconstruct_invoice_components(
                    invoices_df, payments_df, invoice_type, as_of_date
                )
                
                # Filter for selected counterparty (normalize INNs for comparison)
                normalized_inn = str(counterparty_inn).replace('.0', '').strip()
                cp_components = []
                for c in components:
                    comp_inn = str(c.get('counterparty_inn', '')).replace('.0', '').strip()
                    if comp_inn == normalized_inn:
                        cp_components.append(c)
                
                if cp_components:
                    df = pd.DataFrame(cp_components)
                    df['component_amount'] = df['component_amount'].astype(float)
                    df['component_amount_effective'] = df['component_amount'].copy()
                    
                    # Sort by resolution date (chronological flow)
                    df['resolution_date_sort'] = pd.to_datetime(df['resolution_date'])
                    # Put open (unpaid) items at the end
                    df['is_open'] = df['component_type'] == 'open'
                    df = df.sort_values(['is_open', 'resolution_date_sort']).drop(['resolution_date_sort', 'is_open'], axis=1).reset_index(drop=True)
                    
                    # Generate descriptive notes
                    def generate_notes(row):
                        component_type = row['component_type']
                        dpd = row['dpd']
                        
                        if component_type == 'returned':
                            if dpd > 180:
                                return t("risk_note_return_severe")
                            elif dpd > 90:
                                return t("risk_note_return_delay")
                            else:
                                return t("risk_note_return_normal")
                        elif component_type == 'paid':
                            if dpd < 0:
                                return t("risk_note_paid_early")
                            elif dpd == 0:
                                return t("risk_note_paid_on_time")
                            elif dpd <= 30:
                                return t("risk_note_minor_delay")
                            elif dpd <= 60:
                                return t("risk_note_paid_late_cured")
                            elif dpd <= 90:
                                return t("risk_note_paid_late")
                            else:
                                return t("risk_note_paid_very_late")
                        else:  # open
                            if dpd > 180:
                                return t("risk_note_unpaid_major")
                            elif dpd > 90:
                                return t("risk_note_unpaid_severe")
                            elif dpd > 60:
                                return t("risk_note_unpaid_late")
                            elif dpd > 30:
                                return t("risk_note_unpaid_moderate")
                            else:
                                return t("risk_note_unpaid_recent")
                    
                    def adjust_paid_components_for_returns(invoice_subset):
                        """Adjust paid component amounts by retracting later returns."""
                        subset = invoice_subset.copy()
                        if 'component_amount_effective' not in subset.columns:
                            subset['component_amount_effective'] = subset['component_amount'].astype(float)
                        returns_subset = subset[subset['component_type'] == 'returned'].sort_values('resolution_date')
                        if returns_subset.empty:
                            return subset

                        for _, ret_row in returns_subset.iterrows():
                            ret_amount = float(ret_row.get('component_amount') or 0)
                            if ret_amount <= 0:
                                continue

                            ret_date = ret_row.get('resolution_date')
                            paid_candidates = subset[subset['component_type'] == 'paid']

                            if pd.notnull(ret_date):
                                paid_candidates = paid_candidates[pd.notnull(paid_candidates['resolution_date'])]
                                paid_candidates = paid_candidates[paid_candidates['resolution_date'] <= ret_date]

                            paid_candidates = paid_candidates.sort_values('resolution_date', ascending=False)

                            for paid_idx, paid_row in paid_candidates.iterrows():
                                available = float(subset.at[paid_idx, 'component_amount_effective'] or 0)
                                if available <= 0:
                                    continue

                                deduction = min(available, ret_amount)
                                subset.at[paid_idx, 'component_amount_effective'] = max(available - deduction, 0)
                                ret_amount -= deduction

                                if ret_amount <= 0:
                                    break

                        return subset

                    # Calculate total invoice amount for each invoice from original invoice data
                    # Exclude returns - they are credit notes, not part of original invoice total
                    # Filter invoices_df for the selected counterparty
                    if invoice_type == 'OUT':
                        inn_col = 'buyer_inn'
                    else:
                        inn_col = 'seller_inn'
                    
                    # Normalize INN for comparison
                    cp_invoices = invoices_df[
                        invoices_df[inn_col].astype(str).str.replace('.0', '', regex=False).str.strip() == normalized_inn
                    ].copy()
                    
                    # Exclude return documents (they have 'возврат' or 'return' in document_number)
                    cp_invoices = cp_invoices[
                        ~cp_invoices['document_number'].astype(str).str.contains('возврат|return', case=False, regex=True, na=False)
                    ]
                    
                    # Calculate original invoice totals (sum by document_number, excluding returns)
                    invoice_totals = cp_invoices.groupby('document_number')['total_amount'].sum().to_dict()
                    
                    # For any invoices not found in original data, fall back to summing non-returned components
                    for invoice_num in df['invoice_number'].unique():
                        if invoice_num not in invoice_totals:
                            # Sum only paid + open components (exclude returned)
                            non_returned_sum = df[
                                (df['invoice_number'] == invoice_num) & 
                                (df['component_type'] != 'returned')
                            ]['component_amount'].sum()
                            if non_returned_sum > 0:
                                invoice_totals[invoice_num] = non_returned_sum
                    
                    # Calculate running outstanding amounts
                    # Outstanding_before: balance BEFORE applying this transaction
                    # Outstanding: balance AFTER applying this transaction
                    df['outstanding_before'] = 0.0
                    df['outstanding'] = 0.0

                    invoice_pd_percent_map = {}
                    component_pd_contribution_map = {}

                    total_weighted_sum = 0.0
                    total_time_horizon = 0.0

                    for invoice_num in df['invoice_number'].unique():
                        mask = df['invoice_number'] == invoice_num
                        invoice_df = df[mask].copy()

                        # Recalibrate paid components by retracting return amounts from the most recent payments
                        invoice_df = adjust_paid_components_for_returns(invoice_df)
                        df.loc[invoice_df.index, 'component_amount_effective'] = invoice_df['component_amount_effective']

                        # Get invoice total, with fallback if not found in original data
                        if invoice_num in invoice_totals:
                            invoice_total = invoice_totals[invoice_num]
                        else:
                            # Fallback: sum non-returned components (paid + open)
                            non_returned_sum = df[
                                (df['invoice_number'] == invoice_num) & 
                                (df['component_type'] != 'returned')
                            ]['component_amount'].sum()
                            invoice_total = non_returned_sum if non_returned_sum > 0 else df[mask]['component_amount'].sum()
                            # Store it for future use
                            invoice_totals[invoice_num] = invoice_total
                        
                        running_balance = invoice_total
                        outstanding_before_list = []
                        outstanding_after_list = []

                        for idx, row in invoice_df.iterrows():
                            outstanding_before_list.append(running_balance)

                            if row['component_type'] == 'paid':
                                amount_to_apply = row.get('component_amount_effective', row['component_amount'])
                                running_balance -= amount_to_apply
                                running_balance = max(running_balance, 0)
                            elif row['component_type'] == 'returned':
                                # Return increases outstanding (money not collected)
                                running_balance -= row['component_amount']
                            # For 'open', outstanding remains the same

                            outstanding_after_list.append(running_balance)

                        # Update both columns for this invoice
                        df.loc[mask, 'outstanding_before'] = outstanding_before_list
                        df.loc[mask, 'outstanding'] = outstanding_after_list

                        invoice_components_for_pd = []
                        for _, comp_row in invoice_df.iterrows():
                            invoice_components_for_pd.append({
                                'component_type': comp_row.get('component_type'),
                                'component_amount_effective': comp_row.get('component_amount_effective', comp_row.get('component_amount')),
                                'component_amount': comp_row.get('component_amount'),
                                'resolution_date': comp_row.get('resolution_date'),
                                'dpd': comp_row.get('dpd'),
                                '_row_id': comp_row.name
                            })

                        pd_result = calculate_invoice_pd(
                            invoice_components_for_pd,
                            invoice_total,
                            default_days=180,
                            analysis_date=as_of_date
                        )
                        invoice_pd_percent = pd_result.get('pd_percent', 0.0)
                        invoice_pd_percent_map[invoice_num] = invoice_pd_percent
                        weighted_sum = pd_result.get('weighted_sum', 0.0) or 0.0
                        T_value = pd_result.get('time_horizon', 0.0) or 0.0  # The 'T' value

                        total_weighted_sum += weighted_sum
                        total_time_horizon += T_value

                        for breakdown in pd_result.get('component_breakdown', []):
                            row_id = breakdown.get('source_row_id')
                            if row_id is None:
                                continue
                            contribution_amount = breakdown.get('contribution', 0.0) or 0.0
                            percent_value = (
                                (contribution_amount / weighted_sum) * invoice_pd_percent
                                if weighted_sum else 0.0
                            )
                            component_pd_contribution_map[row_id] = percent_value
                    
                    # Add a column showing the sequence within each invoice
                    df['sequence'] = df.groupby('invoice_number').cumcount() + 1
                    df['total_in_group'] = df.groupby('invoice_number')['invoice_number'].transform('count')
                    
                    # Map invoice totals with fallback for missing invoices
                    def get_invoice_total(inv_num):
                        if inv_num in invoice_totals:
                            return invoice_totals[inv_num]
                        # Fallback: sum non-returned components for this invoice
                        mask = df['invoice_number'] == inv_num
                        non_returned_sum = df[mask & (df['component_type'] != 'returned')]['component_amount'].sum()
                        return non_returned_sum if non_returned_sum > 0 else df[mask]['component_amount'].sum()
                    
                    df['invoice_total'] = df['invoice_number'].apply(get_invoice_total)
                    
                    # Create the detailed component table with better formatting
                    def format_match_method(row):
                        method = row.get('payment_method', '—')
                        contract = row.get('contract_number', '')
                        
                        if method == 'contract_match' and contract:
                            return t("risk_match_contract_with_number", contract=str(contract))
                        elif method == 'contract_match':
                            return t("risk_match_contract")
                        elif method == 'fifo':
                            return t("risk_match_fifo")
                        else:
                            return t("risk_match_not_applicable")
                    
                    df['component_amount_display'] = df.apply(
                        lambda row: row['component_amount_effective']
                        if row['component_type'] == 'paid'
                        else row['component_amount'],
                        axis=1
                    )

                    def format_pd_percent(value):
                        if value is None:
                            return '—'
                        return f"{value:,.2f}%".replace('.', ',')

                    df['pd_contribution_value'] = df.index.map(
                        lambda idx: component_pd_contribution_map.get(idx, 0.0)
                    )
                    df['pd_cumulative_value'] = df.groupby('invoice_number')['pd_contribution_value'].cumsum()

                    display_df = pd.DataFrame({
                        'Invoice ID': pd.to_numeric(df['invoice_number'], errors='coerce'),
                        'Invoice Date': df['invoice_date'].apply(lambda x: x.strftime('%d.%m.%Y') if pd.notnull(x) else '—'),
                        'Invoice Total': df['invoice_total'].apply(lambda x: f"{x:,.0f}"),
                        'Invoice PBI (%)': df['invoice_number'].apply(lambda inv: f"{invoice_pd_percent_map.get(inv, 0):.2f}%"),
                        'Contract #': df.get('contract_number', pd.Series(['—'] * len(df))).apply(
                            lambda x: str(x).strip() if pd.notnull(x) and str(x).strip().lower() not in ['', 'nan', 'none'] else '—'
                        ),
                        'Component': df['component_type'].map({
                            'paid': '✅ Paid',
                            'returned': '↩️ Returned',
                            'open': '⏳ Unpaid'
                        }),
                        'Component Amt': df['component_amount_display'].apply(lambda x: f"{x:,.0f}"),
                        'Outstanding_before': df['outstanding_before'].apply(lambda x: f"{x:,.0f}"),
                        'Outstanding_after': df['outstanding'].apply(lambda x: f"{x:,.0f}"),
                        'Resolution Date': df.apply(
                            lambda row: row['resolution_date'].strftime('%d.%m.%Y')
                            if pd.notnull(row['resolution_date']) and row['component_type'] != 'open'
                            else '—', axis=1
                        ),
                        'Due Date': df['due_date'].apply(lambda x: x.strftime('%d.%m.%Y') if pd.notnull(x) else '—'),
                        'Days Past Due': df['dpd'].apply(lambda x: f"{int(x)}"),
                        'Aging Bucket': df['aging_bucket'],
                        'Match Method': df.apply(format_match_method, axis=1),
                        'Payment Purpose': df.get('payment_purpose', pd.Series(['—'] * len(df))).apply(
                            lambda x: str(x)[:100] + '...' if len(str(x)) > 100 else (str(x) if x and x != 'nan' else '—')
                        ),
                        'Notes': df.apply(generate_notes, axis=1),
                        'Part': df.apply(
                            lambda row: f"{int(row['sequence'])}/{int(row['total_in_group'])}",
                            axis=1
                        ),
                        'PBI Contribution (%)': df['pd_contribution_value'].apply(
                            lambda val: format_pd_percent(val) if val else '—'
                        )
                    })
                    
                    # Reorder columns for better display
                    display_df = display_df[[
                        'Invoice ID', 'Invoice Date', 'Invoice Total', 'Invoice PBI (%)', 'Contract #', 'Part', 'Component', 'Component Amt',
                        'Outstanding_before', 'Outstanding_after', 'Resolution Date', 'Due Date', 'Days Past Due', 'Aging Bucket',
                        'PBI Contribution (%)', 'Match Method', 'Payment Purpose', 'Notes'
                    ]]
                    
                    # Filters in expander
                    with st.expander("🔍 Filter Options", expanded=False):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            component_filter = st.multiselect(
                                "Component Type",
                                options=['✅ Paid', '↩️ Returned', '⏳ Unpaid'],
                                default=['✅ Paid', '↩️ Returned', '⏳ Unpaid'],
                                key="component_filter"
                            )
                        
                        with col2:
                            bucket_filter = st.multiselect(
                                "Aging Bucket",
                                options=sorted(df['aging_bucket'].unique().tolist()),
                                default=df['aging_bucket'].unique().tolist(),
                                key="bucket_filter"
                            )
                    
                    # Apply filters
                    filtered_df = display_df[
                        display_df['Component'].isin(component_filter) &
                        display_df['Aging Bucket'].isin(bucket_filter)
                    ]
                    
                    # Display the main component table
                    st.markdown("### 📊 Invoice Component Breakdown (Chronological Flow)")
                    
                    # Show allocation strategy info
                    paid_comps = df[df['component_type'] == 'paid']
                    if not paid_comps.empty and 'payment_method' in paid_comps.columns:
                        contract_matched = (paid_comps['payment_method'] == 'contract_match').sum()
                        fifo_matched = (paid_comps['payment_method'] == 'fifo').sum()
                        total_paid_amt = paid_comps['component_amount'].sum()
                        
                        st.caption(f"""
                        **Payment Allocation:** {contract_matched} by contract 📄 ({total_paid_amt:,.0f} soʻm), 
                        {fifo_matched} by FIFO 🔄 | 
                        Sorted chronologically by resolution date | 
                        Showing {len(filtered_df)} of {len(display_df)} components
                        """)
                    else:
                        st.caption(f"Showing {len(filtered_df)} of {len(display_df)} components | Sorted chronologically")
                    
                    # Style the dataframe with color coding
                    def highlight_row(row):
                        dpd = int(row['Days Past Due'])
                        component = row['Component']
                        
                        if '↩️ Returned' in component:
                            if dpd > 180:
                                return ['background-color: #dc3545; color: white'] * len(row)  # Red for severe
                            else:
                                return ['background-color: #ffc107'] * len(row)  # Yellow
                        elif '⏳ Unpaid' in component:
                            if dpd > 180:
                                return ['background-color: #dc3545; color: white'] * len(row)  # Red
                            elif dpd > 90:
                                return ['background-color: #fd7e14; color: white'] * len(row)  # Orange
                            elif dpd > 60:
                                return ['background-color: #ffc107'] * len(row)  # Yellow
                            else:
                                return ['background-color: #fff3cd'] * len(row)  # Light yellow
                        elif '✅ Paid' in component:
                            if dpd > 90:
                                return ['background-color: #f8d7da'] * len(row)  # Light red
                            elif dpd > 60:
                                return ['background-color: #fff3cd'] * len(row)  # Light yellow
                            else:
                                return ['background-color: #d4edda'] * len(row)  # Light green
                        return [''] * len(row)
                    
                    styled_df = filtered_df.style.apply(highlight_row, axis=1)
                    st.dataframe(
                        styled_df,
                        use_container_width=True,
                        hide_index=True,
                        height=500
                    )
                    
                    # Add legends
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("""
                        **Color Legend:**
                        - 🟢 Green: Paid on time/minor delay (≤60 DPD)
                        - 🟡 Yellow: Moderate delay (61-90 DPD)
                        - 🟠 Orange: Severe unpaid (91-180 DPD)
                        - 🔴 Red: Critical (>180 DPD)
                        """)
                    
                    with col2:
                        st.markdown("""
                        **Payment Matching:**
                        - 📄 Contract: Matched by contract number
                        - 🔄 FIFO: First-In-First-Out allocation
                        - —: Not applicable (unpaid/returned)
                        """)

                                            # Summary metrics in cards
                    
                    st.divider()
                    st.markdown("### PBI Information")
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric(
                            "Total PBI (%)", 
                            f"{(total_weighted_sum / total_time_horizon) * 100:.2f}%"
                        )
                    
                    with col2:
                        st.metric(
                            "Total A (Σ OB×ΔDPD)", 
                            f"{total_weighted_sum:,.0f}"
                        )
                    with col3:
                        st.metric(
                            "Total T (Total×180)", 
                            f"{total_time_horizon:,.0f}"
                        )
                    
                    # Summary metrics in cards
                    st.divider()
                    st.markdown("### 💰 Financial Summary")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    
                    # Calculate total invoiced from original invoice amounts (excluding returns)
                    # Returns are credit notes, not part of original invoice total
                    total_amount = sum(invoice_totals.values()) if invoice_totals else 0
                    paid_total = df[df['component_type'] == 'paid']['component_amount_display'].sum()
                    returned_total = df[df['component_type'] == 'returned']['component_amount'].sum()
                    unpaid_total = df[df['component_type'] == 'open']['component_amount'].sum()
                    
                    with col1:
                        st.metric(
                            "Total Invoiced", 
                            f"{total_amount:,.0f}",
                            help="Sum of original invoice amounts (excluding returns)"
                        )
                    
                    with col2:
                        st.metric(
                            "Total Paid", 
                            f"{paid_total:,.0f}",
                            delta=f"{paid_total/total_amount*100:.1f}%",
                            delta_color="normal"
                        )
                    
                    with col3:
                        st.metric(
                            "Total Returned", 
                            f"{returned_total:,.0f}",
                            delta=f"{returned_total/total_amount*100:.1f}%",
                            delta_color="inverse"
                        )
                    
                    with col4:
                        st.metric(
                            "Total Unpaid", 
                            f"{unpaid_total:,.0f}",
                            delta=f"{unpaid_total/total_amount*100:.1f}%",
                            delta_color="inverse"
                        )
                    
                    # Detailed statistics
                    st.markdown("### 📈 Detailed Statistics")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**By Component Type:**")
                        component_summary = df.groupby('component_type').agg({
                            'component_amount': ['sum', 'count', 'mean'],
                            'dpd': ['mean', 'max']
                        }).round(0)
                        component_summary.columns = ['Total Amount', 'Count', 'Avg Amount', 'Avg DPD', 'Max DPD']
                        component_summary.index = component_summary.index.map({
                            'paid': 'Paid',
                            'returned': 'Returned',
                            'open': 'Unpaid'
                        })
                        st.dataframe(component_summary, use_container_width=True)
                    
                    with col2:
                        st.markdown("**By Aging Bucket:**")
                        aging_summary = df.groupby('aging_bucket').agg({
                            'component_amount': ['sum', 'count']
                        }).round(0)
                        aging_summary.columns = ['Total Amount', 'Count']
                        aging_summary['% of Total'] = (aging_summary['Total Amount'] / total_amount * 100).round(1)
                        st.dataframe(aging_summary, use_container_width=True)
                    
                    # Payment behavior insights
                    if not df[df['component_type'] == 'paid'].empty:
                        st.markdown("### 💡 Payment Behavior Insights")
                        paid_df = df[df['component_type'] == 'paid']
                        
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            avg_dpd = paid_df['dpd'].mean()
                            st.metric("Average DPD (Paid)", f"{avg_dpd:.0f} days")
                        
                        with col2:
                            on_time_rate = (paid_df['dpd'] <= 30).sum() / len(paid_df) * 100
                            st.metric("On-Time Rate", f"{on_time_rate:.1f}%")
                        
                        with col3:
                            late_90_plus = (paid_df['dpd'] > 90).sum()
                            st.metric("Payments >90 DPD", f"{late_90_plus}")
                    
                    # Export options
                    st.divider()
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        csv = filtered_df.to_csv(index=False)
                        st.download_button(
                            "📥 Export to CSV",
                            csv,
                            file_name=f"invoice_components_{counterparty_inn}_{as_of_date.strftime('%Y%m%d')}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                    
                    with col2:
                        # Create Excel with multiple sheets
                        import io
                        from io import BytesIO
                        
                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            filtered_df.to_excel(writer, sheet_name='Components', index=False)
                            component_summary.to_excel(writer, sheet_name='Summary')
                            aging_summary.to_excel(writer, sheet_name='Aging Analysis')
                        
                        st.download_button(
                            "📊 Export to Excel",
                            output.getvalue(),
                            file_name=f"invoice_analysis_{counterparty_inn}_{as_of_date.strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
                else:
                    st.warning(f"⚠️ No components found for counterparty {counterparty_inn}")
                    st.info(
                        f"This could mean:\n"
                        f"- No invoices in the selected time period (lookback: {months_back} months, as of: {as_of_date})\n"
                        f"- The counterparty was found in search but has no invoice data matching current filters\n"
                        f"- Try adjusting the 'Lookback Period' or 'Analysis Date' in the sidebar"
                    )
            else:
                st.warning("⚠️ No invoice data available in the database")
                st.info("Please upload invoice and payment data first")


def show_batch_analysis(user_id, invoice_type, as_of_date, months_back, config, lang):
    """Run batch analysis for multiple counterparties."""
    t = lambda key, **kwargs: tr(key, lang, **kwargs)
    st.header(t("risk_batch_header"))
    
    # Get all counterparties
    counterparties = get_all_counterparties(user_id, invoice_type, months_back, as_of_date)
    
    if counterparties.empty:
        st.info(t("risk_batch_no_data"))
        return
    
    st.write(t("risk_batch_found_count", count=len(counterparties)))
    st.dataframe(counterparties.head(10), use_container_width=True)
    
    # Batch analysis options
    col1, col2 = st.columns(2)
    
    with col1:
        top_n = st.number_input(
            t("risk_batch_top_n_label"),
            min_value=1,
            max_value=len(counterparties),
            value=min(10, len(counterparties)),
            help=t("risk_batch_top_n_help")
        )
    
    with col2:
        min_exposure = st.number_input(
            t("risk_batch_min_exposure_label"),
            min_value=0,
            value=0,
            step=10000,
            help=t("risk_batch_min_exposure_help")
        )
    
    # Run batch analysis
    if st.button(t("risk_batch_run_button"), type="primary"):
        # Filter counterparties
        filtered_cp = counterparties[counterparties['total_invoiced'] >= min_exposure]
        filtered_cp = filtered_cp.nlargest(top_n, 'total_invoiced')
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        results = {}
        
        for idx, row in enumerate(filtered_cp.iterrows()):
            _, cp = row
            counterparty_inn = cp['counterparty_inn']
            
            status_text.text(t("risk_batch_status", current=idx + 1, total=len(filtered_cp), inn=counterparty_inn))
            
            try:
                risk_profile = analyze_single_counterparty(
                    user_id, counterparty_inn, invoice_type, as_of_date, months_back, config, lang
                )
                results[counterparty_inn] = risk_profile
            except Exception as e:
                st.warning(t("risk_batch_error", inn=counterparty_inn, error=str(e)))
            
            progress_bar.progress((idx + 1) / len(filtered_cp))
        
        status_text.text(t("risk_batch_complete"))
        st.session_state.risk_results = results
        
        # Display results summary
        if results:
            results_df = pd.DataFrame(list(results.values()))
            
            st.success(t("risk_batch_success", count=len(results)))
            
            # Summary table
            summary_df = results_df[[
                'counterparty_inn', 'rating', 'pd', 'lgd', 'ead_current',
                'expected_loss', 'recommended_limit'
            ]].copy()
            
            # Format percentages
            summary_df['pd'] = summary_df['pd'].apply(lambda x: f"{x:.2%}")
            summary_df['lgd'] = summary_df['lgd'].apply(lambda x: f"{x:.2%}")
            
            st.dataframe(summary_df, use_container_width=True)
            
            # Export batch results
            if st.button(t("risk_batch_export_button")):
                output_path = f"risk_batch_{invoice_type}_{as_of_date}.xlsx"
                if export_to_excel(list(results.values()), output_path):
                    st.success(t("risk_batch_export_success", path=output_path))
                    with open(output_path, 'rb') as f:
                        st.download_button(
                            t("risk_download_excel_report"),
                            f.read(),
                            file_name=output_path,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )


def calculate_invoice_pd(invoice_components, invoice_total, default_days=180, analysis_date=None):
    """Calculate PD for a single invoice using outstanding balances and DPD."""
    def normalize_date(raw_date):
        if isinstance(raw_date, pd.Timestamp):
            return raw_date.date()
        if isinstance(raw_date, datetime):
            return raw_date.date()
        if isinstance(raw_date, date):
            return raw_date
        return None

    invoice_total = float(invoice_total or 0)
    default_days = default_days or 0
    normalized_analysis_date = normalize_date(analysis_date) or date.today()

    result = {
        'pd_ratio': 0.0,
        'pd_percent': 0.0,
        'weighted_sum': 0.0,
        'time_horizon': invoice_total * default_days if invoice_total > 0 and default_days > 0 else 0.0,
        'component_breakdown': []
    }

    if invoice_total <= 0 or default_days <= 0 or not invoice_components:
        return result

    processed_components = []
    for idx, comp in enumerate(invoice_components):
        comp_type = comp.get('component_type', 'unknown')
        amount = comp.get('component_amount_effective', comp.get('component_amount'))
        try:
            amount = float(amount or 0)
        except (TypeError, ValueError):
            amount = 0.0

        if amount == 0 and comp_type != 'open':
            continue

        dpd_value = comp.get('dpd', 0)
        try:
            dpd_value = float(dpd_value or 0)
        except (TypeError, ValueError):
            dpd_value = 0.0

        sort_date = normalize_date(comp.get('resolution_date'))
        if sort_date is None:
            sort_date = normalized_analysis_date

        processed_components.append({
            'original_index': idx,
            'component_type': comp_type,
            'amount': amount,
            'dpd': dpd_value,
            'sort_date': sort_date,
            'source_row_id': comp.get('_row_id')
        })

    if not processed_components:
        return result

    processed_components.sort(key=lambda c: (c['sort_date'], c['dpd'], c['original_index']))

    current_outstanding = invoice_total
    enriched_components = []
    for comp in processed_components:
        outstanding_before = current_outstanding
        comp_type = comp['component_type']

        if comp_type == 'paid':
            current_outstanding = max(current_outstanding - comp['amount'], 0)
        elif comp_type == 'returned':
            current_outstanding += comp['amount']

        enriched_components.append({
            'component_type': comp_type,
            'dpd': comp['dpd'],
            'outstanding_before': outstanding_before,
            'source_row_id': comp.get('source_row_id')
        })

    weighted_sum = 0.0
    prev_dpd = 0.0
    breakdown = []

    for idx, comp in enumerate(enriched_components):
        dpd_value = comp['dpd']
        delta_dpd = dpd_value - prev_dpd if idx > 0 else dpd_value
        contribution = comp['outstanding_before'] * delta_dpd
        weighted_sum += contribution
        breakdown.append({
            'component_type': comp['component_type'],
            'dpd': dpd_value,
            'delta_dpd': delta_dpd,
            'outstanding_before': comp['outstanding_before'],
            'contribution': contribution,
            'source_row_id': comp.get('source_row_id')
        })
        prev_dpd = dpd_value

    time_horizon = invoice_total * default_days
    pd_ratio = weighted_sum / time_horizon if time_horizon else 0.0
    pd_percent = pd_ratio * 100

    result.update({
        'pd_ratio': pd_ratio,
        'pd_percent': pd_percent,
        'weighted_sum': weighted_sum,
        'time_horizon': time_horizon,
        'component_breakdown': breakdown
    })

    return result


def show_daily_aging_dashboard(user_id, invoice_type, as_of_date, config, lang):
    """Dedicated dashboard for daily aging analysis tied to a specific contract."""
    t = lambda key, **kwargs: tr(key, lang, **kwargs)
    target_contract = 'PБПRV/1'
    st.header(t("risk_daily_header", contract=target_contract))
    
    if invoice_type != 'OUT':
        st.info(t("risk_daily_only_out_info"))
        return
    
    with st.spinner(t("risk_daily_spinner")):
        invoices_df, payments_df = get_invoices_with_payments(
            user_id,
            contract_number=target_contract,
            invoice_type=invoice_type
        )
    
    if invoices_df.empty:
        st.warning(t("risk_daily_no_invoices", contract=target_contract))
        return
    
    invoices_df = invoices_df.copy()
    invoices_df['document_number'] = invoices_df['document_number'].astype(str).str.strip()
    invoices_df['document_date'] = pd.to_datetime(
        invoices_df['document_date'], errors='coerce'
    )
    
    def first_non_empty(series):
        for value in series:
            if pd.notna(value) and str(value).strip() != '':
                return value
        return ''
    
    grouped_invoices = (
        invoices_df
        .sort_values('document_date')
        .groupby('document_number', as_index=False)
        .agg({
            'invoice_id': 'first',
            'document_date': 'min',
            'total_amount': 'sum',
            'contract_number': first_non_empty,
            'buyer_name': first_non_empty,
            'buyer_inn': first_non_empty,
            'seller_name': first_non_empty,
            'seller_inn': first_non_empty
        })
    )
    
    engine = RiskEngine(config, user_id)
    components = engine.reconstruct_invoice_components(
        invoices_df, payments_df, invoice_type, as_of_date
    )
    
    if not components:
        st.warning(t("risk_daily_no_components"))
        return
    
    series_list = []
    summaries = []
    pd_calculations = []

    for _, invoice_row in grouped_invoices.iterrows():
        doc_number = str(invoice_row.get('document_number') or invoice_row.get('invoice_id'))
        inv_components = [c for c in components if str(c.get('invoice_number')) == doc_number]
        series_df, summary = build_daily_aging_series(invoice_row, inv_components, as_of_date)
        
        if series_df is not None and not series_df.empty:
            series_list.append(series_df)
        if summary:
            pd_result = calculate_invoice_pd(
                inv_components,
                summary.get('invoice_total'),
                default_days=180,
                analysis_date=as_of_date
            )
            summary['pd_percent'] = pd_result['pd_percent']
            summary['pd_inputs'] = {
                'weighted_sum': pd_result['weighted_sum'],
                'time_horizon': pd_result['time_horizon']
            }
            pd_calculations.append({
                'Invoice ID': summary['invoice_number'],
                'Invoice Total': summary['invoice_total'],
                'A (Σ OB×ΔDPD)': pd_result['weighted_sum'],
                'T (Total×180)': pd_result['time_horizon'],
                'PD (%)': pd_result['pd_percent']
            })
            summaries.append(summary)
        
    if not series_list:
        st.warning(t("risk_daily_no_activity"))
        return
    
    daily_df = pd.concat(series_list, ignore_index=True)
    contract_daily_df = build_contract_daily_series(daily_df, invoices_df, as_of_date)
    
    if contract_daily_df.empty:
        st.warning(t("risk_daily_no_timeline"))
        return
    
    if summaries:
        st.subheader(t("risk_daily_invoice_snapshot"))
        for summary in summaries:
            with st.container():
                issue_date = summary['issue_date']
                closing_date = summary['closing_date']
                closing_text = closing_date.strftime('%d.%m.%Y') if closing_date else "Open"
                header = t("risk_daily_invoice_header", invoice=summary['invoice_number'], contract=summary.get('contract_number') or '—')
                st.markdown(header)
                
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    st.metric(t("risk_metric_total_amount"), f"{summary['invoice_total']:,.0f}")
                with col2:
                    st.metric(t("risk_metric_outstanding"), f"{summary['outstanding_end']:,.0f}")
                with col3:
                    st.metric(t("risk_metric_tracked_days"), summary['days_recorded'])
                with col4:
                    status = t("risk_metric_status_closed") if closing_date else t("risk_metric_status_open")
                    st.metric(t("risk_metric_status"), status, closing_text if closing_date else "—")
                with col5:
                    st.metric(t("risk_metric_pd_at"), f"{summary.get('pd_percent', 0):.2f}%")
                
                st.caption(
                    t("risk_daily_caption_summary",
                      buyer=summary.get('buyer_name') or '—',
                      issue=issue_date.strftime('%d.%m.%Y'),
                      closed=closing_text,
                      a=summary.get('pd_inputs', {}).get('weighted_sum', 0),
                      t=summary.get('pd_inputs', {}).get('time_horizon', 0))
                )
    
    st.subheader(t("risk_daily_chart_header"))
    chart_df = contract_daily_df.copy()
    chart_df['date'] = pd.to_datetime(chart_df['date'])
    fig = px.line(
        chart_df,
        x='date',
        y='closing_balance',
        markers=True,
        labels={'closing_balance': t("risk_daily_axis_outstanding"), 'date': t("risk_daily_axis_date")}
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader(t("risk_daily_table_header"))
    table_df_display = contract_daily_df.copy()
    table_df_display['date'] = table_df_display['date'].apply(lambda x: x.strftime('%d.%m.%Y'))
    numeric_cols = ['opening_balance', 'change', 'closing_balance']
    table_df_display[numeric_cols] = (
        table_df_display[numeric_cols]
        .apply(pd.to_numeric, errors='coerce')
        .round(2)
    )
    
    display_table = table_df_display.rename(columns={
            'day': t("risk_daily_table_col_day"),
            'date': t("risk_daily_table_col_date"),
            'opening_balance': t("risk_daily_table_col_opening"),
            'change': t("risk_daily_table_col_change"),
            'closing_balance': t("risk_daily_table_col_closing")
        })
    st.dataframe(
        display_table[
            [
                t("risk_daily_table_col_day"),
                t("risk_daily_table_col_date"),
                t("risk_daily_table_col_opening"),
                t("risk_daily_table_col_change"),
                t("risk_daily_table_col_closing")
            ]
        ],
        use_container_width=True,
        height=420
    )
    
    st.caption(t("risk_daily_table_caption"))

    if pd_calculations:
        st.subheader(t("risk_daily_pd_table_header"))
        pd_df = pd.DataFrame(pd_calculations)
        display_pd_df = pd_df.copy()
        display_pd_df['Invoice Total'] = display_pd_df['Invoice Total'].apply(lambda x: f"{x:,.0f}")
        display_pd_df['A (Σ OB×ΔDPD)'] = display_pd_df['A (Σ OB×ΔDPD)'].apply(lambda x: f"{x:,.0f}")
        display_pd_df['T (Total×180)'] = display_pd_df['T (Total×180)'].apply(lambda x: f"{x:,.0f}")
        display_pd_df['PD (%)'] = display_pd_df['PD (%)'].apply(lambda x: f"{x:.2f}%")

        st.dataframe(display_pd_df, use_container_width=True, hide_index=True)

        pd_values = pd_df['PD (%)']
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(t("risk_daily_avg_pd"), f"{pd_values.mean():.2f}%")
        with col2:
            st.metric(t("risk_daily_max_pd"), f"{pd_values.max():.2f}%")
        with col3:
            st.metric(t("risk_daily_min_pd"), f"{pd_values.min():.2f}%")

        st.caption(t("risk_daily_pd_caption"))


def build_daily_aging_series(invoice_row, invoice_components, as_of_date):
    """Create a per-day outstanding balance series for a single invoice."""
    invoice_total = float(invoice_row.get('total_amount') or 0)
    if invoice_total <= 0:
        return pd.DataFrame(), None
    
    invoice_id = invoice_row.get('invoice_id')
    doc_number = invoice_row.get('document_number') or invoice_id
    doc_number = str(doc_number)
    
    issue_date = pd.to_datetime(invoice_row.get('document_date'), errors='coerce')
    if pd.isna(issue_date):
        return pd.DataFrame(), None
    issue_date = issue_date.date()
    
    if isinstance(as_of_date, datetime):
        analysis_date = as_of_date.date()
    elif isinstance(as_of_date, date):
        analysis_date = as_of_date
    else:
        analysis_date = date.today()
    
    events_by_date = {}
    total_paid = 0.0
    total_returned = 0.0
    
    for comp in invoice_components:
        comp_type = comp.get('component_type')
        comp_amount = float(comp.get('component_amount', 0) or 0)
        comp_date = comp.get('resolution_date')
        if pd.isna(comp_date) or comp_amount == 0:
            continue
        if isinstance(comp_date, pd.Timestamp):
            comp_date = comp_date.date()
        elif isinstance(comp_date, datetime):
            comp_date = comp_date.date()
        
        if comp_date < issue_date:
            continue
        
        if comp_type == 'paid':
            delta = -comp_amount
            total_paid += comp_amount
            label = f"Payment {comp_amount:,.0f}"
        elif comp_type == 'returned':
            delta = comp_amount
            total_returned += comp_amount
            label = f"Return {comp_amount:,.0f}"
        else:
            continue
        
        events_by_date.setdefault(comp_date, []).append({
            'delta': delta,
            'label': label,
            'amount': comp_amount,
            'type': comp_type
        })
    
    max_event_date = max(events_by_date.keys()) if events_by_date else issue_date
    net_change = sum(event['delta'] for events in events_by_date.values() for event in events)
    projected_balance = max(invoice_total + net_change, 0)
    end_date = max_event_date
    if projected_balance > 0.01:
        end_date = max(end_date, analysis_date)
    
    records = []
    current_balance = invoice_total
    current_date = issue_date
    day_counter = 1
    resolved_on = None
    tolerance = 0.01
    
    while current_date <= end_date:
        opening_balance = current_balance
        events_today = events_by_date.get(current_date, [])
        change = sum(event['delta'] for event in events_today)
        current_balance = max(opening_balance + change, 0)
        activity_desc = (
            ', '.join(event['label'] for event in events_today) if events_today else '—'
        )
        
        records.append({
            'invoice_number': doc_number,
            'invoice_id': invoice_id,
            'day': day_counter,
            'date': current_date,
            'opening_balance': opening_balance,
            'activity': activity_desc,
            'change': change,
            'closing_balance': current_balance
        })
        
        if current_balance <= tolerance:
            resolved_on = current_date
            break
        
        current_date += timedelta(days=1)
        day_counter += 1
        
        if day_counter > 2000:  # Safety guard
            break
    
    series_df = pd.DataFrame(records)
    summary = {
        'invoice_id': invoice_id,
        'invoice_number': doc_number,
        'invoice_total': invoice_total,
        'issue_date': issue_date,
        'closing_date': resolved_on,
        'days_recorded': len(records),
        'outstanding_end': current_balance,
        'total_paid': total_paid,
        'total_returned': total_returned,
        'contract_number': invoice_row.get('contract_number', ''),
        'buyer_name': invoice_row.get('buyer_name', '')
    }
    
    return series_df, summary


def build_contract_daily_series(daily_df, invoices_df, as_of_date):
    """Aggregate invoice-level aging into a single contract-level timeline."""
    if daily_df.empty:
        return pd.DataFrame()
    
    df = daily_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    
    contract_dates = pd.to_datetime(
        invoices_df.get('contract_date', pd.Series()),
        errors='coerce'
    ).dropna()
    contract_date = contract_dates.min() if not contract_dates.empty else None
    
    invoice_dates = pd.to_datetime(
        invoices_df.get('document_date', pd.Series()),
        errors='coerce'
    ).dropna()
    
    start_date = df['date'].min()
    if contract_date is not None:
        invoices_after_contract = invoice_dates[invoice_dates >= contract_date]
        if not invoices_after_contract.empty:
            start_date = invoices_after_contract.min()
    
    df = df[df['date'] >= start_date]
    
    agg = (
        df.groupby('date')
        .agg({
            'opening_balance': 'sum',
            'change': 'sum',
            'closing_balance': 'sum'
        })
        .reset_index()
        .sort_values('date')
    )
    
    agg['day'] = range(1, len(agg) + 1)
    agg['date'] = agg['date'].dt.date
    
    return agg[['day', 'date', 'opening_balance', 'change', 'closing_balance']]


def display_risk_card(risk_profile, lang):
    """Display risk assessment card."""
    t = lambda key, **kwargs: tr(key, lang, **kwargs)
    st.markdown(t("risk_card_header"))
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        rating = risk_profile.get('rating', 'N/A')
        rating_color = {
            'A': 'green',
            'B': 'blue',
            'C': 'orange',
            'D': 'red'
        }.get(rating, 'gray')
        st.markdown(t("risk_credit_rating_label"))
        st.markdown(f"<h1 style='color:{rating_color};'>{rating}</h1>", unsafe_allow_html=True)
    
    with col2:
        pd_val = risk_profile.get('pd', 0)
        st.metric(t("risk_pd_metric_label"), f"{pd_val:.2%}")
        st.caption(t("risk_pd_score_caption", score=risk_profile.get('pd_score', 0)))
    
    with col3:
        lgd_val = risk_profile.get('lgd', 0)
        st.metric(t("risk_lgd_metric_label"), f"{lgd_val:.2%}")
    
    with col4:
        ead_val = risk_profile.get('ead_current', 0)
        st.metric(t("risk_ead_metric_label"), f"{ead_val:,.0f}")
    
    st.divider()
    
    col1, col2 = st.columns(2)
    
    with col1:
        el_val = risk_profile.get('expected_loss', 0)
        st.metric(
            t("risk_expected_loss_label"),
            f"{el_val:,.0f}",
            delta=None,
            help=t("risk_expected_loss_help")
        )
    
    with col2:
        limit_val = risk_profile.get('recommended_limit', 0)
        st.metric(
            t("risk_credit_limit_label"),
            f"{limit_val:,.0f}",
            delta=None,
            help=t("risk_credit_limit_help")
        )


def analyze_single_counterparty(user_id, counterparty_inn, invoice_type, as_of_date, months_back, config, lang):
    """Run risk analysis for a single counterparty."""
    t = lambda key, **kwargs: tr(key, lang, **kwargs)
    # Clean the INN - remove .0 suffix if present
    counterparty_inn = str(counterparty_inn).replace('.0', '').strip()

    # Fetch data
    invoices_df, payments_df = get_all_invoices_and_payments(
        user_id, invoice_type, months_back, as_of_date
    )

    if invoices_df.empty:
        return {'error': t("risk_error_no_invoices")}

    # Clean INN in dataframes too
    inn_column = 'buyer_inn' if invoice_type == 'OUT' else 'seller_inn'

    if invoice_type == 'OUT' and 'buyer_inn' in invoices_df.columns:
        invoices_df['buyer_inn'] = invoices_df['buyer_inn'].astype(str).str.replace('.0', '', regex=False).str.strip()
    elif 'seller_inn' in invoices_df.columns:
        invoices_df['seller_inn'] = invoices_df['seller_inn'].astype(str).str.replace('.0', '', regex=False).str.strip()

    # Check for this specific counterparty in the invoices
    if inn_column in invoices_df.columns:
        counterparty_invoices = invoices_df[invoices_df[inn_column] == counterparty_inn]

        if len(counterparty_invoices) == 0:
            # Show what INNs actually exist
            unique_inns = invoices_df[inn_column].dropna().unique()

            # Check if this INN exists with different formatting
            for inn in unique_inns:
                if str(inn).replace('.0', '').strip() == counterparty_inn or counterparty_inn in str(inn):
                    counterparty_invoices = invoices_df[invoices_df[inn_column] == inn]
                    break

    if 'counterparty_inn' in payments_df.columns:
        payments_df['counterparty_inn'] = payments_df['counterparty_inn'].astype(str).str.replace('.0', '', regex=False).str.strip()

    # Initialize risk engine
    engine = RiskEngine(config, user_id)

    # Reconstruct components
    components = engine.reconstruct_invoice_components(
        invoices_df, payments_df, invoice_type, as_of_date
    )

    if not components:
        return {'error': t("risk_error_no_components", inn=counterparty_inn)}
    # Calculate risk
    risk_profile = engine.calculate_counterparty_risk(counterparty_inn, components)

    return risk_profile


def run_portfolio_analysis(user_id, invoice_type, as_of_date, months_back, config):
    """Run risk analysis for entire portfolio."""
    # Get all counterparties
    counterparties = get_all_counterparties(user_id, invoice_type, months_back, as_of_date)
    
    if counterparties.empty:
        return {}
    
    # Get all data at once
    invoices_df, payments_df = get_all_invoices_and_payments(
        user_id, invoice_type, months_back, as_of_date
    )
    
    if invoices_df.empty:
        return {}
    
    # Initialize risk engine
    engine = RiskEngine(config, user_id)
    
    # Reconstruct all components
    components = engine.reconstruct_invoice_components(
        invoices_df, payments_df, invoice_type, as_of_date
    )
    
    # Analyze each counterparty
    results = {}
    
    for _, cp in counterparties.iterrows():
        counterparty_inn = cp['counterparty_inn']
        
        try:
            risk_profile = engine.calculate_counterparty_risk(counterparty_inn, components)
            results[counterparty_inn] = risk_profile
        except Exception as e:
            print(f"Error analyzing {counterparty_inn}: {e}")
            continue
    
    return results


if __name__ == "__main__":
    main()

