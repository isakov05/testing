import streamlit as st
import pandas as pd
from datetime import datetime
from utils.validation import validate_dataframe, validate_financial_data, display_validation_results, check_data_freshness, validate_inn_format
from auth.db_authenticator import protect_page

st.set_page_config(page_title="Settings & System Status", page_icon="⚙️", layout="wide")

protect_page()



def get_system_data():
    """Get all processed data from session state"""
    return {
        'invoices_in': st.session_state.get('invoices_in_processed'),
        'invoices_out': st.session_state.get('invoices_out_processed'),
        'bank_statements': st.session_state.get('bank_statements_processed')
    }


def render_data_quality_dashboard():
    """Render data quality dashboard"""
    st.subheader("📊 Data Quality Dashboard")

    data_sources = get_system_data()

    if not any(df is not None and not df.empty for df in data_sources.values()):
        st.warning("No data available for quality assessment. Please upload and process files first.")
        return

    # Data quality checks for each source
    for source_name, df in data_sources.items():
        if df is not None and not df.empty:
            with st.expander(f"📋 {source_name.replace('_', ' ').title()} Quality Report", expanded=False):

                # Basic validation
                if source_name == 'bank_statements':
                    validation_result = validate_financial_data(df, 'amount', 'date')
                else:
                    # For invoices, try to find amount column
                    amount_cols = ['Стоимость поставки с учётом НДС', 'amount', 'Amount', 'Сумма']
                    amount_col = next((col for col in amount_cols if col in df.columns), None)

                    if amount_col:
                        validation_result = validate_financial_data(df, amount_col, 'date')
                    else:
                        validation_result = validate_dataframe(df)

                display_validation_results(validation_result)

                # Data freshness check
                freshness = check_data_freshness(df)
                if freshness['is_fresh']:
                    st.success(f"✅ {freshness['message']}")
                else:
                    st.warning(f"⚠️ {freshness['message']}")

                # INN validation for relevant datasets
                inn_columns = ['inn', 'Taxpayer ID (INN)', 'Продавец (ИНН или ПИНФЛ)', 'Покупатель (ИНН или ПИНФЛ)']
                for col in inn_columns:
                    if col in df.columns:
                        inn_validation = validate_inn_format(df[col])
                        if inn_validation['total_count'] > 0:
                            valid_pct = (inn_validation['valid_count'] / inn_validation['total_count']) * 100
                            if valid_pct >= 80:
                                st.success(f"✅ INN validation ({col}): {valid_pct:.1f}% valid ({inn_validation['valid_count']}/{inn_validation['total_count']})")
                            elif valid_pct >= 50:
                                st.warning(f"⚠️ INN validation ({col}): {valid_pct:.1f}% valid ({inn_validation['valid_count']}/{inn_validation['total_count']})")
                            else:
                                st.error(f"❌ INN validation ({col}): {valid_pct:.1f}% valid ({inn_validation['valid_count']}/{inn_validation['total_count']})")

                            if inn_validation['invalid_examples']:
                                st.caption(f"Invalid examples: {', '.join(inn_validation['invalid_examples'][:3])}")
                        break


def render_system_status():
    """Render system status and health checks"""
    st.subheader("🔧 System Status")

    # Session state information
    data_sources = get_system_data()

    status_col1, status_col2, status_col3 = st.columns(3)

    with status_col1:
        st.markdown("**📁 Data Sources**")
        for source_name, df in data_sources.items():
            if df is not None and not df.empty:
                st.success(f"✅ {source_name.replace('_', ' ').title()}: {len(df):,} records")
            else:
                st.error(f"❌ {source_name.replace('_', ' ').title()}: No data")

    with status_col2:
        st.markdown("**💾 Session Information**")
        total_records = sum(len(df) for df in data_sources.values() if df is not None and not df.empty)
        st.info(f"📊 Total Records: {total_records:,}")

        # Memory usage estimation
        memory_usage = sum(df.memory_usage(deep=True).sum() for df in data_sources.values() if df is not None and not df.empty)
        memory_mb = memory_usage / (1024 * 1024)
        st.info(f"💾 Memory Usage: ~{memory_mb:.1f} MB")

    with status_col3:
        st.markdown("**🕐 System Time**")
        st.info(f"Current Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Last processing time (if available)
        if any(df is not None and not df.empty for df in data_sources.values()):
            st.success("✅ Data Processed Successfully")
        else:
            st.warning("⚠️ No Data Processed")


def render_data_export_tools():
    """Render data export and backup tools"""
    st.subheader("📤 Data Export & Backup")

    data_sources = get_system_data()

    if not any(df is not None and not df.empty for df in data_sources.values()):
        st.info("No data available for export.")
        return

    # Export options
    export_col1, export_col2 = st.columns(2)

    with export_col1:
        st.markdown("**Individual Exports**")
        for source_name, df in data_sources.items():
            if df is not None and not df.empty:
                # Create CSV export
                csv_data = df.to_csv(index=False)
                st.download_button(
                    label=f"📥 Export {source_name.replace('_', ' ').title()} (CSV)",
                    data=csv_data,
                    file_name=f"{source_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key=f"export_{source_name}"
                )

    with export_col2:
        st.markdown("**Bulk Export**")
        if st.button("📦 Prepare Bulk Export", type="primary"):
            # Combine all data sources into a single export
            combined_data = []

            for source_name, df in data_sources.items():
                if df is not None and not df.empty:
                    df_copy = df.copy()
                    df_copy['data_source'] = source_name
                    df_copy['export_timestamp'] = datetime.now()
                    combined_data.append(df_copy)

            if combined_data:
                bulk_df = pd.concat(combined_data, ignore_index=True, sort=False)
                csv_data = bulk_df.to_csv(index=False)

                st.download_button(
                    label="📥 Download Bulk Export (CSV)",
                    data=csv_data,
                    file_name=f"flott_dashboard_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
                st.success(f"✅ Bulk export ready! {len(bulk_df):,} total records from {len(combined_data)} sources.")


def render_system_settings():
    """Render system settings and preferences"""
    st.subheader("⚙️ System Settings")

    # Language settings (if translations are available)
    current_lang = st.session_state.get('language', 'en')

    settings_col1, settings_col2 = st.columns(2)

    with settings_col1:
        st.markdown("**🌐 Language & Localization**")

        selected_lang = st.selectbox(
            "Interface Language",
            options=['en', 'ru'],
            index=0 if current_lang == 'en' else 1,
            format_func=lambda x: 'English' if x == 'en' else 'Русский'
        )

        if selected_lang != current_lang:
            st.session_state.language = selected_lang
            if st.button("Apply Language Change"):
                st.rerun()

        # Currency settings
        currency = st.selectbox(
            "Default Currency",
            options=['USD', 'UZS', 'EUR'],
            index=0
        )

    with settings_col2:
        st.markdown("**📊 Display Settings**")

        # Chart preferences
        chart_theme = st.selectbox(
            "Chart Theme",
            options=['plotly', 'plotly_white', 'plotly_dark'],
            index=0
        )

        # Date format
        date_format = st.selectbox(
            "Date Format",
            options=['YYYY-MM-DD', 'DD/MM/YYYY', 'MM/DD/YYYY'],
            index=0
        )

    # Apply settings button
    if st.button("💾 Save Settings", type="primary"):
        # Save settings to session state
        st.session_state.update({
            'currency': currency,
            'chart_theme': chart_theme,
            'date_format': date_format
        })
        st.success("✅ Settings saved successfully!")


def render_cache_management():
    """Render cache and session management tools"""
    st.subheader("🗄️ Cache & Session Management")

    cache_col1, cache_col2 = st.columns(2)

    with cache_col1:
        st.markdown("**🧹 Clear Data**")

        if st.button("🗑️ Clear All Processed Data", type="secondary"):
            # Clear all processed data from session state
            keys_to_clear = [
                'invoices_in_processed', 'invoices_out_processed', 'bank_statements_processed',
                'invoices_in_uploaded', 'invoices_out_uploaded', 'bank_statements_uploaded'
            ]

            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]

            st.success("✅ All processed data cleared!")
            st.info("💡 You'll need to re-upload and process your files.")

        if st.button("🔄 Clear Cache", type="secondary"):
            st.cache_data.clear()
            st.success("✅ Application cache cleared!")

    with cache_col2:
        st.markdown("**📋 Session Info**")

        session_keys = [key for key in st.session_state.keys() if not key.startswith('_')]
        st.info(f"Active session variables: {len(session_keys)}")

        with st.expander("🔍 View Session State", expanded=False):
            for key in sorted(session_keys):
                value = st.session_state[key]
                if isinstance(value, pd.DataFrame):
                    st.write(f"**{key}**: DataFrame ({len(value)} rows, {len(value.columns)} cols)")
                else:
                    st.write(f"**{key}**: {type(value).__name__}")


def main() -> None:

    st.title("⚙️ Settings & System Status")
    st.caption("Monitor system health, data quality, and manage platform settings")

    # Create tabs for different settings sections
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Data Quality",
        "🔧 System Status",
        "📤 Export Tools",
        "⚙️ Settings",
        "🗄️ Cache Management"
    ])

    with tab1:
        render_data_quality_dashboard()

    with tab2:
        render_system_status()

    with tab3:
        render_data_export_tools()

    with tab4:
        render_system_settings()

    with tab5:
        render_cache_management()


if __name__ == "__main__":
    main()


