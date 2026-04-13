import streamlit as st
import pandas as pd
from datetime import datetime
from auth.db_authenticator import protect_page, get_current_user
from translations import get_text
from utils.company_operations import (
    load_company_info,
    refresh_company_info,
    # get_business_metrics,
    get_company_inn_for_user,
    get_or_fetch_company_info,
    set_company_inn_for_user,
    get_deals_summary,
    get_court_cases_summary,
    get_connections_summary,
    get_liabilities_summary,
    get_licenses_list,
    get_or_fetch_viewed_company
)
from utils.myorg_api import search_companies
import re

st.set_page_config(
    page_title="Company Overview",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Protect this page - require authentication
protect_page()


def format_currency(amount):
    """Format currency with thousands separator"""
    if amount is None:
        return None
    try:
        return f"{int(amount):,}".replace(",", " ")
    except (ValueError, TypeError):
        return str(amount)


def clean_detail_field(value, lang='ru'):
    """
    Clean detail fields that might be JSON objects.
    Example: {"id": 2, "name": "Малый бизнес", "name_uz": "...", "name_en": "..."}
    Returns just the name in the preferred language.
    """
    if value is None:
        return None

    # If it's already a dict, extract the name
    if isinstance(value, dict):
        # Try language-specific name first
        lang_key = f'name_{lang}'
        if lang_key in value:
            return value[lang_key]

        # Fallback to 'name' field (usually Russian)
        if 'name' in value:
            return value['name']

        # Try other language variants
        for key in ['name_ru', 'name_en', 'name_uz']:
            if key in value:
                return value[key]

        # If no name field, return the whole object as string
        print(f"[DEBUG] clean_detail_field: dict has no name field: {value}")
        return str(value)

    # If it's a string, try to parse as JSON
    if isinstance(value, str):
        # Check if it looks like a JSON object
        value_stripped = value.strip()
        if value_stripped.startswith('{') or value_stripped.startswith('['):
            # Try to parse JSON string
            try:
                import json
                parsed = json.loads(value_stripped)
                print(f"[DEBUG] clean_detail_field: successfully parsed JSON string to: {type(parsed)}")

                if isinstance(parsed, dict):
                    # Recursively call to extract name from parsed dict
                    return clean_detail_field(parsed, lang)
                else:
                    print(f"[DEBUG] clean_detail_field: parsed JSON is not a dict, returning as-is")
                    return value  # Not a dict, return original string
            except (json.JSONDecodeError, ValueError) as e:
                print(f"[DEBUG] clean_detail_field: failed to parse JSON: {e}")
                return value  # Not valid JSON, return as-is
        else:
            # Regular string, not JSON
            return value

    # For any other type, convert to string
    return str(value) if value else None


def format_date_relative(date_str, lang='en'):
    """Format date with relative time (e.g., 'a month ago')"""
    if not date_str:
        return None

    try:
        # Parse date
        if isinstance(date_str, str):
            reg_date = datetime.strptime(date_str.split()[0], "%Y-%m-%d")
        else:
            reg_date = date_str

        # Calculate difference
        diff = datetime.now() - reg_date
        days_diff = diff.days

        # Format date
        formatted_date = reg_date.strftime("%d-%m-%Y")

        # Add relative time
        if days_diff < 30:
            relative = get_text("ago_days", lang).format(days_diff)
        elif days_diff < 365:
            months = days_diff // 30
            if months == 1:
                relative = get_text("ago_month", lang)
            else:
                relative = get_text("ago_months", lang).format(months)
        else:
            years = days_diff // 365
            if years == 1:
                relative = get_text("ago_year", lang)
            else:
                relative = get_text("ago_years", lang).format(years)

        return f"{formatted_date} ({relative})"
    except Exception:
        return date_str


def render_info_display(label, value, lang='en'):
    """Render an info display row with label and value"""
    no_data_text = get_text("no_data", lang)
    display_value = value if value else no_data_text

    st.markdown(f"""
    <div style="display: flex; flex-direction: column; margin-bottom: 1rem;">
        <span style="font-size: 0.875rem; color: #666; margin-bottom: 0.25rem;">{label}</span>
        <span style="font-size: 1rem; color: #000; font-weight: 500;">{display_value}</span>
    </div>
    """, unsafe_allow_html=True)


def render_status_badge(is_active, status_text, lang='en'):
    """Render company status badge"""
    if is_active:
        color = "#10b981"  # green
        status_label = get_text("status_active", lang)
    else:
        color = "#ef4444"  # red
        status_label = get_text("status_inactive", lang)

    st.markdown(f"""
    <div style="padding: 0.5rem 1rem; background-color: {color}20; border-radius: 0.375rem; display: inline-block; margin-right: 0.5rem;">
        <span style="color: {color}; font-weight: 600;">{status_label}</span>
        {f" — {status_text}" if status_text else ""}
    </div>
    """, unsafe_allow_html=True)


def render_indicator_badge(label, is_active, color_active="#3b82f6", color_inactive="#9ca3af"):
    """Render a small indicator badge"""
    color = color_active if is_active else color_inactive
    opacity = "1" if is_active else "0.5"

    st.markdown(f"""
    <div style="display: inline-block; padding: 0.375rem 0.75rem; background-color: {color}20; border-radius: 0.375rem; margin-right: 0.5rem; margin-bottom: 0.5rem; opacity: {opacity};">
        <span style="color: {color}; font-weight: 600; font-size: 0.875rem;">{'✓' if is_active else '○'} {label}</span>
    </div>
    """, unsafe_allow_html=True)


def render_verified_badge():
    """Render verified checkmark badge"""
    return """
    <svg width="20" height="20" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg" style="vertical-align: middle; margin-left: 0.5rem;">
        <path fill-rule="evenodd" clip-rule="evenodd" d="M5.957 1.051a2.51 2.51 0 014.086 0l.16.224a.837.837 0 00.773.345l.71-.08a2.51 2.51 0 012.773 2.774l-.079.71a.837.837 0 00.345.774l.224.159a2.51 2.51 0 010 4.086l-.224.16a.837.837 0 00-.345.773l.08.71a2.51 2.51 0 01-2.774 2.773l-.71-.079a.837.837 0 00-.774.345l-.159.224a2.51 2.51 0 01-4.086 0l-.16-.224a.837.837 0 00-.773-.345l-.71.08a2.51 2.51 0 01-2.773-2.774l.079-.71a.837.837 0 00-.345-.774l-.224-.159a2.51 2.51 0 010-4.086l.224-.16a.837.837 0 00.345-.773l-.08-.71A2.51 2.51 0 014.315 1.54l.71.079a.837.837 0 00.774-.345l.159-.224zm5.145 4.683a.837.837 0 010 1.184l-3.196 3.196c-.41.41-1.076.41-1.486 0L4.898 8.592A.837.837 0 016.08 7.408L7.163 8.49 9.92 5.734a.837.837 0 011.183 0z" fill="#34BB73"></path>
    </svg>
    """


def render_company_details(company_data, lang='en', show_extended_tabs=True):
    """
    Reusable component to render complete company details
    Used for both main company overview and "View Details" feature

    Args:
        company_data: Dictionary with company information
        lang: Language code
        show_extended_tabs: Whether to show extended data tabs (deals, court cases, etc.)
    """
    if not company_data:
        st.warning(get_text("no_company_data", lang))
        return

    # Company header with name and verification badge
    company_name = company_data.get('company_name', '')
    is_verified = company_data.get('is_verified', False)

    st.markdown(f"""
    <div style="margin: 1rem 0;">
        <h2 style="font-size: 1.5rem; font-weight: 700; margin: 0; display: inline;">
            {company_name}
            {render_verified_badge() if is_verified else ''}
    </div>
    """, unsafe_allow_html=True)

    # Status badges and indicators
    status = company_data.get('status', '')
    status_desc = company_data.get('status_description', '')
    activity_state = company_data.get('activity_state', 1)
    is_active = activity_state == 1

    itpark = company_data.get('itpark', False)
    is_large_taxpayer = company_data.get('is_large_taxpayer', False)
    is_bankrupt = company_data.get('is_bankrupt', False)
    is_abuse_vat = company_data.get('is_abuse_vat', False)
    trust = company_data.get('trust')
    score = company_data.get('score')

    left_col, right_col = st.columns([3, 2])
    with left_col:
        render_status_badge(is_active, status_desc, lang)
        render_indicator_badge(get_text("itpark_member", lang), itpark, "#8b5cf6", "#9ca3af")
        render_indicator_badge(get_text("large_taxpayer", lang), is_large_taxpayer, "#3b82f6", "#9ca3af")
        if is_bankrupt:
            render_indicator_badge(get_text("bankrupt_status", lang), True, "#ef4444", "#9ca3af")
        if is_abuse_vat:
            render_indicator_badge(get_text("vat_abuse", lang), True, "#f59e0b", "#9ca3af")

    with right_col:
        if trust is not None or score is not None:
            col_t, col_s = st.columns(2)
            if trust is not None:
                with col_t:
                    st.metric(get_text("trust_score", lang), trust)
            if score is not None:
                with col_s:
                    st.metric(get_text("company_score", lang), score)

    st.markdown("---")

    # Basic Information Section
    st.markdown(f"### {get_text('basic_information', lang)}")

    col1, col2 = st.columns(2)

    with col1:
        render_info_display(get_text("field_inn", lang), company_data.get('inn'), lang)
        render_info_display(
            get_text("field_registration_date", lang),
            format_date_relative(company_data.get('registration_date'), lang),
            lang
        )
        render_info_display(
            get_text("field_statutory_fund", lang),
            f"{format_currency(company_data.get('statutory_fund'))} UZS" if company_data.get('statutory_fund') else None,
            lang
        )
        render_info_display(get_text("field_director", lang), company_data.get('director_name'), lang)

    with col2:
        small_biz = company_data.get('is_small_business', False)
        render_info_display(
            get_text("field_small_business", lang),
            get_text("yes", lang) if small_biz else get_text("no", lang),
            lang
        )
        # Clean detail fields before display
        render_info_display(get_text("field_enterprise_category", lang), clean_detail_field(company_data.get('enterprise_category'), lang), lang)
        render_info_display(get_text("field_taxation_type", lang), clean_detail_field(company_data.get('taxation_type'), lang), lang)
        village_name = company_data.get('village_name')
        if village_name:
            render_info_display(get_text("field_village", lang), village_name, lang)

    st.markdown("---")

    # Founders Section
    st.markdown(f"### {get_text('founders_section', lang)}")

    raw_data = company_data.get('raw_data', {})
    founders = raw_data.get('founders', []) if raw_data else []

    if founders:
        for founder in founders:
            name = founder.get('name', '')
            percentage = founder.get('percentage', 0)
            is_individual = founder.get('is_individual', 1) == 1

            founder_type = get_text("founder_individual", lang) if is_individual else get_text("founder_legal", lang)

            st.markdown(f"""
            <div style="padding: 0.75rem; background-color: #f9fafb; border-radius: 0.375rem; margin-bottom: 0.5rem;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <p style="margin: 0; font-weight: 600; font-size: 0.9rem;">{name}</p>
                        <p style="margin: 0; color: #666; font-size: 0.75rem;">{founder_type}</p>
                    </div>
                    <div style="text-align: right;">
                        <p style="margin: 0; font-weight: 700; font-size: 1.1rem; color: #3b82f6;">{percentage}%</p>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info(get_text("no_founders_data", lang))

    st.markdown("---")

    # Contacts Section
    st.markdown(f"### {get_text('contacts', lang)}")

    col1, col2 = st.columns(2)

    with col1:
        render_info_display(get_text("field_region", lang), company_data.get('region'), lang)
        render_info_display(get_text("field_city", lang), company_data.get('city'), lang)
        render_info_display(get_text("field_address", lang), company_data.get('street_address'), lang)

    with col2:
        render_info_display(get_text("field_email", lang), company_data.get('email'), lang)
        phone = company_data.get('phone')
        if raw_data and raw_data.get('phones'):
            phones_list = raw_data.get('phones', [])
            phone_display = ", ".join(phones_list) if phones_list else phone
        else:
            phone_display = phone
        render_info_display(get_text("field_phone", lang), phone_display, lang)

    # Extended Data Tabs (optional)
    if show_extended_tabs:
        st.markdown("---")
        st.markdown(f"### {get_text('extended_data_title', lang)}")

        # Get company INN for extended data queries
        company_inn = company_data.get('inn')

        # Create tabs for extended data
        tab1, tab2, tab3, tab4 = st.tabs([
            get_text('tab_deals', lang),
            get_text('tab_court_cases', lang),
            get_text('tab_connections', lang),
            get_text('tab_licenses', lang)
        ])

        with tab1:
            # Get deals summary using the proper function with fallback logic
            deals_summary = get_deals_summary(company_inn)

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric(get_text("total_deals", lang), deals_summary.get('total_deals', 0))
            with col2:
                st.metric(get_text("customer_deals", lang), deals_summary.get('customer_deals', 0))
            with col3:
                st.metric(get_text("provider_deals", lang), deals_summary.get('provider_deals', 0))
            with col4:
                avg_size = deals_summary.get('avg_deal_size', 0)
                st.metric(get_text("avg_deal_size", lang), f"{format_currency(avg_size)} UZS" if avg_size > 0 else "0 UZS")

            deals_list = deals_summary.get('deals', [])
            if deals_list:
                st.dataframe(pd.DataFrame(deals_list), use_container_width=True, hide_index=True)
            else:
                st.info(get_text("no_deals_data", lang))

        with tab2:
            # Get court cases summary using the proper function with fallback logic
            court_summary = get_court_cases_summary(company_inn)

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(get_text("total_court_cases", lang), court_summary.get('total_cases', 0))
            with col2:
                st.metric(get_text("pending_court_cases", lang), court_summary.get('pending_cases', 0))
            with col3:
                st.metric(get_text("closed_court_cases", lang), court_summary.get('closed_cases', 0))

            cases_list = court_summary.get('cases', [])
            if cases_list:
                st.dataframe(pd.DataFrame(cases_list), use_container_width=True, hide_index=True)
            else:
                st.info(get_text("no_court_cases_data", lang))

        with tab3:
            # Get connections summary using the proper function with fallback logic
            connections_summary = get_connections_summary(company_inn)

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(get_text("total_connections", lang), connections_summary.get('total_connections', 0))
            with col2:
                st.metric(get_text("connections_as_director", lang), connections_summary.get('as_director', 0))
            with col3:
                st.metric(get_text("connections_as_founder", lang), connections_summary.get('as_founder', 0))

            connections_list = connections_summary.get('connections', [])
            if connections_list:
                for conn in connections_list[:10]:
                    st.markdown(f"""
                    <div style="padding: 0.75rem; background-color: #f9fafb; border-radius: 0.375rem; margin-bottom: 0.5rem;">
                        <p style="margin: 0; font-weight: 600;">{conn.get('name', '')}</p>
                        <p style="margin: 0; color: #666; font-size: 0.875rem;">{get_text("connection_inn", lang)}: {conn.get('inn', '')}</p>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info(get_text("no_connections_data", lang))

        with tab4:
            # Get licenses list using the proper function with fallback logic
            licenses_list = get_licenses_list(company_inn)

            if licenses_list:
                st.metric(get_text("total_active_licenses", lang), len(licenses_list))

                for lic in licenses_list[:6]:  # Show first 6
                    category = lic.get('category', lic.get('category_en', ''))
                    number = lic.get('number', '')
                    validity = lic.get('validity', '')

                    st.markdown(f"""
                    <div style="padding: 0.75rem; background-color: #f9fafb; border-radius: 0.375rem; margin-bottom: 0.5rem; border-left: 4px solid #10b981;">
                        <p style="margin: 0; font-weight: 700;">{category}</p>
                        <p style="margin: 0.25rem 0; color: #666; font-size: 0.875rem;">{get_text("license_number", lang)}: {number}</p>
                        <p style="margin: 0; color: #666; font-size: 0.875rem;">{get_text("license_validity", lang)}: {validity}</p>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info(get_text("no_licenses_data", lang))


def highlight_text(text, query):
    """Highlight matching text in search results"""
    if not text or not query:
        return text
    
    # Escape special regex characters in query
    escaped_query = re.escape(query)
    
    # Case-insensitive replacement with green highlight
    pattern = re.compile(f'({escaped_query})', re.IGNORECASE)
    highlighted = pattern.sub(r'<span style="background-color: #86efac; padding: 0.125rem 0.25rem; border-radius: 0.25rem; font-weight: 600;">\1</span>', str(text))
    
    return highlighted


def render_search_result_card(result, query, lang='en'):
    """Render a search result card for a company"""
    company_name = result.get('name', '')
    inn = result.get('inn', '')
    director = result.get('director', '')
    address = result.get('address', '')
    is_active = result.get('activity_state', 1) == 1

    
    # Highlight matching text
    highlighted_name = highlight_text(company_name, query)
    highlighted_director = highlight_text(director, query)
    highlighted_inn = highlight_text(inn, query)
    
    # Status color
    status_color = "#10b981" if is_active else "#ef4444"
    status_label = get_text("status_active", lang) if is_active else get_text("status_inactive", lang)
    
    # Create card HTML (removed HTML comments to avoid rendering issues)
    director_html = f'<div style="margin-bottom: 0.5rem;"><span style="color: #666; font-size: 0.875rem; font-weight: 500;">{get_text("field_director", lang)}:</span><span style="color: #111827; font-size: 0.875rem; margin-left: 0.5rem;">{highlighted_director}</span></div>' if director else ''
    
    address_html = f'<div style="margin-bottom: 0.5rem;"><span style="color: #666; font-size: 0.875rem; font-weight: 500;">{get_text("field_address", lang)}:</span><span style="color: #111827; font-size: 0.875rem; margin-left: 0.5rem;">{address}</span></div>' if address else ''
    
    oked_html = f'<div style="margin-top: 0.5rem; padding-top: 0.5rem; border-top: 1px solid #e5e7eb;"><span style="color: #666; font-size: 0.75rem;">{oked_name}</span></div>' if oked_name else ''
    
    card_html = f'''<div style="padding: 1.25rem; background-color: #ffffff; border: 1px solid #e5e7eb; border-radius: 0.5rem; margin-bottom: 1rem; cursor: pointer; transition: all 0.2s; box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);" onmouseover="this.style.boxShadow='0 4px 6px rgba(0, 0, 0, 0.1)'; this.style.borderColor='#3b82f6';" onmouseout="this.style.boxShadow='0 1px 3px rgba(0, 0, 0, 0.1)'; this.style.borderColor='#e5e7eb';"><h3 style="margin: 0 0 0.75rem 0; font-size: 1.125rem; font-weight: 600; color: #111827;">{highlighted_name}</h3><div style="margin-bottom: 0.75rem;"><span style="display: inline-block; padding: 0.25rem 0.75rem; background-color: {status_color}20; color: {status_color}; border-radius: 0.375rem; font-size: 0.875rem; font-weight: 600;">{status_label}</span></div>{director_html}<div style="margin-bottom: 0.5rem;"><span style="color: #666; font-size: 0.875rem; font-weight: 500;">{get_text("field_inn", lang)}:</span><span style="color: #111827; font-size: 0.875rem; margin-left: 0.5rem; font-family: monospace;">{highlighted_inn}</span></div>{address_html}{oked_html}</div>'''
    
    return card_html


def main():
    lang = st.session_state.get('language', 'en')

    # Page title
    st.title(get_text("company_overview_title", lang))

    # Get current user
    user_info = get_current_user()
    user_id = user_info.get('id')

    # Create tabs for Company Overview, My Counterparties, and Counterparty Search
    tab1, tab2, tab3 = st.tabs([
        get_text("tab_company_overview", lang),
        "My Counterparties" if lang == 'en' else "Мои контрагенты",
        get_text("tab_counterparty_search", lang)
    ])

    # ========== TAB 1: COMPANY OVERVIEW ==========
    with tab1:
        render_company_overview_tab(user_id, lang)

    # ========== TAB 2: MY COUNTERPARTIES ==========
    with tab2:
        render_my_counterparties_tab(user_id, lang)

    # ========== TAB 3: COUNTERPARTY SEARCH ==========
    with tab3:
        render_counterparty_search_tab(user_id, lang)


def render_my_counterparties_tab(user_id, lang):
    """Render My Counterparties tab - shows all INNs from invoices + bank txns"""
    from utils.company_operations import get_my_counterparties

    st.subheader("My Counterparties" if lang == 'en' else "Мои контрагенты")
    st.caption("All companies you have transacted with (from invoices and bank statements)")

    # Load counterparties
    with st.spinner("Loading counterparties..." if lang == 'en' else "Загрузка контрагентов..."):
        counterparties = get_my_counterparties(user_id)

    if counterparties.empty:
        st.info("No counterparties found. Please upload invoices or bank statements." if lang == 'en' else "Контрагенты не найдены. Пожалуйста, загрузите счета-фактуры или банковские выписки.")
        return

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Counterparties" if lang == 'en' else "Всего контрагентов", len(counterparties))

    with col2:
        if 'source' in counterparties.columns:
            customers = counterparties[counterparties['source'].str.contains('invoice_out', na=False)]
            st.metric("Customers" if lang == 'en' else "Клиенты", len(customers))
        else:
            st.metric("Customers" if lang == 'en' else "Клиенты", 0)

    with col3:
        if 'source' in counterparties.columns:
            suppliers = counterparties[counterparties['source'].str.contains('invoice_in', na=False)]
            st.metric("Suppliers" if lang == 'en' else "Поставщики", len(suppliers))
        else:
            st.metric("Suppliers" if lang == 'en' else "Поставщики", 0)

    with col4:
        if 'total_amount' in counterparties.columns:
            total_volume = counterparties['total_amount'].sum()
            st.metric("Total Volume" if lang == 'en' else "Общий объем", f"{total_volume:,.0f}")
        else:
            st.metric("Total Volume" if lang == 'en' else "Общий объем", "0")

    # Check if a counterparty is selected for detail view
    if 'selected_counterparty_inn' in st.session_state and st.session_state.selected_counterparty_inn:
        selected_inn = st.session_state.selected_counterparty_inn

        # Header with back button
        col1, col2 = st.columns([4, 1])
        with col1:
            st.write(f"### {('Counterparty Details' if lang == 'en' else 'Детали контрагента')}")
        with col2:
            if st.button("← " + ("Back to List" if lang == 'en' else "Назад к списку"), type="secondary"):
                del st.session_state.selected_counterparty_inn
                st.rerun()

        st.divider()

        # Load company details from database
        with st.spinner(("Loading company data..." if lang == 'en' else "Загрузка данных компании...")):
            company_data = load_company_info(selected_inn)

        if company_data:
            render_company_details(company_data, lang, show_extended_tabs=True)
        else:
            st.warning(("No data found for this company in database." if lang == 'en' else "Данные компании не найдены в базе данных."))
    else:
        # Search box
        search_term = st.text_input(
            "Search by INN or Name" if lang == 'en' else "Поиск по ИНН или названию",
            placeholder="Enter INN or company name..." if lang == 'en' else "Введите ИНН или название компании..."
        )

        if search_term:
            search_lower = search_term.lower()
            # Fix search mask - search in both INN and company_name columns
            mask = (
                counterparties['inn'].astype(str).str.lower().str.contains(search_lower, na=False) |
                counterparties['name'].astype(str).str.lower().str.contains(search_lower, na=False)
            )
            counterparties = counterparties[mask]

        # Display table
        st.write(f"### {('Counterparties' if lang == 'en' else 'Контрагенты')} ({len(counterparties)})")

        if not counterparties.empty:
            # Format display columns
            display_cols = ['inn', 'name', 'source', 'transaction_count', 'total_amount', 'last_transaction_date']

            # Add optional columns if they exist
            for col in ['city', 'status', 'trust', 'score']:
                if col in counterparties.columns:
                    display_cols.append(col)

            # Only show columns that exist
            display_cols = [col for col in display_cols if col in counterparties.columns]

            # Format the dataframe for display
            display_df = counterparties[display_cols].copy()

            # Format dates
            if 'last_transaction_date' in display_df.columns:
                display_df['last_transaction_date'] = pd.to_datetime(display_df['last_transaction_date'], errors='coerce').dt.strftime('%Y-%m-%d')

            st.dataframe(
                display_df,
                hide_index=True,
                use_container_width=True,
                column_config={
                    'inn': st.column_config.TextColumn('INN' if lang == 'en' else 'ИНН', width='small'),
                    'name': st.column_config.TextColumn('Company Name' if lang == 'en' else 'Название компании', width='large'),
                    'source': st.column_config.TextColumn('Source' if lang == 'en' else 'Источник', width='medium'),
                    'transaction_count': st.column_config.NumberColumn('Transactions' if lang == 'en' else 'Операции', format='%d'),
                    'total_amount': st.column_config.NumberColumn('Total Amount' if lang == 'en' else 'Общая сумма', format='%.2f'),
                    'last_transaction_date': st.column_config.TextColumn('Last Transaction' if lang == 'en' else 'Последняя операция'),
                    'city': st.column_config.TextColumn('City' if lang == 'en' else 'Город', width='small'),
                    'status': st.column_config.TextColumn('Status' if lang == 'en' else 'Статус', width='small'),
                    'trust': st.column_config.NumberColumn('Trust Score' if lang == 'en' else 'Доверие', format='%.1f'),
                    'score': st.column_config.NumberColumn('Score' if lang == 'en' else 'Оценка', format='%.1f')
                }
            )

            # Select counterparty to view details
            st.divider()
            selected_inn = st.selectbox(
                ("Select a counterparty to view details:" if lang == 'en' else "Выберите контрагента для просмотра деталей:"),
                options=counterparties['inn'].tolist(),
                format_func=lambda x: f"{x} - {counterparties[counterparties['inn']==x]['name'].iloc[0]}",
                key="counterparty_selector"
            )

            if st.button(("View Details" if lang == 'en' else "Посмотреть детали"), type="primary"):
                st.session_state.selected_counterparty_inn = str(selected_inn).replace('.0', '').strip()
                st.rerun()

            # Download button
            csv = counterparties.to_csv(index=False).encode('utf-8')
            from datetime import datetime
            filename = f"counterparties_{datetime.now().strftime('%Y%m%d')}.csv"

            st.download_button(
                label="Download as CSV" if lang == 'en' else "Скачать CSV",
                data=csv,
                file_name=filename,
                mime="text/csv"
            )
        else:
            st.info("No counterparties match the selected filters." if lang == 'en' else "Нет контрагентов, соответствующих выбранным фильтрам.")


def render_counterparty_search_tab(user_id, lang):
    """Render the counterparty search tab"""

    # Search header with styled title
    st.markdown(f"""
    <div style="margin: 1.5rem 0 1rem 0;">
        <h3 style="font-size: 1.5rem; font-weight: 600; color: #111827; margin: 0;">
            {get_text("search_companies", lang)}
        </h3>
    </div>
    """, unsafe_allow_html=True)
    # Search input with Find button
    search_col1, search_col2 = st.columns([5, 1])
    
    with search_col1:
        search_query = st.text_input(
            "Search",
            placeholder="Enter company name, INN, or director name...",
            label_visibility="collapsed",
            key="company_search_input"
        )
    
    with search_col2:
        search_button = st.button(
            "Find" if lang == 'en' else "Найти",
            type="primary",
            use_container_width=True,
            key="search_button"
        )
    
    # Initialize session state for search results
    if 'search_results' not in st.session_state:
        st.session_state.search_results = []
    if 'search_query_executed' not in st.session_state:
        st.session_state.search_query_executed = ''
    if 'search_page' not in st.session_state:
        st.session_state.search_page = 0
    if 'total_search_results' not in st.session_state:
        st.session_state.total_search_results = 0
    if 'expanded_company_inn' not in st.session_state:
        st.session_state.expanded_company_inn = None
    
    # Execute search when button is clicked
    if search_button and search_query:
        with st.spinner(get_text("searching", lang)):
            success, results, error, total = search_companies(search_query, tab=1)
            
            if success:
                st.session_state.search_results = results or []
                st.session_state.search_query_executed = search_query
                st.session_state.search_page = 0  # Reset to first page on new search
                st.session_state.total_search_results = total or len(results or [])
                
                if results:
                    st.success(f"Found {total or len(results)} {'companies' if lang == 'en' else 'компаний'}")
                else:
                    st.info(get_text("no_search_results", lang))
            else:
                st.error(f"Search error: {error}")
                st.session_state.search_results = []
                st.session_state.total_search_results = 0
    
    # Display search results with pagination
    if st.session_state.search_results:
        # Pagination settings
        results_per_page = 10
        total_results = st.session_state.total_search_results
        total_pages = (total_results + results_per_page - 1) // results_per_page  # Ceiling division
        current_page = st.session_state.search_page
        
        # Header with total count
        st.markdown(f"""
        <div style="margin: 1.5rem 0 1rem 0;">
            <h4 style="font-size: 1.125rem; font-weight: 600; color: #111827; margin: 0;">
                Companies <span style="color: #6b7280; font-weight: 400; font-size: 1rem;">({total_results})</span>
            </h4>
        </div>
        """, unsafe_allow_html=True)
        
        # Pagination controls at the top
        if total_pages > 1:
            col1, col2, col3 = st.columns([1, 2, 1])
            
            with col1:
                if current_page > 0:
                    if st.button("← Previous", key="prev_top"):
                        st.session_state.search_page -= 1
                        st.rerun()
            
            with col2:
                st.markdown(f"""
                <div style="text-align: center; padding: 0.5rem;">
                    <span style="color: #666; font-size: 0.875rem;">
                        Page {current_page + 1} of {total_pages}
                    </span>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                if current_page < total_pages - 1:
                    if st.button("Next →", key="next_top"):
                        st.session_state.search_page += 1
                        st.rerun()
        
        # Calculate slice for current page
        start_idx = current_page * results_per_page
        end_idx = min(start_idx + results_per_page, len(st.session_state.search_results))
        page_results = st.session_state.search_results[start_idx:end_idx]
        
        # Display each search result as a clickable card
        for idx, result in enumerate(page_results):
            actual_idx = start_idx + idx  # Global index for unique keys
            result_inn = result.get('inn', '')
            
            # Render the card
            st.markdown(
                render_search_result_card(result, st.session_state.search_query_executed, lang),
                unsafe_allow_html=True
            )
            
            # Add expand/collapse button below each card
            is_expanded = st.session_state.expanded_company_inn == result_inn
            button_label = get_text("collapse_details", lang) if is_expanded else get_text("view_details", lang)

            if st.button(
                button_label,
                key=f"view_company_{actual_idx}_{result_inn}",
                type="primary" if is_expanded else "secondary"
            ):
                if result_inn:
                    # Toggle expansion
                    if st.session_state.expanded_company_inn == result_inn:
                        st.session_state.expanded_company_inn = None
                    else:
                        st.session_state.expanded_company_inn = result_inn
                    st.rerun()
                else:
                    st.error(get_text("no_inn_available", lang))

            # Show expanded details below the button
            if is_expanded and result_inn:
                with st.container(border=True):
                    with st.spinner(get_text("fetching_company_data", lang)):
                        # Load company data from database (no API fetch)
                        company_data = load_company_info(result_inn)

                        if company_data:
                            # Show last updated info if available
                            last_updated = company_data.get('last_updated')
                            if last_updated:
                                st.caption(f"Last updated: {last_updated}")

                            # Render full company details
                            render_company_details(company_data, lang, show_extended_tabs=True)
                        else:
                            st.error(get_text("view_company_error", lang).format("Company not found in database"))
        
        # Pagination controls at the bottom
        if total_pages > 1:
            st.markdown("<div style='margin-top: 2rem;'></div>", unsafe_allow_html=True)
            col1, col2, col3 = st.columns([1, 2, 1])
            
            with col1:
                if current_page > 0:
                    if st.button("← Previous", key="prev_bottom"):
                        st.session_state.search_page -= 1
                        st.rerun()
            
            with col2:
                st.markdown(f"""
                <div style="text-align: center; padding: 0.5rem;">
                    <span style="color: #666; font-size: 0.875rem;">
                        Page {current_page + 1} of {total_pages} • Showing {start_idx + 1}-{end_idx} of {total_results}
                    </span>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                if current_page < total_pages - 1:
                    if st.button("Next →", key="next_bottom"):
                        st.session_state.search_page += 1
                        st.rerun()


def render_company_overview_tab(user_id, lang):
    """Render the company overview tab"""

    # Get user's INN from database
    user_inn = get_company_inn_for_user(user_id)

    # First, try to load company info from database using INN
    company_data = None
    if user_inn:
        company_data = load_company_info(user_inn)

    # If no company data in database, try to fetch from MyOrg API
    if not company_data:

        if user_inn:
            # User has INN configured, fetch from MyOrg API
            with st.spinner(get_text("loading_company_data", lang)):
                success, company_data, error = get_or_fetch_company_info(user_inn)
                
                if not success:
                    st.error(get_text("api_fetch_error", lang).format(error=error))
                    st.info(get_text("configure_inn_message", lang))
                    return
        else:
            # User doesn't have INN configured - prompt to enter and fetch
            st.warning(get_text("no_company_data", lang))
            st.info(get_text("configure_inn_message", lang))

            with st.form("enter_inn_form"):
                entered_inn = st.text_input(get_text("enter_company_inn", lang), value="", max_chars=9)
                submit_inn = st.form_submit_button(get_text("save_and_fetch", lang))

                if submit_inn:
                    clean_inn = str(entered_inn).replace(' ', '').strip()
                    if not clean_inn.isdigit() or len(clean_inn) != 9:
                        st.error(get_text("invalid_inn", lang))
                        st.stop()

                    ok, err = set_company_inn_for_user(user_id, clean_inn)
                    if not ok:
                        st.error(err or get_text("invalid_inn", lang))
                        st.stop()

                    with st.spinner(get_text("loading_company_data", lang)):
                        success, company_data, error = get_or_fetch_company_info(clean_inn)
                        if success and company_data:
                            st.success(get_text("inn_saved", lang))
                            st.rerun()
                        else:
                            st.error(get_text("api_fetch_error", lang).format(error=error))
                            st.stop()

            return

    # Company header with name and verification badge
    company_name = company_data.get('company_name', '')
    is_verified = company_data.get('is_verified', False)

    st.markdown(f"""
    <div style="margin: 2rem 0;">
        <h1 style="font-size: 2rem; font-weight: 700; margin: 0; display: inline;">
            {company_name}
            {render_verified_badge() if is_verified else ''}
    </div>
    """, unsafe_allow_html=True)

    # Consolidated header row: status + indicators (left) and metrics (right)
    status = company_data.get('status', '')
    status_desc = company_data.get('status_description', '')
    activity_state = company_data.get('activity_state', 1)
    is_active = activity_state == 1

    itpark = company_data.get('itpark', False)
    is_large_taxpayer = company_data.get('is_large_taxpayer', False)
    is_bankrupt = company_data.get('is_bankrupt', False)
    is_abuse_vat = company_data.get('is_abuse_vat', False)
    trust = company_data.get('trust')
    score = company_data.get('score')

    left_col, right_col = st.columns([3, 2])
    with left_col:
        render_status_badge(is_active, status_desc, lang)
        render_indicator_badge(get_text("itpark_member", lang), itpark, "#8b5cf6", "#9ca3af")
        render_indicator_badge(get_text("large_taxpayer", lang), is_large_taxpayer, "#3b82f6", "#9ca3af")
        if is_bankrupt:
            render_indicator_badge(get_text("bankrupt_status", lang), True, "#ef4444", "#9ca3af")
        if is_abuse_vat:
            render_indicator_badge(get_text("vat_abuse", lang), True, "#f59e0b", "#9ca3af")

    with right_col:
        if trust is not None or score is not None:
            col_t, col_s = st.columns(2)
            if trust is not None:
                with col_t:
                    st.metric(get_text("trust_score", lang), trust)
            if score is not None:
                with col_s:
                    st.metric(get_text("company_score", lang), score)

    # Last updated info
    last_updated = company_data.get('last_updated', '')
    if last_updated:
        try:
            if isinstance(last_updated, str):
                update_date = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S")
            else:
                update_date = last_updated
            formatted_update = update_date.strftime("%d.%m.%Y")
            st.markdown(f"<span style='color: #666; font-size: 0.875rem;'>{get_text('updated', lang)}: {formatted_update}</span>", unsafe_allow_html=True)
        except Exception:
            pass

    # Refresh button
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button(get_text("refresh_data", lang), use_container_width=True):
            with st.spinner(get_text("refreshing_data", lang)):
                inn = company_data.get('inn')
                success, new_data, error = refresh_company_info(inn)
                if success:
                    st.success(get_text("refresh_success", lang))
                    st.rerun()
                else:
                    st.error(get_text("refresh_error", lang).format(error))

    st.markdown("---")

    # Basic Information Section
    st.markdown(f"### {get_text('basic_information', lang)}")

    col1, col2 = st.columns(2)

    with col1:
        render_info_display(
            get_text("field_inn", lang),
            company_data.get('inn'),
            lang
        )
        render_info_display(
            get_text("field_registration_date", lang),
            format_date_relative(company_data.get('registration_date'), lang),
            lang
        )
        render_info_display(
            get_text("field_statutory_fund", lang),
            f"{format_currency(company_data.get('statutory_fund'))} UZS" if company_data.get('statutory_fund') else None,
            lang
        )
        render_info_display(
            get_text("field_director", lang),
            company_data.get('director_name'),
            lang
        )

    with col2:
        small_biz = company_data.get('is_small_business', False)
        render_info_display(
            get_text("field_small_business", lang),
            get_text("yes", lang) if small_biz else get_text("no", lang),
            lang
        )
        # Clean detail fields before display
        render_info_display(
            get_text("field_enterprise_category", lang),
            clean_detail_field(company_data.get('enterprise_category'), lang),
            lang
        )
        render_info_display(
            get_text("field_taxation_type", lang),
            clean_detail_field(company_data.get('taxation_type'), lang),
            lang
        )
        # Village/MFY
        village_name = company_data.get('village_name')
        if village_name:
            render_info_display(
                get_text("field_village", lang),
                village_name,
                lang
            )

    st.markdown("---")

    # Registration Details Section
    st.markdown(f"### {get_text('registration_details', lang)}")

    col1, col2 = st.columns(2)

    with col1:
        render_info_display(
            get_text("field_registration_number", lang),
            company_data.get('registration_number'),
            lang
        )
        render_info_display(
            get_text("field_registration_date", lang),
            format_date_relative(company_data.get('registration_date'), lang),
            lang
        )

    with col2:
        render_info_display(
            get_text("field_registration_center", lang),
            company_data.get('registration_center'),
            lang
        )
        relevance_date = company_data.get('relevance_date')
        if relevance_date:
            render_info_display(
                get_text("field_relevance_date", lang),
                format_date_relative(relevance_date, lang),
                lang
            )

    st.markdown("---")

    # Statistical Codes Section
    st.markdown(f"### {get_text('statistical_codes', lang)}")

    # OKED (Activity Type)
    oked_code = company_data.get('oked_code')
    oked_desc = company_data.get('oked_description')
    if oked_code or oked_desc:
        render_info_display(
            get_text("field_activity_type", lang),
            f"<div><p style='margin: 0; font-weight: 600;'>{oked_code}</p><p style='margin: 0;'>{oked_desc}</p></div>" if oked_code and oked_desc else (oked_code or oked_desc),
            lang
        )

    # OPF (Organizational Form)
    opf_code = company_data.get('opf_code')
    opf_desc = company_data.get('opf_description')
    if opf_code or opf_desc:
        render_info_display(
            get_text("field_opf", lang),
            f"<div><p style='margin: 0; font-weight: 600;'>{opf_code}</p><p style='margin: 0;'>{opf_desc}</p></div>" if opf_code and opf_desc else (opf_code or opf_desc),
            lang
        )

    # SOOGU
    soogu_code = company_data.get('soogu_code')
    soogu_desc = company_data.get('soogu_description')
    if soogu_code or soogu_desc:
        render_info_display(
            get_text("field_soogu", lang),
            f"<div><p style='margin: 0; font-weight: 600;'>{soogu_code}</p><p style='margin: 0;'>{soogu_desc}</p></div>" if soogu_code and soogu_desc else (soogu_code or soogu_desc),
            lang
        )

    # SOATO (Territory)
    soato_code = company_data.get('soato_code')
    soato_desc = company_data.get('soato_description')
    if soato_code or soato_desc:
        render_info_display(
            get_text("field_soato", lang),
            f"<div><p style='margin: 0; font-weight: 600;'>{soato_code}</p><p style='margin: 0;'>{soato_desc}</p></div>" if soato_code and soato_desc else (soato_code or soato_desc),
            lang
        )

    st.markdown("---")

    # Founders Section
    st.markdown(f"### {get_text('founders_section', lang)}")

    raw_data = company_data.get('raw_data', {})
    founders = raw_data.get('founders', []) if raw_data else []

    if founders:
        for founder in founders:
            name = founder.get('name', '')
            percentage = founder.get('percentage', 0)
            is_individual = founder.get('is_individual', 1) == 1

            founder_type = get_text("founder_individual", lang) if is_individual else get_text("founder_legal", lang)

            st.markdown(f"""
            <div style="padding: 1rem; background-color: #f9fafb; border-radius: 0.5rem; margin-bottom: 0.75rem;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <p style="margin: 0; font-weight: 600; font-size: 1rem;">{name}</p>
                        <p style="margin: 0; color: #666; font-size: 0.875rem;">{founder_type}</p>
                    </div>
                    <div style="text-align: right;">
                        <p style="margin: 0; font-weight: 700; font-size: 1.25rem; color: #3b82f6;">{percentage}%</p>
                        <p style="margin: 0; color: #666; font-size: 0.75rem;">{get_text("field_ownership_percentage", lang)}</p>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info(get_text("no_founders_data", lang))

    st.markdown("---")

    # Contacts Section
    st.markdown(f"### {get_text('contacts', lang)}")

    col1, col2 = st.columns(2)

    with col1:
        render_info_display(
            get_text("field_region", lang),
            company_data.get('region'),
            lang
        )
        render_info_display(
            get_text("field_city", lang),
            company_data.get('city'),
            lang
        )
        render_info_display(
            get_text("field_address", lang),
            company_data.get('street_address'),
            lang
        )

    with col2:
        render_info_display(
            get_text("field_email", lang),
            company_data.get('email'),
            lang
        )
        # Handle phones array
        phone = company_data.get('phone')
        if raw_data and raw_data.get('phones'):
            phones_list = raw_data.get('phones', [])
            phone_display = ", ".join(phones_list) if phones_list else phone
        else:
            phone_display = phone

        render_info_display(
            get_text("field_phone", lang),
            phone_display,
            lang
        )

    # st.markdown("---")

    # # Business Metrics Section
    # st.markdown(f"### {get_text('business_metrics_section', lang)}")

    # metrics = get_business_metrics(user_id)

    # col1, col2, col3 = st.columns(3)

    # with col1:
    #     st.metric(
    #         get_text("total_deals", lang),
    #         metrics.get('total_deals', 0),
    #         delta=f"{get_text('customer_deals', lang)}: {metrics.get('customer_deals', 0)} | {get_text('provider_deals', lang)}: {metrics.get('provider_deals', 0)}"
    #     )

    # with col2:
    #     st.metric(
    #         get_text("active_licenses", lang),
    #         metrics.get('total_licenses', 0)
    #     )

    # with col3:
    #     st.metric(
    #         get_text("total_buildings", lang),
    #         metrics.get('total_buildings', 0)
    #     )

    # # Cadastres
    # cadastres = metrics.get('total_cadastres', 0)
    # if cadastres > 0:
    #     st.metric(
    #         get_text("total_cadastres", lang),
    #         cadastres
    #     )

    st.markdown("---")

    # ========== EXTENDED COMPANY INFORMATION (TABS) ==========
    st.markdown(f"### {get_text('extended_data_title', lang)}")

    # Get company INN for extended data queries
    company_inn = company_data.get('inn')

    # Show liabilities warning if exists (before tabs)
    liabilities_summary = get_liabilities_summary(company_inn)
    if liabilities_summary.get('has_liabilities', False):
        st.warning(get_text("liabilities_warning", lang))

    # Create tabs for extended data
    tab1, tab2, tab3, tab4 = st.tabs([
        get_text('tab_deals', lang),
        get_text('tab_court_cases', lang),
        get_text('tab_connections', lang),
        get_text('tab_licenses', lang)
    ])

    # ========== TAB 1: DEALS & PROCUREMENT ==========
    with tab1:
        deals_summary = get_deals_summary(company_inn)

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                get_text("total_deals", lang),
                deals_summary.get('total_deals', 0)
            )

        with col2:
            st.metric(
                get_text("customer_deals", lang),
                deals_summary.get('customer_deals', 0)
            )

        with col3:
            st.metric(
                get_text("provider_deals", lang),
                deals_summary.get('provider_deals', 0)
            )

        with col4:
            avg_size = deals_summary.get('avg_deal_size', 0)
            st.metric(
                get_text("avg_deal_size", lang),
                f"{format_currency(avg_size)} UZS" if avg_size > 0 else "0 UZS"
            )

        # Show deals table if there are deals
        deals_list = deals_summary.get('deals', [])
        if deals_list:
            with st.expander(get_text("show_deals_table", lang)):
                # Create DataFrame from deals
                deals_df = pd.DataFrame(deals_list)

                # Select and rename columns for display
                display_cols = []
                col_mapping = {}

                if 'deal_date' in deals_df.columns:
                    display_cols.append('deal_date')
                    col_mapping['deal_date'] = get_text("deal_date", lang)

                if 'customer_name' in deals_df.columns:
                    display_cols.append('customer_name')
                    col_mapping['customer_name'] = get_text("deal_customer", lang)

                if 'provider_name' in deals_df.columns:
                    display_cols.append('provider_name')
                    col_mapping['provider_name'] = get_text("deal_provider", lang)

                if 'deal_cost' in deals_df.columns:
                    display_cols.append('deal_cost')
                    col_mapping['deal_cost'] = get_text("deal_amount", lang)

                if 'category_name' in deals_df.columns:
                    display_cols.append('category_name')
                    col_mapping['category_name'] = get_text("deal_category", lang)

                if 'is_completed' in deals_df.columns:
                    display_cols.append('is_completed')
                    col_mapping['is_completed'] = get_text("deal_status", lang)

                # Filter and rename
                if display_cols:
                    display_df = deals_df[display_cols].copy()
                    display_df = display_df.rename(columns=col_mapping)

                    # Format status column
                    if get_text("deal_status", lang) in display_df.columns:
                        display_df[get_text("deal_status", lang)] = display_df[get_text("deal_status", lang)].apply(
                            lambda x: get_text("deal_completed", lang) if x == 1 else get_text("deal_pending", lang)
                        )

                    st.dataframe(display_df, use_container_width=True, hide_index=True)
                else:
                    st.dataframe(deals_df, use_container_width=True, hide_index=True)
        else:
            st.info(get_text("no_deals_data", lang))

    # ========== TAB 2: COURT CASES ==========
    with tab2:
        court_summary = get_court_cases_summary(company_inn)

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                get_text("total_court_cases", lang),
                court_summary.get('total_cases', 0)
            )

        with col2:
            st.metric(
                get_text("pending_court_cases", lang),
                court_summary.get('pending_cases', 0)
            )

        with col3:
            st.metric(
                get_text("closed_court_cases", lang),
                court_summary.get('closed_cases', 0)
            )

        # Show court cases table if there are cases
        cases_list = court_summary.get('cases', [])
        if cases_list:
            with st.expander(get_text("show_cases_table", lang)):
                cases_df = pd.DataFrame(cases_list)

                display_cols = []
                col_mapping = {}

                if 'casenumber' in cases_df.columns:
                    display_cols.append('casenumber')
                    col_mapping['casenumber'] = get_text("case_number", lang)

                if 'type' in cases_df.columns:
                    display_cols.append('type')
                    col_mapping['type'] = get_text("case_type", lang)

                if 'category' in cases_df.columns:
                    display_cols.append('category')
                    col_mapping['category'] = get_text("case_category", lang)

                if 'court' in cases_df.columns:
                    display_cols.append('court')
                    col_mapping['court'] = get_text("case_court", lang)

                if 'hearing_date' in cases_df.columns:
                    display_cols.append('hearing_date')
                    col_mapping['hearing_date'] = get_text("case_hearing_date", lang)

                if 'result' in cases_df.columns:
                    display_cols.append('result')
                    col_mapping['result'] = get_text("case_result", lang)

                if display_cols:
                    display_df = cases_df[display_cols].copy()
                    display_df = display_df.rename(columns=col_mapping)
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
                else:
                    st.dataframe(cases_df, use_container_width=True, hide_index=True)
        else:
            st.info(get_text("no_court_cases_data", lang))

    # ========== TAB 3: BUSINESS CONNECTIONS ==========
    with tab3:
        connections_summary = get_connections_summary(company_inn)

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                get_text("total_connections", lang),
                connections_summary.get('total_connections', 0)
            )

        with col2:
            st.metric(
                get_text("connections_as_director", lang),
                connections_summary.get('as_director', 0)
            )

        with col3:
            st.metric(
                get_text("connections_as_founder", lang),
                connections_summary.get('as_founder', 0)
            )

        # Show connections list
        connections_list = connections_summary.get('connections', [])
        if connections_list:
            for conn in connections_list[:10]:  # Show first 10 connections
                company_name = conn.get('name', '')
                inn = conn.get('inn', '')
                director = conn.get('director', '')
                address = conn.get('address', '')
                reg_date = conn.get('registration_date', '')
                is_director = conn.get('is_director', 0) == 1

                role_badge = "🎯 " + get_text("connections_as_director", lang) if is_director else "💼 " + get_text("connections_as_founder", lang)

                st.markdown(f"""
                <div style="padding: 1rem; background-color: #f9fafb; border-radius: 0.5rem; margin-bottom: 0.75rem;">
                    <div style="display: flex; justify-content: space-between; align-items: start;">
                        <div style="flex: 1;">
                            <p style="margin: 0; font-weight: 600; font-size: 1rem;">{company_name}</p>
                            <p style="margin: 0.25rem 0; color: #666; font-size: 0.875rem;">{get_text("connection_inn", lang)}: {inn}</p>
                            {f'<p style="margin: 0.25rem 0; color: #666; font-size: 0.875rem;">{get_text("connection_director", lang)}: {director}</p>' if director else ''}
                            {f'<p style="margin: 0.25rem 0; color: #666; font-size: 0.875rem;">{get_text("connection_address", lang)}: {address}</p>' if address else ''}
                        </div>
                        <div style="text-align: right; margin-left: 1rem;">
                            <p style="margin: 0; font-size: 0.875rem; color: #3b82f6; font-weight: 600;">{role_badge}</p>
                            {f'<p style="margin: 0.25rem 0; color: #666; font-size: 0.75rem;">{reg_date}</p>' if reg_date else ''}
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info(get_text("no_connections_data", lang))

    # ========== TAB 4: LICENSES ==========
    with tab4:
        licenses_list = get_licenses_list(company_inn)

        if licenses_list:
            st.metric(get_text("total_active_licenses", lang), len(licenses_list))

            # Display licenses as cards in a grid
            cols_per_row = 3
            for i in range(0, len(licenses_list), cols_per_row):
                cols = st.columns(cols_per_row)
                for j, lic in enumerate(licenses_list[i:i+cols_per_row]):
                    with cols[j]:
                        category = lic.get('category', lic.get('category_en', ''))
                        number = lic.get('number', '')
                        issue_date = lic.get('date_of_issue', '')
                        validity = lic.get('validity', '')
                        status = lic.get('status', 0)

                        status_color = "#10b981" if status == 1 else "#ef4444"
                        status_text = get_text("license_active", lang) if status == 1 else get_text("license_expired", lang)

                        st.markdown(f"""
                        <div style="padding: 1rem; background-color: #f9fafb; border-radius: 0.5rem; border-left: 4px solid {status_color}; margin-bottom: 1rem;">
                            <p style="margin: 0; font-weight: 700; font-size: 1rem; color: #1f2937;">{category}</p>
                            <p style="margin: 0.5rem 0; color: #666; font-size: 0.875rem;">{get_text("license_number", lang)}: {number}</p>
                            <p style="margin: 0.25rem 0; color: #666; font-size: 0.875rem;">{get_text("license_issue_date", lang)}: {issue_date}</p>
                            <p style="margin: 0.25rem 0; color: #666; font-size: 0.875rem;">{get_text("license_validity", lang)}: {validity}</p>
                            <p style="margin: 0.5rem 0 0 0; font-size: 0.75rem; color: {status_color}; font-weight: 600;">{status_text}</p>
                        </div>
                        """, unsafe_allow_html=True)
        else:
            st.info(get_text("no_licenses_data", lang))

    st.markdown("---")

    # Download report button (placeholder)
    st.markdown(f"""
    <div style="text-align: center; margin: 2rem 0;">
        <a href="#" style="
            display: inline-block;
            padding: 0.75rem 1.5rem;
            background-color: #f3f4f6;
            color: #374151;
            text-decoration: none;
            border-radius: 0.375rem;
            font-weight: 600;
            pointer-events: none;
            opacity: 0.6;
        ">{get_text("download_report", lang)}</a>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
