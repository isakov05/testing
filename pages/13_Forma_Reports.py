import streamlit as st
import pandas as pd
import numpy as np
import re
from datetime import datetime
from io import BytesIO
import plotly.express as px
import plotly.graph_objects as go

# Page configuration
st.set_page_config(
    page_title="Forma Reports Dashboard",
    page_icon="📊",
    layout="wide"
)

# ==================== HELPER FUNCTIONS ====================

def normalize_column_name(name: str) -> str:
    """Normalize column names for comparison"""
    if name is None:
        return ''
    return re.sub(r'\s+', ' ', str(name).strip().lower())


def find_column_by_keywords(df: pd.DataFrame, keyword_groups: list) -> str:
    """
    Find a column whose normalized name contains all keywords from any tuple in keyword_groups.
    keyword_groups: list of tuples, each tuple is a set of keywords that must all appear.
    """
    if df is None or df.empty:
        return None

    normalized_columns = {normalize_column_name(col): col for col in df.columns}

    for norm_name, original_name in normalized_columns.items():
        for keywords in keyword_groups:
            if all(keyword in norm_name for keyword in keywords):
                return original_name
    return None


# Keyword definitions for common columns in legal info files
STIR_KEYWORDS = [('stir',), ('inn',), ("stir", "tashkilot"), ("inn", "tashkilot")]
FULL_NAME_KEYWORDS = [("to'liq", 'nomi'), ('full', 'name'), ('poln', 'nomi'), ('poln', 'name')]
SHORT_NAME_KEYWORDS = [('qisqacha', 'nomi'), ('short', 'name'), ('qisqa', 'nomi'), ('brief', 'name')]
DIRECTOR_SURNAME_KEYWORDS = [('familiyasi',), ('surname',), ('familiya',)]
DIRECTOR_NAME_KEYWORDS = [('ismi',), ('name',)]
CHARTER_CAPITAL_KEYWORDS = [('ustav', 'fondi'), ('charter', 'capital')]
TAX_REGIME_KEYWORDS = [('soliq', 'rejimi'), ('tax', 'regime')]
STATUS_KEYWORDS = [('holati',), ('status',)]

@st.cache_data
def read_excel_file(file, filename: str) -> pd.DataFrame:
    """
    Read Excel file (XLSB or XLSX) and return DataFrame.
    Cached to avoid re-reading on every interaction.
    """
    try:
        # Determine engine based on file extension
        if filename.endswith('.xlsb'):
            df = pd.read_excel(file, engine="pyxlsb")
        elif filename.endswith('.xlsx'):
            df = pd.read_excel(file, engine="openpyxl")
        else:
            st.error(f"Unsupported file format: {filename}")
            return None
        return df
    except Exception as e:
        st.error(f"Unable to read file {filename}: {str(e)}")
        return None


def is_legal_info_file(df: pd.DataFrame) -> bool:
    """
    Detect if the DataFrame is a legal information file.
    """
    if df is None or df.empty:
        return False

    stir_col = find_column_by_keywords(df, STIR_KEYWORDS)
    full_name_col = find_column_by_keywords(df, FULL_NAME_KEYWORDS)
    return bool(stir_col and full_name_col)


def get_company_name_mapping(legal_df: pd.DataFrame) -> dict:
    """
    Create a mapping of STIR to company names from legal info file.
    Returns dict: {STIR: {'full_name': ..., 'short_name': ..., 'director': ...}}
    """
    if legal_df is None or not is_legal_info_file(legal_df):
        return {}

    stir_col = find_column_by_keywords(legal_df, STIR_KEYWORDS)
    short_name_col = find_column_by_keywords(legal_df, SHORT_NAME_KEYWORDS)
    full_name_col = find_column_by_keywords(legal_df, FULL_NAME_KEYWORDS)
    director_surname_col = find_column_by_keywords(legal_df, DIRECTOR_SURNAME_KEYWORDS)
    director_name_col = find_column_by_keywords(legal_df, DIRECTOR_NAME_KEYWORDS)
    charter_capital_col = find_column_by_keywords(legal_df, CHARTER_CAPITAL_KEYWORDS)
    tax_regime_col = find_column_by_keywords(legal_df, TAX_REGIME_KEYWORDS)
    status_col = find_column_by_keywords(legal_df, STATUS_KEYWORDS)

    if not stir_col:
        return {}

    mapping = {}
    for _, row in legal_df.iterrows():
        stir = row.get(stir_col)
        stir_key = normalize_stir_value(stir)
        if stir_key is None:
            continue
        mapping[stir_key] = {
            'full_name': row.get(full_name_col, '') if full_name_col else '',
            'short_name': row.get(short_name_col, '') if short_name_col else '',
            'director_surname': row.get(director_surname_col, '') if director_surname_col else '',
            'director_name': row.get(director_name_col, '') if director_name_col else '',
            'charter_capital': row.get(charter_capital_col, 0) if charter_capital_col else 0,
            'tax_regime': row.get(tax_regime_col, None) if tax_regime_col else None,
            'status': row.get(status_col, None) if status_col else None
        }
    return mapping


def normalize_stir_value(value):
    """
    Normalize STIR values (float/int/str) to a consistent integer key.
    Returns None if the value cannot be parsed.
    """
    if value is None:
        return None

    if isinstance(value, (int, np.integer)):
        return int(value)

    if isinstance(value, (float, np.floating)):
        if np.isnan(value):
            return None
        return int(round(value))

    if isinstance(value, str):
        value_str = value.strip().replace("'", "").replace('"', '').replace(' ', '')
        if not value_str:
            return None

        # Try direct integer conversion
        if value_str.isdigit():
            return int(value_str)

        # Try float conversion (handles values like "200542182.0")
        try:
            return int(round(float(value_str)))
        except ValueError:
            pass

        # Extract first continuous digit sequence as fallback
        digit_groups = re.findall(r'\d+', value_str)
        if digit_groups:
            return int(digit_groups[0])

        return None

    # Fallback: attempt string conversion
    try:
        return int(float(str(value)))
    except (ValueError, TypeError):
        return None


def clean_company_name(value: str) -> str:
    """
    Clean company name strings. Returns None if value is empty/invalid.
    """
    if value is None:
        return None

    if isinstance(value, float) and np.isnan(value):
        return None

    value_str = str(value).strip()
    if not value_str or value_str.lower() in {'nan', 'none', 'null'}:
        return None

    return value_str


def build_company_name_lookup(df: pd.DataFrame) -> dict:
    """
    Build a lookup dictionary {normalized_stir: company_name} from the dataframe.
    """
    if df is None or df.empty or 'STIR' not in df.columns or 'Company Name' not in df.columns:
        return {}

    lookup = {}
    subset = df[['STIR', 'Company Name']].dropna(subset=['STIR'])

    for stir, name in subset.itertuples(index=False):
        stir_key = normalize_stir_value(stir)
        clean_name = clean_company_name(name)
        if stir_key is None or not clean_name or clean_name.upper().startswith('STIR '):
            continue
        lookup[stir_key] = clean_name

    return lookup


def lookup_company_mapping_name(stir, company_mapping: dict):
    """
    Get company name from mapping dict using normalized STIR.
    """
    if not company_mapping:
        return None

    stir_key = normalize_stir_value(stir)
    if stir_key is None:
        return None

    entry = company_mapping.get(stir_key)
    if not entry:
        return None

    return entry.get('short_name') or entry.get('full_name')


def get_company_label(stir, lookup: dict) -> str:
    """
    Resolve display label for a STIR using the provided lookup dict.
    """
    stir_key = normalize_stir_value(stir)
    if stir_key is not None:
        name = lookup.get(stir_key)
        if name:
            return name
    return f"STIR {stir}"


def enrich_with_company_names(df: pd.DataFrame, company_mapping: dict) -> pd.DataFrame:
    """
    Add/standardize company name column using available mapping or fallback columns.
    """
    if df is None or df.empty:
        return df

    df_copy = df.copy()

    fallback_columns = []
    if 'Company Name' in df_copy.columns:
        fallback_columns.append('Company Name')

    short_name_col = find_column_by_keywords(df_copy, SHORT_NAME_KEYWORDS)
    if short_name_col and short_name_col not in fallback_columns:
        fallback_columns.append(short_name_col)

    full_name_col = find_column_by_keywords(df_copy, FULL_NAME_KEYWORDS)
    if full_name_col and full_name_col not in fallback_columns:
        fallback_columns.append(full_name_col)

    fallback_series = None
    if fallback_columns:
        fallback_series = pd.Series([None] * len(df_copy), index=df_copy.index, dtype="object")
        for col in fallback_columns:
            col_clean = df_copy[col].apply(clean_company_name)
            fallback_series = fallback_series.combine_first(col_clean)

    name_series = pd.Series([None] * len(df_copy), index=df_copy.index, dtype="object")

    if 'STIR' in df_copy.columns:
        name_series = df_copy['STIR'].apply(
            lambda stir: clean_company_name(lookup_company_mapping_name(stir, company_mapping))
        )

    if fallback_series is not None:
        name_series = name_series.combine_first(fallback_series)

    if 'STIR' in df_copy.columns:
        stir_labels = df_copy['STIR'].apply(lambda stir: f"STIR {stir}" if pd.notna(stir) else '')
        empty_mask = name_series.apply(lambda x: clean_company_name(x) is None)
        name_series.loc[empty_mask] = stir_labels.loc[empty_mask]
    else:
        name_series = name_series.fillna('')

    df_copy['Company Name'] = name_series.apply(lambda x: clean_company_name(x) or x or '')

    if 'STIR' in df_copy.columns:
        cols = list(df_copy.columns)
        cols.remove('Company Name')
        stir_idx = cols.index('STIR')
        cols.insert(stir_idx + 1, 'Company Name')
        df_copy = df_copy[cols]

    return df_copy


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the DataFrame:
    - Strip whitespace from string columns
    - Remove newlines from column names
    - Parse date columns
    - Ensure numeric columns are properly typed
    """
    if df is None or df.empty:
        return df

    try:
        # Clean column names (strip whitespace, remove newlines)
        df.columns = df.columns.str.strip().str.replace('\n', ' ').str.replace('\r', '')

        # Strip whitespace from all string columns
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()

        # Parse date columns (look for columns with "sana" - date in Uzbek)
        date_columns = [col for col in df.columns if 'sana' in col.lower()]
        for col in date_columns:
            try:
                # Try multiple date formats
                df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed')
            except:
                pass

        # Convert numeric columns (already done by pandas, but ensure consistency)
        for col in df.columns:
            if df[col].dtype in ['float64', 'int64']:
                # Replace inf with NaN
                df[col] = df[col].replace([np.inf, -np.inf], np.nan)

        return df

    except Exception as e:
        st.warning(f"Some cleaning operations failed: {str(e)}")
        return df


def apply_filters(df: pd.DataFrame, year_filter=None, quarter_filter=None, stir_search=None) -> pd.DataFrame:
    """
    Apply filters to the DataFrame based on user selections.
    """
    if df is None or df.empty:
        return df

    filtered_df = df.copy()

    # Year filter
    if year_filter and 'Yil' in df.columns:
        filtered_df = filtered_df[filtered_df['Yil'].isin(year_filter)]

    # Quarter filter
    if quarter_filter and 'Choraklik' in df.columns:
        filtered_df = filtered_df[filtered_df['Choraklik'].isin(quarter_filter)]

    # STIR search (text search in STIR column)
    if stir_search and 'STIR' in df.columns:
        filtered_df = filtered_df[
            filtered_df['STIR'].astype(str).str.contains(stir_search, case=False, na=False)
        ]

    return filtered_df


def to_csv(df: pd.DataFrame) -> str:
    """Convert DataFrame to CSV string for download."""
    return df.to_csv(index=False).encode('utf-8')


def get_summary_stats(df: pd.DataFrame) -> dict:
    """
    Generate summary statistics for the DataFrame.
    """
    if df is None or df.empty:
        return {}

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    return {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'numeric_columns': len(numeric_cols),
        'date_columns': len([col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]),
        'missing_values': df.isnull().sum().sum(),
        'duplicate_rows': df.duplicated().sum()
    }


def calculate_financial_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate financial ratios for Forma 1 (Balance Sheet) data.
    Returns a DataFrame with ratios for each company.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # Check if this is Forma 1 (Balance Sheet) data
    if 'Balans aktivi bo\'yicha jami (S400)' not in df.columns:
        return pd.DataFrame()

    ratios = []

    for idx, row in df.iterrows():
        company_ratios = {'STIR': row.get('STIR', 'N/A')}

        # Add company name if available
        if 'Company Name' in df.columns:
            company_ratios['Company Name'] = row.get('Company Name', '')

        # Get key balance sheet items
        total_assets = row.get('Balans aktivi bo\'yicha jami (S400)', 0)
        current_assets = row.get('II bo\'lim bo\'yicha jami (S390)', 0)
        current_liabilities = row.get('Joriy majburiyatlar (S600)', 0)
        total_equity = row.get('I bo\'lim bo\'yicha jami (S480)', 0)
        total_liabilities = row.get('ІІ bo\'lim bo\'yicha jami (S770)', 0)
        cash = row.get('Pul mablag\'lari (S320)', 0)
        inventory = row.get('Tovar-moddiy zahiralari, jami (S140)', 0)

        # Calculate ratios
        # 1. Current Ratio (Liquidity)
        if current_liabilities and current_liabilities != 0:
            company_ratios['Current Ratio'] = round(current_assets / current_liabilities, 2)
        else:
            company_ratios['Current Ratio'] = np.nan

        # 2. Quick Ratio (Acid Test)
        if current_liabilities and current_liabilities != 0:
            quick_assets = current_assets - inventory
            company_ratios['Quick Ratio'] = round(quick_assets / current_liabilities, 2)
        else:
            company_ratios['Quick Ratio'] = np.nan

        # 3. Debt to Equity Ratio
        if total_equity and total_equity != 0:
            company_ratios['Debt to Equity'] = round(total_liabilities / total_equity, 2)
        else:
            company_ratios['Debt to Equity'] = np.nan

        # 4. Debt to Assets Ratio
        if total_assets and total_assets != 0:
            company_ratios['Debt to Assets'] = round(total_liabilities / total_assets, 2)
        else:
            company_ratios['Debt to Assets'] = np.nan

        # 5. Equity Ratio
        if total_assets and total_assets != 0:
            company_ratios['Equity Ratio'] = round(total_equity / total_assets, 2)
        else:
            company_ratios['Equity Ratio'] = np.nan

        ratios.append(company_ratios)

    return pd.DataFrame(ratios)


def calculate_profitability_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate profitability metrics for Forma 2 (Income Statement) data.
    Returns a DataFrame with metrics for each company.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # Check if this is Forma 2 (Income Statement) data
    if 'Mahsulot (tovar, ish va xizmat) larni sotishdan sof tushum (S010)' not in df.columns:
        return pd.DataFrame()

    metrics = []

    for idx, row in df.iterrows():
        company_metrics = {'STIR': row.get('STIR', 'N/A')}

        # Add company name if available
        if 'Company Name' in df.columns:
            company_metrics['Company Name'] = row.get('Company Name', '')

        # Get key income statement items
        revenue = row.get('Mahsulot (tovar, ish va xizmat) larni sotishdan sof tushum (S010)', 0)
        gross_profit = row.get('Mahsulot (tovar, ish va xizmat) larni sotishning yalpi foydasi (zarari) (S030)', 0)
        operating_profit = row.get('Asosiy faoliyatning foydasi (zarari) (S100)', 0)
        net_profit = row.get('Hisobot davrining sof foydasi (zarari) (S270)', 0)
        cost_of_sales = row.get('Sotilgan mahsulot (tovar, ish va xizmat) larnining tannarxi (S020)', 0)

        # Calculate metrics
        # 1. Gross Profit Margin
        if revenue and revenue != 0:
            company_metrics['Gross Profit Margin (%)'] = round((gross_profit / revenue) * 100, 2)
        else:
            company_metrics['Gross Profit Margin (%)'] = np.nan

        # 2. Operating Profit Margin
        if revenue and revenue != 0:
            company_metrics['Operating Margin (%)'] = round((operating_profit / revenue) * 100, 2)
        else:
            company_metrics['Operating Margin (%)'] = np.nan

        # 3. Net Profit Margin
        if revenue and revenue != 0:
            company_metrics['Net Profit Margin (%)'] = round((net_profit / revenue) * 100, 2)
        else:
            company_metrics['Net Profit Margin (%)'] = np.nan

        # 4. Cost to Revenue Ratio
        if revenue and revenue != 0:
            company_metrics['Cost Ratio (%)'] = round((cost_of_sales / revenue) * 100, 2)
        else:
            company_metrics['Cost Ratio (%)'] = np.nan

        # Add absolute values for context
        company_metrics['Revenue'] = revenue
        company_metrics['Net Profit'] = net_profit

        metrics.append(company_metrics)

    return pd.DataFrame(metrics)


def detect_anomalies(df: pd.DataFrame, column: str, threshold: float = 3.0) -> pd.DataFrame:
    """
    Detect anomalies in a numeric column using z-score method.
    Returns DataFrame with anomalies.
    """
    if df is None or df.empty or column not in df.columns:
        return pd.DataFrame()

    # Calculate z-scores
    mean = df[column].mean()
    std = df[column].std()

    if std == 0:
        return pd.DataFrame()

    df['z_score'] = np.abs((df[column] - mean) / std)
    anomalies = df[df['z_score'] > threshold].copy()

    return anomalies[['STIR', column, 'z_score']].sort_values('z_score', ascending=False) if 'STIR' in df.columns else pd.DataFrame()


# ==================== COMPANY PROFILES DASHBOARD ====================

def render_company_profiles(legal_df: pd.DataFrame, filename: str, tab_key: str):
    """
    Render company profiles dashboard for legal information file.
    """
    st.subheader("🏢 Company Directory")

    # Search functionality
    search_col1, search_col2 = st.columns([2, 1])

    with search_col1:
        search_term = st.text_input(
            "Search by Company Name or STIR",
            key=f"company_search_{tab_key}",
            help="Enter company name or STIR to filter"
        )

    with search_col2:
        sort_by = st.selectbox(
            "Sort by",
            options=['Company Name', 'Charter Capital', 'STIR'],
            key=f"sort_by_{tab_key}"
        )

    # Filter dataframe based on search
    filtered_df = legal_df.copy()
    if search_term:
        mask = (
            filtered_df['STIR (Tashkilot)'].astype(str).str.contains(search_term, case=False, na=False) |
            filtered_df["Subyektning to'liq nomi (Tashkilot)"].astype(str).str.contains(search_term, case=False, na=False) |
            filtered_df["Subyektning qisqacha nomi (Tashkilot)"].astype(str).str.contains(search_term, case=False, na=False)
        )
        filtered_df = filtered_df[mask]

    st.info(f"Showing {len(filtered_df)} of {len(legal_df)} companies")

    # Summary metrics
    st.markdown("---")
    st.header("📊 Overview")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_companies = len(legal_df)
        st.metric("Total Companies", f"{total_companies:,}")

    with col2:
        total_capital = legal_df['Ustav fondi miqdori (Tashkilot)'].sum()
        st.metric("Total Charter Capital", f"{total_capital/1e9:.1f}B")

    with col3:
        avg_capital = legal_df['Ustav fondi miqdori (Tashkilot)'].mean()
        st.metric("Avg Charter Capital", f"{avg_capital/1e6:.1f}M")

    with col4:
        active_companies = (legal_df['Holati (Tashkilot)'] == 1).sum()
        st.metric("Active Companies", f"{active_companies:,}")

    # Charter Capital Distribution
    st.markdown("---")
    st.header("💰 Charter Capital Analysis")

    col_viz1, col_viz2 = st.columns(2)

    with col_viz1:
        # Histogram of charter capital
        capital_data = legal_df[legal_df['Ustav fondi miqdori (Tashkilot)'] > 0].copy()
        capital_data['Capital (Millions)'] = capital_data['Ustav fondi miqdori (Tashkilot)'] / 1e6

        fig_hist = px.histogram(
            capital_data,
            x='Capital (Millions)',
            title='Charter Capital Distribution',
            labels={'Capital (Millions)': 'Charter Capital (Millions)', 'count': 'Number of Companies'},
            nbins=30
        )
        st.plotly_chart(fig_hist, use_container_width=True, key=f"capital_hist_{tab_key}")

    with col_viz2:
        # Top 10 by charter capital
        top_capital = legal_df.nlargest(10, 'Ustav fondi miqdori (Tashkilot)')[
            ['STIR (Tashkilot)', "Subyektning qisqacha nomi (Tashkilot)", 'Ustav fondi miqdori (Tashkilot)']
        ].copy()
        top_capital['Capital (Billions)'] = top_capital['Ustav fondi miqdori (Tashkilot)'] / 1e9

        fig_top = px.bar(
            top_capital,
            x="Subyektning qisqacha nomi (Tashkilot)",
            y='Capital (Billions)',
            title='Top 10 Companies by Charter Capital',
            labels={' Subyektning qisqacha nomi (Tashkilot)': 'Company', 'Capital (Billions)': 'Capital (Billions)'}
        )
        fig_top.update_layout(xaxis={'tickangle': -45})
        st.plotly_chart(fig_top, use_container_width=True, key=f"top_capital_{tab_key}")

    # ==================== PIE CHARTS FOR CATEGORICAL DATA ====================

    st.markdown("---")
    st.header("📊 Categorical Distributions")

    col_pie1, col_pie2 = st.columns(2)

    with col_pie1:
        # Tax Regime Distribution
        if 'Soliq rejimi (Tashkilot)' in legal_df.columns:
            tax_regime_counts = legal_df['Soliq rejimi (Tashkilot)'].value_counts()

            # Map codes to readable names if needed
            tax_regime_labels = {
                1: 'General Tax Regime',
                2: 'Simplified Tax Regime',
                3: 'Special Tax Regime',
                4: 'Other'
            }

            labels = [tax_regime_labels.get(idx, f'Type {idx}') for idx in tax_regime_counts.index]

            fig_tax = px.pie(
                values=tax_regime_counts.values,
                names=labels,
                title='Tax Regime Distribution',
                hole=0.3,
                color_discrete_sequence=px.colors.sequential.Plasma
            )
            fig_tax.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_tax, use_container_width=True, key=f"tax_regime_pie_{tab_key}")
        else:
            st.info("Tax regime data not available")

    with col_pie2:
        # Company Status Distribution
        if 'Holati (Tashkilot)' in legal_df.columns:
            status_counts = legal_df['Holati (Tashkilot)'].value_counts()

            status_labels = {
                1: 'Active',
                0: 'Inactive',
                2: 'Suspended',
                3: 'Liquidated'
            }

            labels = [status_labels.get(idx, f'Status {idx}') for idx in status_counts.index]

            fig_status = px.pie(
                values=status_counts.values,
                names=labels,
                title='Company Status Distribution',
                color_discrete_sequence=px.colors.sequential.Viridis
            )
            fig_status.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_status, use_container_width=True, key=f"status_pie_{tab_key}")
        else:
            st.info("Company status data not available")

    # Additional categorical visualizations
    col_pie3, col_pie4 = st.columns(2)

    with col_pie3:
        # Ownership Form Distribution (if available)
        if "Tashkiliy-huquqiy shakli (Tashkilot)" in legal_df.columns:
            ownership_counts = legal_df["Tashkiliy-huquqiy shakli (Tashkilot)"].value_counts().head(10)

            fig_ownership = px.bar(
                x=ownership_counts.index.astype(str),
                y=ownership_counts.values,
                title='Top 10 Ownership Forms',
                labels={'x': 'Ownership Type', 'y': 'Count'},
                color=ownership_counts.values,
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig_ownership, use_container_width=True, key=f"ownership_bar_{tab_key}")
        else:
            # Charter Capital by Company (Sunburst alternative)
            st.info("Ownership form distribution not available")

    with col_pie4:
        # Registration year distribution if available
        if "Ro'yxatga olish sanasi (Tashkilot)" in legal_df.columns:
            reg_dates = pd.to_datetime(legal_df["Ro'yxatga olish sanasi (Tashkilot)"], errors='coerce')
            legal_df_temp = legal_df.copy()
            legal_df_temp['Registration Year'] = reg_dates.dt.year

            year_counts = legal_df_temp['Registration Year'].value_counts().sort_index()

            fig_year = px.line(
                x=year_counts.index,
                y=year_counts.values,
                title='Company Registrations by Year',
                labels={'x': 'Year', 'y': 'Number of Registrations'},
                markers=True
            )
            st.plotly_chart(fig_year, use_container_width=True, key=f"reg_year_line_{tab_key}")
        else:
            st.info("Registration date not available")

    # Company Table
    st.markdown("---")
    st.header("📋 Company Directory")

    # Prepare display columns
    display_cols = [
        'STIR (Tashkilot)',
        "Subyektning qisqacha nomi (Tashkilot)",
        'Ustav fondi miqdori (Tashkilot)',
        'Familiyasi (Rahbar)',
        'Ismi (Rahbar)',
        'Soliq rejimi (Tashkilot)',
        "Ro'yxatga olish sanasi (Tashkilot)"
    ]

    display_df = filtered_df[display_cols].copy()
    display_df.columns = ['STIR', 'Company Name', 'Charter Capital', 'Director Surname', 'Director Name', 'Tax Regime', 'Registration Date']

    # Format charter capital
    display_df['Charter Capital'] = display_df['Charter Capital'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A")

    st.dataframe(display_df, use_container_width=True, height=500)

    # Download functionality
    st.markdown("---")
    st.header("💾 Export Data")

    csv_data = to_csv(legal_df)
    st.download_button(
        label="📥 Download Complete Company Directory (CSV)",
        data=csv_data,
        file_name="company_directory.csv",
        mime="text/csv",
        help="Download all company information",
        key=f"download_legal_{tab_key}"
    )


# ==================== DASHBOARD FUNCTION ====================

def render_dashboard(df: pd.DataFrame, filename: str, tab_key: str, company_mapping: dict = None):
    """
    Render the complete dashboard for a single file.
    """
    # Enrich with company names (from mapping or inline columns)
    if 'STIR' in df.columns or any(
        col in df.columns for col in ["Company Name", "Subyektning qisqacha nomi (Tashkilot)", "Subyektning to'liq nomi (Tashkilot)"]
    ):
        df = enrich_with_company_names(df, company_mapping or {})
        if company_mapping:
            st.success(f"✨ Enriched with company names from legal information file")

    # Filters section
    st.subheader("🔍 Filters")

    filter_col1, filter_col2, filter_col3 = st.columns(3)

    with filter_col1:
        # Year filter
        year_filter = None
        if 'Yil' in df.columns:
            years = sorted(df['Yil'].dropna().unique())
            if len(years) > 0:
                year_filter = st.multiselect(
                    "Year (Yil)",
                    options=years,
                    default=years,
                    help="Filter by year",
                    key=f"year_{tab_key}"
                )

    with filter_col2:
        # Quarter filter
        quarter_filter = None
        if 'Choraklik' in df.columns:
            quarters = sorted(df['Choraklik'].dropna().unique())
            if len(quarters) > 0:
                quarter_filter = st.multiselect(
                    "Quarter (Choraklik)",
                    options=quarters,
                    default=quarters,
                    help="Filter by quarter",
                    key=f"quarter_{tab_key}"
                )

    with filter_col3:
        # STIR search
        stir_search = None
        if 'STIR' in df.columns:
            stir_search = st.text_input(
                "STIR Search",
                help="Search for specific STIR (Tax ID)",
                key=f"stir_{tab_key}"
            )
            if stir_search == "":
                stir_search = None

    # Apply filters
    df_filtered = apply_filters(
        df,
        year_filter=year_filter,
        quarter_filter=quarter_filter,
        stir_search=stir_search
    )

    st.markdown("---")

    # ==================== SUMMARY SECTION ====================

    st.header("📈 Summary")

    # Get summary statistics
    summary = get_summary_stats(df_filtered)

    # Display metrics in columns
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Rows", f"{summary.get('total_rows', 0):,}")

    with col2:
        st.metric("Total Columns", summary.get('total_columns', 0))

    with col3:
        st.metric("Numeric Fields", summary.get('numeric_columns', 0))

    with col4:
        st.metric("Missing Values", f"{summary.get('missing_values', 0):,}")

    # Additional info
    st.markdown("---")

    col5, col6 = st.columns(2)

    with col5:
        st.info(f"**Uploaded File:** {filename}")

    with col6:
        st.info(f"**Date Columns:** {summary.get('date_columns', 0)} | **Duplicate Rows:** {summary.get('duplicate_rows', 0):,}")

    # ==================== DATA STATISTICS ====================

    st.header("📊 Data Statistics")

    # Show describe() for numeric columns
    numeric_df = df_filtered.select_dtypes(include=[np.number])

    if not numeric_df.empty:
        with st.expander("📋 Statistical Summary (Numeric Columns)", expanded=False):
            st.dataframe(numeric_df.describe(), use_container_width=True)
    else:
        st.warning("No numeric columns found in the dataset.")

    # ==================== VISUALIZATIONS ====================

    st.header("📉 Visualizations")

    # Create tabs for different visualizations
    viz_tab1, viz_tab2, viz_tab3, viz_tab4, viz_tab5 = st.tabs(["Distribution", "Top Companies", "Pie Charts", "Box Plots", "Trends & Treemap"])

    with viz_tab1:
        # Show distribution of records by Year and Quarter if available
        if 'Yil' in df_filtered.columns and 'Choraklik' in df_filtered.columns:
            records_by_period = df_filtered.groupby(['Yil', 'Choraklik']).size().reset_index(name='Count')
            records_by_period['Period'] = records_by_period['Yil'].astype(str) + ' Q' + records_by_period['Choraklik'].astype(str)

            fig = px.bar(
                records_by_period,
                x='Period',
                y='Count',
                title='Records by Year and Quarter',
                labels={'Count': 'Number of Records', 'Period': 'Period'},
                color='Count',
                color_continuous_scale='Blues'
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True, key=f"period_chart_{tab_key}")
        else:
            st.info("Year (Yil) and Quarter (Choraklik) columns not found for period distribution.")

    with viz_tab2:
        # Show top companies by first numeric column (if STIR exists)
        if 'STIR' in df_filtered.columns and not numeric_df.empty:
            # Get first numeric column
            first_numeric_col = numeric_df.columns[0]

            # Group by STIR and sum the first numeric column
            top_companies = df_filtered.groupby('STIR')[first_numeric_col].sum().sort_values(ascending=False).head(10)

            company_name_lookup = build_company_name_lookup(df_filtered)
            labels = [get_company_label(stir, company_name_lookup) for stir in top_companies.index]

            fig = px.bar(
                x=labels,
                y=top_companies.values,
                title=f'Top 10 Companies by {first_numeric_col}',
                labels={'x': 'Company', 'y': first_numeric_col},
                color=top_companies.values,
                color_continuous_scale='Greens'
            )
            fig.update_layout(showlegend=False, xaxis={'tickangle': -45})
            st.plotly_chart(fig, use_container_width=True, key=f"companies_chart_{tab_key}")
        else:
            st.info("STIR column not found or no numeric columns available for top companies analysis.")

    with viz_tab3:
        st.subheader("📊 Pie Charts - Categorical Distributions")

        # Check file type for appropriate pie charts
        is_forma1 = 'Balans aktivi bo\'yicha jami (S400)' in df.columns
        is_forma2 = 'Mahsulot (tovar, ish va xizmat) larni sotishdan sof tushum (S010)' in df.columns

        col_pie1, col_pie2 = st.columns(2)

        with col_pie1:
            if is_forma2:
                # Revenue distribution pie chart - Top 10 companies
                revenue_col = 'Mahsulot (tovar, ish va xizmat) larni sotishdan sof tushum (S010)'
                if 'STIR' in df_filtered.columns:
                    revenue_data = df_filtered.groupby('STIR')[revenue_col].sum().sort_values(ascending=False).head(10)

                    company_name_lookup = build_company_name_lookup(df_filtered)
                    labels = [get_company_label(stir, company_name_lookup) for stir in revenue_data.index]

                    fig_pie1 = px.pie(
                        values=revenue_data.values,
                        names=labels,
                        title='Top 10 Companies by Revenue Share',
                        hole=0.3  # Donut chart
                    )
                    fig_pie1.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig_pie1, use_container_width=True, key=f"revenue_pie_{tab_key}")
                else:
                    st.info("STIR column not found for revenue distribution")

            elif is_forma1:
                # Assets distribution pie chart - Top 10 companies
                assets_col = 'Balans aktivi bo\'yicha jami (S400)'
                if 'STIR' in df_filtered.columns:
                    assets_data = df_filtered.groupby('STIR')[assets_col].sum().sort_values(ascending=False).head(10)

                    company_name_lookup = build_company_name_lookup(df_filtered)
                    labels = [get_company_label(stir, company_name_lookup) for stir in assets_data.index]

                    fig_pie1 = px.pie(
                        values=assets_data.values,
                        names=labels,
                        title='Top 10 Companies by Total Assets Share',
                        hole=0.3
                    )
                    fig_pie1.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig_pie1, use_container_width=True, key=f"assets_pie_{tab_key}")
                else:
                    st.info("STIR column not found for assets distribution")
            else:
                st.info("Pie charts available for Forma 1 or Forma 2 files")

        with col_pie2:
            # Quarter distribution if available
            if 'Choraklik' in df_filtered.columns:
                quarter_counts = df_filtered['Choraklik'].value_counts()
                fig_pie2 = px.pie(
                    values=quarter_counts.values,
                    names=[f'Q{q}' for q in quarter_counts.index],
                    title='Records Distribution by Quarter',
                    color_discrete_sequence=px.colors.sequential.RdBu
                )
                st.plotly_chart(fig_pie2, use_container_width=True, key=f"quarter_pie_{tab_key}")
            else:
                # Year distribution as alternative
                if 'Yil' in df_filtered.columns:
                    year_counts = df_filtered['Yil'].value_counts()
                    fig_pie2 = px.pie(
                        values=year_counts.values,
                        names=year_counts.index,
                        title='Records Distribution by Year',
                        color_discrete_sequence=px.colors.sequential.Viridis
                    )
                    st.plotly_chart(fig_pie2, use_container_width=True, key=f"year_pie_{tab_key}")
                else:
                    st.info("No temporal columns found for distribution")

    with viz_tab4:
        st.subheader("📦 Box Plots - Distribution Analysis")

        # Select numeric columns for box plots
        if not numeric_df.empty:
            # Show box plots for key metrics
            numeric_cols_list = numeric_df.columns.tolist()

            # Select column for box plot
            selected_box_col = st.selectbox(
                "Select metric for box plot",
                options=numeric_cols_list[:20],  # First 20 numeric columns
                key=f"boxplot_col_{tab_key}",
                help="View distribution and outliers"
            )

            if selected_box_col:
                col_box1, col_box2 = st.columns(2)

                with col_box1:
                    # Overall distribution box plot
                    fig_box1 = px.box(
                        df_filtered,
                        y=selected_box_col,
                        title=f'Distribution of {selected_box_col}',
                        labels={selected_box_col: selected_box_col},
                        points='outliers'  # Show outlier points
                    )
                    st.plotly_chart(fig_box1, use_container_width=True, key=f"box1_{tab_key}")

                with col_box2:
                    # Box plot by quarter if available
                    if 'Choraklik' in df_filtered.columns:
                        fig_box2 = px.box(
                            df_filtered,
                            x='Choraklik',
                            y=selected_box_col,
                            title=f'{selected_box_col} by Quarter',
                            labels={'Choraklik': 'Quarter', selected_box_col: selected_box_col},
                            points='outliers'
                        )
                        st.plotly_chart(fig_box2, use_container_width=True, key=f"box2_{tab_key}")
                    elif 'Yil' in df_filtered.columns:
                        fig_box2 = px.box(
                            df_filtered,
                            x='Yil',
                            y=selected_box_col,
                            title=f'{selected_box_col} by Year',
                            labels={'Yil': 'Year', selected_box_col: selected_box_col},
                            points='outliers'
                        )
                        st.plotly_chart(fig_box2, use_container_width=True, key=f"box2_{tab_key}")
                    else:
                        st.info("No temporal grouping available")
        else:
            st.info("No numeric columns available for box plots")

    with viz_tab5:
        st.subheader("📈 Time-Series Trends & Hierarchical Views")

        # Check file type for appropriate visualizations
        is_forma1 = 'Balans aktivi bo\'yicha jami (S400)' in df.columns
        is_forma2 = 'Mahsulot (tovar, ish va xizmat) larni sotishdan sof tushum (S010)' in df.columns

        # TREEMAP VISUALIZATION
        st.markdown("#### 🗺️ Treemap - Hierarchical Distribution")

        if 'STIR' in df_filtered.columns and not numeric_df.empty:
            # Select metric for treemap
            treemap_metric = st.selectbox(
                "Select metric for treemap",
                options=numeric_df.columns[:20].tolist(),
                key=f"treemap_metric_{tab_key}",
                help="Visualize hierarchical distribution of companies by metric"
            )

            if treemap_metric:
                # Prepare data for treemap - Top 15 companies
                treemap_data = df_filtered.groupby('STIR')[treemap_metric].sum().sort_values(ascending=False).head(15).reset_index()

                company_name_lookup = build_company_name_lookup(df_filtered)
                treemap_data['Company'] = treemap_data['STIR'].apply(
                    lambda stir: get_company_label(stir, company_name_lookup)
                )

                # Create treemap
                fig_treemap = px.treemap(
                    treemap_data,
                    path=['Company'],
                    values=treemap_metric,
                    title=f'Top 15 Companies - {treemap_metric} (Treemap)',
                    color=treemap_metric,
                    color_continuous_scale='Viridis',
                    hover_data=['STIR']
                )
                fig_treemap.update_layout(height=500)
                st.plotly_chart(fig_treemap, use_container_width=True, key=f"treemap_{tab_key}")
        else:
            st.info("Treemap requires STIR column and numeric data")

        st.markdown("---")

        # TIME-SERIES TREND ANALYSIS
        st.markdown("#### 📊 Time-Series Trend Analysis")

        if 'Yil' in df_filtered.columns and 'Choraklik' in df_filtered.columns and not numeric_df.empty:
            # Create period column
            df_trend = df_filtered.copy()
            df_trend['Period'] = df_trend['Yil'].astype(str) + '-Q' + df_trend['Choraklik'].astype(str)

            # Select metric for trend
            trend_metric = st.selectbox(
                "Select metric for trend analysis",
                options=numeric_df.columns[:15].tolist(),
                key=f"trend_metric_{tab_key}",
                help="View how this metric changes over time"
            )

            if trend_metric:
                col_trend1, col_trend2 = st.columns(2)

                with col_trend1:
                    # Aggregate trend line
                    trend_agg = df_trend.groupby('Period')[trend_metric].agg(['mean', 'sum', 'count']).reset_index()

                    fig_trend_line = go.Figure()

                    # Mean line
                    fig_trend_line.add_trace(go.Scatter(
                        x=trend_agg['Period'],
                        y=trend_agg['mean'],
                        mode='lines+markers',
                        name='Average',
                        line=dict(color='blue', width=3),
                        marker=dict(size=8)
                    ))

                    fig_trend_line.update_layout(
                        title=f'Average {trend_metric} Over Time',
                        xaxis_title='Period',
                        yaxis_title=f'Average {trend_metric}',
                        hovermode='x unified'
                    )

                    st.plotly_chart(fig_trend_line, use_container_width=True, key=f"trend_line_{tab_key}")

                with col_trend2:
                    # Total trend
                    fig_trend_bar = px.bar(
                        trend_agg,
                        x='Period',
                        y='sum',
                        title=f'Total {trend_metric} Over Time',
                        labels={'sum': f'Total {trend_metric}', 'Period': 'Period'},
                        color='sum',
                        color_continuous_scale='Blues'
                    )
                    st.plotly_chart(fig_trend_bar, use_container_width=True, key=f"trend_bar_{tab_key}")

                # Show top companies trends
                st.markdown("#### 🏆 Top Companies Performance Over Time")

                # Get top 5 companies by total metric
                top5_companies = df_trend.groupby('STIR')[trend_metric].sum().sort_values(ascending=False).head(5)

                if len(top5_companies) > 0:
                    # Filter data for top companies
                    top5_trend = df_trend[df_trend['STIR'].isin(top5_companies.index)].copy()

                    # Prepare company labels
                    if 'Company Name' in top5_trend.columns:
                        top5_trend['Label'] = top5_trend['Company Name']
                    else:
                        top5_trend['Label'] = 'STIR ' + top5_trend['STIR'].astype(str)

                    # Create multi-line chart
                    fig_multiline = px.line(
                        top5_trend,
                        x='Period',
                        y=trend_metric,
                        color='Label',
                        title=f'Top 5 Companies - {trend_metric} Trend',
                        labels={trend_metric: trend_metric, 'Period': 'Period'},
                        markers=True
                    )
                    fig_multiline.update_layout(height=400)
                    st.plotly_chart(fig_multiline, use_container_width=True, key=f"multiline_{tab_key}")

                # Area chart for stacked view
                st.markdown("#### 📊 Stacked Area - Market Composition")

                if len(top5_companies) > 0:
                    # Pivot data for stacked area
                    pivot_data = top5_trend.pivot_table(
                        index='Period',
                        columns='Label',
                        values=trend_metric,
                        aggfunc='sum',
                        fill_value=0
                    ).reset_index()

                    fig_area = go.Figure()

                    for col in pivot_data.columns[1:]:  # Skip 'Period' column
                        fig_area.add_trace(go.Scatter(
                            x=pivot_data['Period'],
                            y=pivot_data[col],
                            mode='lines',
                            name=col,
                            stackgroup='one',
                            fillcolor=None
                        ))

                    fig_area.update_layout(
                        title=f'Stacked Area - Top 5 Companies {trend_metric}',
                        xaxis_title='Period',
                        yaxis_title=f'Total {trend_metric}',
                        hovermode='x unified',
                        height=400
                    )

                    st.plotly_chart(fig_area, use_container_width=True, key=f"area_{tab_key}")

        elif 'Yil' in df_filtered.columns and not numeric_df.empty:
            # Only year available - show yearly trends
            st.info("Only yearly data available. Showing yearly trends.")

            trend_metric_yr = st.selectbox(
                "Select metric for yearly trend",
                options=numeric_df.columns[:15].tolist(),
                key=f"trend_metric_yr_{tab_key}"
            )

            if trend_metric_yr:
                yearly_agg = df_filtered.groupby('Yil')[trend_metric_yr].agg(['mean', 'sum']).reset_index()

                fig_yearly = px.bar(
                    yearly_agg,
                    x='Yil',
                    y='sum',
                    title=f'Total {trend_metric_yr} by Year',
                    labels={'sum': f'Total {trend_metric_yr}', 'Yil': 'Year'},
                    color='sum',
                    color_continuous_scale='Greens'
                )
                st.plotly_chart(fig_yearly, use_container_width=True, key=f"yearly_{tab_key}")
        else:
            st.info("Time-series analysis requires Year (Yil) and Quarter (Choraklik) columns")

    # ==================== FINANCIAL ANALYSIS ====================

    st.header("💰 Financial Analysis")

    # Check file type and show appropriate analysis
    is_forma1 = 'Balans aktivi bo\'yicha jami (S400)' in df.columns
    is_forma2 = 'Mahsulot (tovar, ish va xizmat) larni sotishdan sof tushum (S010)' in df.columns

    if is_forma1:
        # Forma 1: Balance Sheet Analysis
        st.subheader("📊 Financial Ratios (Balance Sheet)")

        ratios_df = calculate_financial_ratios(df_filtered)

        if not ratios_df.empty:
            # Show summary statistics for ratios
            col_r1, col_r2, col_r3 = st.columns(3)

            with col_r1:
                avg_current_ratio = ratios_df['Current Ratio'].mean()
                st.metric("Avg Current Ratio", f"{avg_current_ratio:.2f}" if not np.isnan(avg_current_ratio) else "N/A")

            with col_r2:
                avg_debt_equity = ratios_df['Debt to Equity'].mean()
                st.metric("Avg Debt to Equity", f"{avg_debt_equity:.2f}" if not np.isnan(avg_debt_equity) else "N/A")

            with col_r3:
                avg_equity_ratio = ratios_df['Equity Ratio'].mean()
                st.metric("Avg Equity Ratio", f"{avg_equity_ratio:.2f}" if not np.isnan(avg_equity_ratio) else "N/A")

            # Distribution of Current Ratio
            valid_current_ratios = ratios_df['Current Ratio'].dropna()
            if len(valid_current_ratios) > 0:
                fig_ratio = px.histogram(
                    ratios_df,
                    x='Current Ratio',
                    title='Distribution of Current Ratio (Liquidity)',
                    labels={'Current Ratio': 'Current Ratio', 'count': 'Number of Companies'},
                    nbins=20
                )
                st.plotly_chart(fig_ratio, use_container_width=True, key=f"current_ratio_dist_{tab_key}")

            # Show detailed ratios table
            with st.expander("📋 Detailed Financial Ratios by Company", expanded=False):
                # Reorder columns to show Company Name first if available
                if 'Company Name' in ratios_df.columns:
                    display_cols = ['Company Name', 'STIR'] + [col for col in ratios_df.columns if col not in ['Company Name', 'STIR']]
                    st.dataframe(ratios_df[display_cols], use_container_width=True, height=300)
                else:
                    st.dataframe(ratios_df, use_container_width=True, height=300)

    elif is_forma2:
        # Forma 2: Income Statement Analysis
        st.subheader("📈 Profitability Metrics (Income Statement)")

        metrics_df = calculate_profitability_metrics(df_filtered)

        if not metrics_df.empty:
            # Show summary statistics for metrics
            col_m1, col_m2, col_m3, col_m4 = st.columns(4)

            with col_m1:
                avg_gross_margin = metrics_df['Gross Profit Margin (%)'].mean()
                st.metric("Avg Gross Margin", f"{avg_gross_margin:.1f}%" if not np.isnan(avg_gross_margin) else "N/A")

            with col_m2:
                avg_operating_margin = metrics_df['Operating Margin (%)'].mean()
                st.metric("Avg Operating Margin", f"{avg_operating_margin:.1f}%" if not np.isnan(avg_operating_margin) else "N/A")

            with col_m3:
                avg_net_margin = metrics_df['Net Profit Margin (%)'].mean()
                st.metric("Avg Net Margin", f"{avg_net_margin:.1f}%" if not np.isnan(avg_net_margin) else "N/A")

            with col_m4:
                total_revenue = metrics_df['Revenue'].sum()
                st.metric("Total Revenue", f"{total_revenue:,.0f}")

            # Profitability distribution
            valid_net_margins = metrics_df['Net Profit Margin (%)'].dropna()
            if len(valid_net_margins) > 0:
                fig_profit = px.histogram(
                    metrics_df,
                    x='Net Profit Margin (%)',
                    title='Distribution of Net Profit Margin',
                    labels={'Net Profit Margin (%)': 'Net Profit Margin (%)', 'count': 'Number of Companies'},
                    nbins=20
                )
                st.plotly_chart(fig_profit, use_container_width=True, key=f"net_margin_dist_{tab_key}")

            # Top and Bottom Performers
            col_perf1, col_perf2 = st.columns(2)

            with col_perf1:
                st.markdown("**🏆 Top 10 Most Profitable (by Net Margin)**")
                if 'Company Name' in metrics_df.columns:
                    display_cols = ['Company Name', 'STIR', 'Net Profit Margin (%)', 'Revenue']
                else:
                    display_cols = ['STIR', 'Net Profit Margin (%)', 'Revenue']
                top_profitable = metrics_df.nlargest(10, 'Net Profit Margin (%)')[display_cols]
                st.dataframe(top_profitable, use_container_width=True, height=300)

            with col_perf2:
                st.markdown("**⚠️ Bottom 10 Performers (by Net Margin)**")
                if 'Company Name' in metrics_df.columns:
                    display_cols = ['Company Name', 'STIR', 'Net Profit Margin (%)', 'Revenue']
                else:
                    display_cols = ['STIR', 'Net Profit Margin (%)', 'Revenue']
                bottom_profitable = metrics_df.nsmallest(10, 'Net Profit Margin (%)')[display_cols]
                st.dataframe(bottom_profitable, use_container_width=True, height=300)

            # Show detailed metrics table
            with st.expander("📋 Detailed Profitability Metrics by Company", expanded=False):
                # Reorder columns to show Company Name first if available
                if 'Company Name' in metrics_df.columns:
                    display_cols = ['Company Name', 'STIR'] + [col for col in metrics_df.columns if col not in ['Company Name', 'STIR']]
                    st.dataframe(metrics_df[display_cols], use_container_width=True, height=300)
                else:
                    st.dataframe(metrics_df, use_container_width=True, height=300)

    else:
        st.info("Financial analysis is available for Forma 1 (Balance Sheet) or Forma 2 (Income Statement) files.")

    # ==================== CORRELATION ANALYSIS ====================

    st.header("🔥 Correlation Heatmap")

    if not numeric_df.empty and len(numeric_df.columns) > 1:
        # Select columns for correlation analysis
        st.info("Showing correlation between numeric metrics (first 15 columns for clarity)")

        # Limit to first 15 numeric columns for readability
        correlation_cols = numeric_df.columns[:min(15, len(numeric_df.columns))].tolist()

        if len(correlation_cols) > 1:
            # Calculate correlation matrix
            corr_matrix = df_filtered[correlation_cols].corr()

            # Create heatmap
            fig_heatmap = px.imshow(
                corr_matrix,
                labels=dict(x="Metric", y="Metric", color="Correlation"),
                x=corr_matrix.columns,
                y=corr_matrix.columns,
                color_continuous_scale='RdBu_r',
                zmin=-1,
                zmax=1,
                title='Correlation Matrix of Financial Metrics'
            )
            fig_heatmap.update_layout(height=600)
            st.plotly_chart(fig_heatmap, use_container_width=True, key=f"heatmap_{tab_key}")

            # Show top correlations
            with st.expander("🔍 Top Positive & Negative Correlations", expanded=False):
                # Get upper triangle of correlation matrix
                corr_pairs = []
                for i in range(len(corr_matrix.columns)):
                    for j in range(i+1, len(corr_matrix.columns)):
                        corr_pairs.append({
                            'Metric 1': corr_matrix.columns[i],
                            'Metric 2': corr_matrix.columns[j],
                            'Correlation': corr_matrix.iloc[i, j]
                        })

                corr_df = pd.DataFrame(corr_pairs)
                corr_df = corr_df.dropna()

                if not corr_df.empty:
                    col_corr1, col_corr2 = st.columns(2)

                    with col_corr1:
                        st.markdown("**📈 Top 10 Positive Correlations**")
                        top_positive = corr_df.nlargest(10, 'Correlation')
                        st.dataframe(top_positive, use_container_width=True)

                    with col_corr2:
                        st.markdown("**📉 Top 10 Negative Correlations**")
                        top_negative = corr_df.nsmallest(10, 'Correlation')
                        st.dataframe(top_negative, use_container_width=True)
        else:
            st.info("Need at least 2 numeric columns for correlation analysis")
    else:
        st.info("Correlation analysis requires numeric columns")

    # ==================== ANOMALY DETECTION ====================

    st.header("🔍 Anomaly Detection")

    # Select a numeric column for anomaly detection
    numeric_cols_for_anomaly = df_filtered.select_dtypes(include=[np.number]).columns.tolist()

    if numeric_cols_for_anomaly and 'STIR' in df_filtered.columns:
        selected_col = st.selectbox(
            "Select a column to detect anomalies",
            options=numeric_cols_for_anomaly,
            key=f"anomaly_col_{tab_key}"
        )

        threshold = st.slider(
            "Anomaly Threshold (Z-Score)",
            min_value=2.0,
            max_value=5.0,
            value=3.0,
            step=0.5,
            help="Higher threshold = fewer anomalies detected",
            key=f"anomaly_threshold_{tab_key}"
        )

        if selected_col:
            anomalies = detect_anomalies(df_filtered, selected_col, threshold)

            if not anomalies.empty:
                st.warning(f"⚠️ Found {len(anomalies)} anomalies in **{selected_col}**")

                # Add company names to anomalies if available
                if 'Company Name' in df_filtered.columns:
                    anomalies_display = anomalies.copy()
                    # Merge with company names
                    company_name_map = df_filtered.set_index('STIR')['Company Name'].to_dict()
                    anomalies_display['Company Name'] = anomalies_display['STIR'].map(company_name_map)
                    # Reorder columns to show Company Name first
                    cols = ['Company Name', 'STIR', selected_col, 'z_score']
                    anomalies_display = anomalies_display[cols]
                    st.dataframe(anomalies_display, use_container_width=True)
                else:
                    st.dataframe(anomalies, use_container_width=True)

                # Visualize anomalies
                fig_anomaly = go.Figure()

                # All data points
                fig_anomaly.add_trace(go.Scatter(
                    x=list(range(len(df_filtered))),
                    y=df_filtered[selected_col],
                    mode='markers',
                    name='Normal',
                    marker=dict(color='blue', size=8)
                ))

                # Anomalies
                anomaly_indices = anomalies.index.tolist()
                fig_anomaly.add_trace(go.Scatter(
                    x=anomaly_indices,
                    y=[df_filtered.loc[i, selected_col] for i in anomaly_indices],
                    mode='markers',
                    name='Anomaly',
                    marker=dict(color='red', size=12, symbol='x')
                ))

                fig_anomaly.update_layout(
                    title=f'Anomaly Detection: {selected_col}',
                    xaxis_title='Index',
                    yaxis_title=selected_col
                )

                st.plotly_chart(fig_anomaly, use_container_width=True, key=f"anomaly_chart_{tab_key}")
            else:
                st.success(f"✅ No anomalies detected in **{selected_col}** (threshold: {threshold})")
    else:
        st.info("Anomaly detection requires numeric columns and STIR column.")

    # ==================== COMPANY COMPARISON ====================

    st.header("🔄 Company Comparison")

    if 'STIR' in df_filtered.columns and len(df_filtered) > 1:
        # Select companies to compare
        all_stirs = df_filtered['STIR'].unique().tolist()

        # Create display options with company names if available
        if 'Company Name' in df_filtered.columns:
            stir_name_map = df_filtered.drop_duplicates('STIR').set_index('STIR')['Company Name'].to_dict()
            stir_options = {stir: f"{stir_name_map.get(stir, f'STIR {stir}')} ({stir})" for stir in all_stirs}

            selected_display = st.multiselect(
                "Select companies to compare",
                options=list(stir_options.values()),
                default=list(stir_options.values())[:min(5, len(all_stirs))],
                key=f"compare_stirs_{tab_key}"
            )

            # Convert back to STIR codes
            reverse_map = {v: k for k, v in stir_options.items()}
            selected_stirs = [reverse_map[disp] for disp in selected_display]
        else:
            selected_stirs = st.multiselect(
                "Select companies to compare (by STIR)",
                options=all_stirs,
                default=all_stirs[:min(5, len(all_stirs))],
                key=f"compare_stirs_{tab_key}"
            )

        if selected_stirs:
            comparison_df = df_filtered[df_filtered['STIR'].isin(selected_stirs)].copy()

            # Select numeric columns to compare
            numeric_cols_to_compare = df_filtered.select_dtypes(include=[np.number]).columns.tolist()

            if numeric_cols_to_compare:
                selected_metric = st.selectbox(
                    "Select metric to compare",
                    options=numeric_cols_to_compare[:20],  # Show first 20 to avoid clutter
                    key=f"compare_metric_{tab_key}"
                )

                if selected_metric:
                    # Prepare labels for comparison chart
                    if 'Company Name' in comparison_df.columns:
                        comparison_df_chart = comparison_df.copy()
                        comparison_df_chart['Display Name'] = comparison_df_chart['Company Name'].fillna(
                            'STIR ' + comparison_df_chart['STIR'].astype(str)
                        )
                        x_col = 'Display Name'
                        x_label = 'Company'
                    else:
                        comparison_df_chart = comparison_df.copy()
                        comparison_df_chart['Display Name'] = 'STIR ' + comparison_df_chart['STIR'].astype(str)
                        x_col = 'Display Name'
                        x_label = 'Company (STIR)'

                    # Create comparison bar chart
                    fig_compare = px.bar(
                        comparison_df_chart,
                        x=x_col,
                        y=selected_metric,
                        title=f'Comparison: {selected_metric}',
                        labels={x_col: x_label, selected_metric: selected_metric},
                        text=selected_metric
                    )
                    fig_compare.update_traces(texttemplate='%{text:.2s}', textposition='outside')
                    fig_compare.update_layout(xaxis={'tickangle': -45})
                    st.plotly_chart(fig_compare, use_container_width=True, key=f"compare_chart_{tab_key}")

                    # Show comparison table
                    st.subheader("Comparison Table")
                    if 'Company Name' in comparison_df.columns:
                        compare_cols = ['Company Name', 'STIR'] + [col for col in numeric_cols_to_compare[:10] if col not in ['STIR', 'Company Name']]
                    else:
                        compare_cols = ['STIR'] + [col for col in numeric_cols_to_compare[:10] if col not in ['STIR', 'Company Name']]
                    st.dataframe(comparison_df[compare_cols], use_container_width=True)
    else:
        st.info("Company comparison requires multiple companies with STIR column.")

    # ==================== DATA DISPLAY ====================

    st.header("📋 Data Table")

    # Show first 200 rows
    display_limit = min(200, len(df_filtered))
    st.info(f"Displaying first {display_limit} rows out of {len(df_filtered):,} total rows")

    # Display the dataframe
    st.dataframe(
        df_filtered.head(200),
        use_container_width=True,
        height=400
    )

    # ==================== DOWNLOAD SECTION ====================

    st.header("💾 Download Data")

    col_download1, col_download2 = st.columns(2)

    with col_download1:
        # Download cleaned CSV
        csv_data = to_csv(df_filtered)
        st.download_button(
            label="📥 Download Filtered CSV",
            data=csv_data,
            file_name=f"filtered_{filename}.csv",
            mime="text/csv",
            help="Download the cleaned and filtered data as CSV",
            key=f"download_filtered_{tab_key}"
        )

    with col_download2:
        # Download full dataset (no filter)
        csv_full = to_csv(df)
        st.download_button(
            label="📥 Download Full CSV",
            data=csv_full,
            file_name=f"full_{filename}.csv",
            mime="text/csv",
            help="Download the complete cleaned dataset (no filters applied)",
            key=f"download_full_{tab_key}"
        )

    # ==================== COLUMN INFORMATION ====================

    with st.expander("ℹ️ Column Information", expanded=False):
        st.subheader("Column Names and Data Types")

        col_info = pd.DataFrame({
            'Column Name': df.columns,
            'Data Type': df.dtypes.values,
            'Non-Null Count': df.count().values,
            'Null Count': df.isnull().sum().values,
            'Unique Values': [df[col].nunique() for col in df.columns]
        })

        st.dataframe(col_info, use_container_width=True, height=300)


# ==================== MAIN PAGE ====================

st.title("📊 Forma Reports Dashboard")
st.markdown("Upload and analyze Forma reports (XLSB) and Company Legal Information (XLSX) with automatic data cleaning, enrichment, and comprehensive analytics.")

# File uploader - NOW ACCEPTS MULTIPLE FILES (XLSB and XLSX)
uploaded_files = st.file_uploader(
    "Upload Excel file(s)",
    type=["xlsb", "xlsx"],
    accept_multiple_files=True,
    help="Upload Forma reports (XLSB) or Company Legal Info (XLSX) files"
)

# Initialize session state for uploaded files
if 'uploaded_files_data' not in st.session_state:
    st.session_state['uploaded_files_data'] = {}
if 'legal_info_file' not in st.session_state:
    st.session_state['legal_info_file'] = None
if 'company_mapping' not in st.session_state:
    st.session_state['company_mapping'] = {}

# Process uploaded files
if uploaded_files:
    for uploaded_file in uploaded_files:
        # Check if file is already processed
        if uploaded_file.name not in st.session_state['uploaded_files_data']:
            with st.spinner(f"Reading {uploaded_file.name}..."):
                df_raw = read_excel_file(uploaded_file, uploaded_file.name)

            if df_raw is not None:
                # Check if this is a legal info file
                if is_legal_info_file(df_raw):
                    st.session_state['legal_info_file'] = (uploaded_file.name, df_raw)
                    st.session_state['company_mapping'] = get_company_name_mapping(df_raw)
                    st.success(f"✅ {uploaded_file.name} identified as Legal Information file - {len(df_raw)} companies")
                    # Store it for display
                    st.session_state['uploaded_files_data'][uploaded_file.name] = df_raw
                else:
                    # Regular Forma file
                    with st.spinner(f"Cleaning {uploaded_file.name}..."):
                        df_cleaned = clean_dataframe(df_raw)

                    if df_cleaned is not None and not df_cleaned.empty:
                        # Store in session state
                        st.session_state['uploaded_files_data'][uploaded_file.name] = df_cleaned
                        st.success(f"✅ {uploaded_file.name} uploaded and cleaned successfully")
                    else:
                        st.error(f"Failed to clean {uploaded_file.name}. Please check the file format.")
            else:
                st.error(f"Failed to read {uploaded_file.name}. Please ensure it's a valid Excel file.")

# Check if data is loaded
if not st.session_state['uploaded_files_data']:
    st.info("👆 Please upload Forma reports (.xlsb) and/or Company Legal Info (.xlsx) files to get started.")
    st.markdown("""
    ### Supported Files:
    - **Forma 1** (Balance Sheet) - XLSB format
    - **Forma 2** (Income Statement) - XLSB format
    - **Company Legal Information** - XLSX format (enriches reports with company names and profiles)

    💡 *Tip: Upload all three for comprehensive company analysis!*
    """)
    st.stop()

# Display files in tabs
st.markdown("---")

# Show summary of loaded files
col_summary1, col_summary2, col_summary3 = st.columns(3)

with col_summary1:
    st.metric("📁 Total Files", len(st.session_state['uploaded_files_data']))

with col_summary2:
    forma_count = sum(1 for fname, df in st.session_state['uploaded_files_data'].items() if not is_legal_info_file(df))
    st.metric("📊 Forma Reports", forma_count)

with col_summary3:
    if st.session_state.get('legal_info_file'):
        st.metric("🏢 Company Info", "✅ Loaded", delta="Enriched")
    else:
        st.metric("🏢 Company Info", "Not Loaded")

st.markdown("---")

# Create tabs for each uploaded file
file_names = list(st.session_state['uploaded_files_data'].keys())
company_mapping = st.session_state.get('company_mapping', {})

if len(file_names) == 1:
    # Single file - no tabs needed
    filename = file_names[0]
    df = st.session_state['uploaded_files_data'][filename]

    # Check if it's a legal info file
    if is_legal_info_file(df):
        render_company_profiles(df, filename, filename.replace('.', '_').replace(' ', '_'))
    else:
        render_dashboard(df, filename, filename.replace('.', '_').replace(' ', '_'), company_mapping)
else:
    # Multiple files - create tabs
    tabs = st.tabs(file_names)

    for idx, (tab, filename) in enumerate(zip(tabs, file_names)):
        with tab:
            df = st.session_state['uploaded_files_data'][filename]

            # Check if it's a legal info file
            if is_legal_info_file(df):
                render_company_profiles(df, filename, f"{idx}_{filename.replace('.', '_').replace(' ', '_')}")
            else:
                render_dashboard(df, filename, f"{idx}_{filename.replace('.', '_').replace(' ', '_')}", company_mapping)

# ==================== COMBINED ANALYSIS (when both Forma 1 & 2 are uploaded) ====================

# Check if we have both Forma 1 and Forma 2 files
forma1_files = []
forma2_files = []

for filename, df in st.session_state['uploaded_files_data'].items():
    if 'Balans aktivi bo\'yicha jami (S400)' in df.columns:
        forma1_files.append((filename, df))
    elif 'Mahsulot (tovar, ish va xizmat) larni sotishdan sof tushum (S010)' in df.columns:
        forma2_files.append((filename, df))

if len(forma1_files) > 0 and len(forma2_files) > 0:
    st.markdown("---")
    st.header("🔗 Combined Analysis (Balance Sheet + Income Statement)")

    # Select which files to combine
    col_select1, col_select2 = st.columns(2)

    with col_select1:
        selected_forma1 = st.selectbox(
            "Select Balance Sheet (Forma 1)",
            options=[f[0] for f in forma1_files],
            key="combined_forma1"
        )

    with col_select2:
        selected_forma2 = st.selectbox(
            "Select Income Statement (Forma 2)",
            options=[f[0] for f in forma2_files],
            key="combined_forma2"
        )

    if selected_forma1 and selected_forma2:
        # Get the selected dataframes
        df_forma1 = next(df for name, df in forma1_files if name == selected_forma1)
        df_forma2 = next(df for name, df in forma2_files if name == selected_forma2)

        # Merge on STIR
        if 'STIR' in df_forma1.columns and 'STIR' in df_forma2.columns:
            merged_df = pd.merge(df_forma1, df_forma2, on='STIR', suffixes=('_BS', '_IS'))

            st.success(f"✅ Successfully matched {len(merged_df)} companies across both files")

            # Calculate combined metrics
            combined_metrics = []

            for idx, row in merged_df.iterrows():
                stir = row['STIR']
                metrics = {'STIR': stir}

                # Add company name if available
                mapping_entry = None
                if company_mapping:
                    stir_key = normalize_stir_value(stir)
                    if stir_key is not None:
                        mapping_entry = company_mapping.get(stir_key)

                if mapping_entry:
                    mapping_name = clean_company_name(mapping_entry.get('short_name')) or clean_company_name(mapping_entry.get('full_name'))
                    if mapping_name:
                        metrics['Company Name'] = mapping_name
                    metrics['Charter Capital'] = mapping_entry.get('charter_capital', 0)
                    metrics['Director'] = f"{mapping_entry.get('director_surname', '')} {mapping_entry.get('director_name', '')}".strip()

                # From Balance Sheet
                total_assets = row.get('Balans aktivi bo\'yicha jami (S400)', 0)
                total_equity = row.get('I bo\'lim bo\'yicha jami (S480)', 0)

                # From Income Statement
                revenue = row.get('Mahsulot (tovar, ish va xizmat) larni sotishdan sof tushum (S010)', 0)
                net_profit = row.get('Hisobot davrining sof foydasi (zarari) (S270)', 0)

                # Calculate ROA (Return on Assets)
                if total_assets and total_assets != 0:
                    metrics['ROA (%)'] = round((net_profit / total_assets) * 100, 2)
                else:
                    metrics['ROA (%)'] = np.nan

                # Calculate ROE (Return on Equity)
                if total_equity and total_equity != 0:
                    metrics['ROE (%)'] = round((net_profit / total_equity) * 100, 2)
                else:
                    metrics['ROE (%)'] = np.nan

                # Calculate Asset Turnover
                if total_assets and total_assets != 0:
                    metrics['Asset Turnover'] = round(revenue / total_assets, 2)
                else:
                    metrics['Asset Turnover'] = np.nan

                metrics['Total Assets'] = total_assets
                metrics['Total Equity'] = total_equity
                metrics['Revenue'] = revenue
                metrics['Net Profit'] = net_profit

                combined_metrics.append(metrics)

            combined_df = pd.DataFrame(combined_metrics)

            # Display combined metrics
            col_cm1, col_cm2, col_cm3 = st.columns(3)

            with col_cm1:
                avg_roa = combined_df['ROA (%)'].mean()
                st.metric("Average ROA", f"{avg_roa:.2f}%" if not np.isnan(avg_roa) else "N/A")

            with col_cm2:
                avg_roe = combined_df['ROE (%)'].mean()
                st.metric("Average ROE", f"{avg_roe:.2f}%" if not np.isnan(avg_roe) else "N/A")

            with col_cm3:
                avg_turnover = combined_df['Asset Turnover'].mean()
                st.metric("Average Asset Turnover", f"{avg_turnover:.2f}" if not np.isnan(avg_turnover) else "N/A")

            # Visualizations
            viz_combined1, viz_combined2 = st.tabs(["ROA vs ROE", "Top Performers"])

            with viz_combined1:
                # Scatter plot: ROA vs ROE
                hover_cols = ['STIR', 'Total Assets']
                if 'Company Name' in combined_df.columns:
                    hover_cols.insert(0, 'Company Name')

                fig_scatter = px.scatter(
                    combined_df,
                    x='ROA (%)',
                    y='ROE (%)',
                    size='Revenue',
                    hover_data=hover_cols,
                    title='Return on Assets vs Return on Equity',
                    labels={'ROA (%)': 'Return on Assets (%)', 'ROE (%)': 'Return on Equity (%)'}
                )
                st.plotly_chart(fig_scatter, use_container_width=True, key="combined_scatter")

            with viz_combined2:
                # Top performers by ROE
                if 'Company Name' in combined_df.columns:
                    display_cols = ['Company Name', 'STIR', 'ROE (%)', 'ROA (%)', 'Revenue', 'Net Profit']
                else:
                    display_cols = ['STIR', 'ROE (%)', 'ROA (%)', 'Revenue', 'Net Profit']

                top_roe = combined_df.nlargest(10, 'ROE (%)')[display_cols]
                st.markdown("**🏆 Top 10 Companies by ROE**")
                st.dataframe(top_roe, use_container_width=True)

            # Full combined metrics table
            with st.expander("📋 Complete Combined Metrics", expanded=False):
                # Reorder columns to show Company Name first if available
                if 'Company Name' in combined_df.columns:
                    display_cols = ['Company Name', 'STIR'] + [col for col in combined_df.columns if col not in ['Company Name', 'STIR']]
                    st.dataframe(combined_df[display_cols], use_container_width=True, height=400)
                else:
                    st.dataframe(combined_df, use_container_width=True, height=400)

            # Download combined analysis
            st.markdown("### 💾 Download Combined Analysis")
            csv_combined = to_csv(combined_df)
            st.download_button(
                label="📥 Download Combined Metrics CSV",
                data=csv_combined,
                file_name="combined_analysis.csv",
                mime="text/csv",
                help="Download the combined analysis with ROA, ROE, and other metrics"
            )

        else:
            st.error("Cannot merge files: STIR column not found in one or both files")

# Footer
st.markdown("---")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
