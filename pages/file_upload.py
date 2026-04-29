import streamlit as st
import pandas as pd
from io import BytesIO
from auth.db_authenticator import protect_page
from translations import get_language_options, get_text
from utils.column_mappings import apply_column_mappings
from utils.bank_statement_processor import process_bank_statement_with_types
from utils.smart_column_mapper import apply_smart_column_mapping
from utils.bank_format_fixer import fix_complex_bank_format
from utils.session_loader import (
    store_invoices,
    store_bank_transactions,
    store_reconciliation,
    load_user_invoices,
    load_user_bank_transactions,
)

# Get language
lang = st.session_state.get('language', 'en')

# Page title
st.set_page_config(page_title="File Upload & Processing", page_icon="📁", layout="wide")

protect_page()

# Session-only mode: data lives in session_state for the duration of the browser session.


# Initialize file processing states
for file_type in ['invoices_in', 'invoices_out', 'bank_statements', 'reconciliation']:
    if f'{file_type}_uploaded' not in st.session_state:
        st.session_state[f'{file_type}_uploaded'] = []
    if f'{file_type}_processed' not in st.session_state:
        st.session_state[f'{file_type}_processed'] = None

# Main dashboard
st.title("📁 " + ("Загрузка и обработка файлов" if lang == "ru" else "File Upload & Processing"))
st.write("📄 " + ("Загрузите файлы для начала анализа" if lang == "ru" else "Upload files to begin analysis"))

# Helper function to convert DataFrame to Excel
def to_excel(df):
    """Convert DataFrame to Excel format"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

@st.cache_data
def process_reconciliation_report(file_data):
    """Process reconciliation report: extract INN and debt columns."""
    try:
        # Read with known header offset; allow CSV too
        file_name = file_data.name.lower()

        if file_name.endswith('.csv'):
            df = pd.read_csv(file_data)
        elif file_name.endswith('.xls'):
            # Handle .xls files with multiple engine attempts
            df = None
            last_error = None

            # Method 1: Try openpyxl first (sometimes works with .xls files)
            try:
                df = pd.read_excel(file_data, header=4, engine='xlrd')
            except Exception as e:
                last_error = str(e)

            if df is None:
                return None, f"Failed to read .xls file. Error: {last_error}"
        else:
            # Handle .xlsx files with flexible header detection
            df = None
            header_found = False

            # Try different header rows (4, 0, 1, 2, 3)
            for header_row in [4, 0, 1, 2, 3]:
                try:
                    test_df = pd.read_excel(file_data, header=header_row, engine='openpyxl')
                    if not test_df.empty and len(test_df.columns) > 2:
                        # Check if this looks like a reasonable header
                        col_str = ' '.join(str(col).lower() for col in test_df.columns[:5])
                        if any(keyword in col_str for keyword in ['инн', 'inn', 'долг', 'debt', 'сумма', 'amount', 'покупатель', 'customer']):
                            df = test_df
                            header_found = True
                            break
                except Exception:
                    continue

            # If no good header found, default to header=4
            if df is None:
                df = pd.read_excel(file_data, header=4, engine='openpyxl')
                st.warning("⚠️ Using default header=4, column detection might be inaccurate")

        # Check if DataFrame is empty or has insufficient rows
        if df.empty:
            return None, "File appears to be empty or header row not found"

        # Look for expected columns with flexible matching
        column_mapping = {}

        # Enhanced INN column detection
        inn_candidates = [
            'Покупатель.ИНН', 'ИНН', 'INN', 'Taxpayer ID', 'Customer INN',
            'покупатель', 'customer', 'инн', 'налоговый', 'taxpayer'
        ]

        for col in df.columns:
            col_str = str(col).strip()
            if any(candidate.lower() in col_str.lower() for candidate in inn_candidates):
                column_mapping['inn'] = col
                break

        # Enhanced debt/amount column detection with data validation
        debt_candidates = [
            'Задолженность', 'Долг', 'Сумма', 'Amount', 'Debt', 'Outstanding',
            'задолженность', 'долг', 'сумма', 'amount', 'debt', 'outstanding',
            'balance', 'баланс', 'остаток', 'сальдо'
        ]

        # Strategy 1: Look for columns that match keywords AND contain numeric data
        potential_debt_cols = []
        for col in df.columns:
            col_str = str(col).strip()

            if any(candidate.lower() in col_str.lower() for candidate in debt_candidates):
                # Validate that this column actually contains numbers
                sample_data = df[col].head(10)
                numeric_count = pd.to_numeric(sample_data, errors='coerce').notna().sum()
                text_count = sample_data.apply(lambda x: isinstance(x, str) and not str(x).replace('.','').replace(',','').replace('-','').isdigit()).sum()

                # If mostly numeric, this is a good candidate
                if numeric_count > text_count:
                    potential_debt_cols.append({
                        'col': col,
                        'col_str': col_str,
                        'numeric_count': numeric_count,
                        'text_count': text_count
                    })

        # Choose the best candidate (most numeric data)
        if potential_debt_cols:
            best_candidate = max(potential_debt_cols, key=lambda x: x['numeric_count'])
            column_mapping['debt'] = best_candidate['col']
        else:
            # Strategy 2: If no keyword match works, look for rightmost numeric column
            for col in reversed(df.columns):  # Start from rightmost columns
                col_str = str(col).strip()
                sample_data = df[col].head(10)
                numeric_count = pd.to_numeric(sample_data, errors='coerce').notna().sum()

                if numeric_count >= 5:  # At least 5 numeric values in first 10 rows
                    column_mapping['debt'] = col
                    break

        # Check if we found the required columns
        if 'inn' not in column_mapping:
            available_cols = list(df.columns)
            st.error("❌ Could not automatically detect INN column")
            return None, f"Could not find INN column. Available columns: {available_cols[:10]}"

        if 'debt' not in column_mapping:
            available_cols = list(df.columns)
            st.error("❌ Could not automatically detect debt/amount column")
            return None, f"Could not find debt/amount column. Available columns: {available_cols[:10]}"

        # Create clean DataFrame with standardized column names
        clean_df = pd.DataFrame()

        # Process INN column
        inn_col = column_mapping['inn']
        inn_data = df[inn_col]

        # Clean INN data - remove non-numeric characters and try conversion
        inn_cleaned = inn_data.astype(str).str.replace(r'[^\d]', '', regex=True)
        clean_df['Customer_INN'] = pd.to_numeric(inn_cleaned, errors='coerce')

        # Alternative: try original values if cleaning didn't work
        if clean_df['Customer_INN'].notna().sum() == 0:
            clean_df['Customer_INN'] = pd.to_numeric(inn_data, errors='coerce')

        # Process debt/amount column
        debt_col = column_mapping['debt']
        debt_data = df[debt_col]

        # Clean amount data - remove currency symbols, spaces, and commas
        debt_cleaned = debt_data.astype(str).str.replace(r'[^\d.,-]', '', regex=True)
        debt_cleaned = debt_cleaned.str.replace(',', '.', regex=False)  # Handle European decimal format
        clean_df['Outstanding_Amount'] = pd.to_numeric(debt_cleaned, errors='coerce')

        # Alternative: try original values if cleaning didn't work
        if clean_df['Outstanding_Amount'].notna().sum() == 0:
            clean_df['Outstanding_Amount'] = pd.to_numeric(debt_data, errors='coerce')

        # Add original columns for reference
        clean_df['Original_INN_Column'] = inn_data
        clean_df['Original_Amount_Column'] = debt_data

        # Remove rows with invalid data
        initial_count = len(clean_df)
        clean_df = clean_df.dropna(subset=['Customer_INN', 'Outstanding_Amount'])
        final_count = len(clean_df)

        if final_count == 0:
            st.error("❌ No valid records found after data cleaning")
            return None, "No valid records found after data cleaning"

        return clean_df, None

    except Exception as e:
        error_msg = f"Unexpected error processing reconciliation file: {str(e)}"
        st.error(f"❌ {error_msg}")
        return None, error_msg

def extract_inn_from_text(text):
    """Extract INN (taxpayer ID) from text using regex patterns"""
    if pd.isna(text) or not isinstance(text, str):
        return None

    # INN patterns: typically 9 digits for organizations, 14 for individuals
    # Look for 9-digit patterns (most common for businesses)
    import re

    # Pattern 1: 9 consecutive digits
    match = re.search(r'\b(\d{9})\b', text)
    if match:
        return int(match.group(1))

    # Pattern 2: 9 digits with spaces or other separators
    match = re.search(r'\b(\d{3}[\s\-]?\d{3}[\s\-]?\d{3})\b', text)
    if match:
        digits_only = re.sub(r'\D', '', match.group(1))
        if len(digits_only) == 9:
            return int(digits_only)

    # Pattern 3: Look for patterns after company names (common format)
    match = re.search(r'["\']?[^"\']*["\']?\s+(\d{9})', text)
    if match:
        return int(match.group(1))

    return None

def clean_numeric_value(value):
    """Clean numeric values that may have spaces, commas, or other formatting"""
    if pd.isna(value):
        return 0

    # Convert to string
    str_value = str(value).strip()

    # Remove spaces and common separators
    str_value = str_value.replace(' ', '').replace('\u00A0', '')  # Remove normal and non-breaking spaces
    str_value = str_value.replace(',', '.')  # Replace comma decimal separator with dot

    # Try to convert to numeric
    try:
        return float(str_value)
    except (ValueError, TypeError):
        return 0

def enhance_bank_statement_for_cash_flow(df):
    """
    Enhanced processing to ensure bank statements have all columns required for cash flow analysis.
    Creates the exact column names expected by ar_cash_flow_analysis.py
    """
    if df is None or df.empty:
        return df

    df_enhanced = df.copy()

    # The cash flow analysis expects these exact column names: 'inn', 'date', 'contract_number', 'amount'
    # Let's create them from the available columns

    # 1. Find and standardize INN column -> 'inn'
    inn_col = None
    for col in df_enhanced.columns:
        col_lower = str(col).lower()
        if any(keyword in col_lower for keyword in ['инн', 'taxpayer', 'inn', 'tax id']):
            inn_col = col
            break

    if inn_col:
        # Convert to numeric and create 'inn' column
        df_enhanced['inn'] = pd.to_numeric(df_enhanced[inn_col], errors='coerce')
    else:
        # Try to extract INN from account name or other text fields
        df_enhanced['inn'] = pd.NA

        # Look for account name or similar fields that might contain INN
        text_fields = []
        for col in df_enhanced.columns:
            col_lower = str(col).lower()
            if any(keyword in col_lower for keyword in ['наименование', 'account name', 'название', 'имя', 'name', 'плательщик', 'получатель']):
                text_fields.append(col)

        # Extract INNs from text fields
        for field in text_fields:
            if field in df_enhanced.columns:
                extracted_inns = df_enhanced[field].apply(extract_inn_from_text)
                # Fill in missing INNs with extracted values
                df_enhanced['inn'] = df_enhanced['inn'].fillna(extracted_inns)

    # 2. Find and standardize Date column -> 'date'
    date_col = None
    date_candidates = [
        'Document Date', 'Дата документ', 'Дата платежа', 'Date',
        'Processing Date', 'Дата обработки', 'Document_Date'
    ]

    for col in df_enhanced.columns:
        col_lower = str(col).lower()
        if any(keyword in col_lower for keyword in ['date', 'дата']):
            date_col = col
            break

    if date_col:
        # Enhanced date parsing with multiple format attempts
        # First try without dayfirst=True to handle ISO format dates correctly
        df_enhanced['date'] = pd.to_datetime(df_enhanced[date_col], errors='coerce')

        # If many dates failed to parse, try different approaches
        successful_dates = df_enhanced['date'].notna().sum()
        total_non_empty = df_enhanced[date_col].notna().sum()

        if successful_dates < total_non_empty * 0.5 and total_non_empty > 0:
            # Try alternative date parsing
            # First try dayfirst=True for DD/MM/YYYY formats
            try:
                alt_dates = pd.to_datetime(df_enhanced[date_col], errors='coerce', dayfirst=True)
                alt_successful = alt_dates.notna().sum()
                if alt_successful > successful_dates:
                    df_enhanced['date'] = alt_dates
                    successful_dates = alt_successful
            except:
                pass

            # Then try different date formats
            if successful_dates < total_non_empty * 0.5:
                date_formats = ['%d.%m.%Y', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y']
                for fmt in date_formats:
                    try:
                        alt_dates = pd.to_datetime(df_enhanced[date_col], format=fmt, errors='coerce')
                        alt_successful = alt_dates.notna().sum()
                        if alt_successful > successful_dates:
                            df_enhanced['date'] = alt_dates
                            successful_dates = alt_successful
                            break
                    except:
                        continue
    else:
        df_enhanced['date'] = pd.NaT
        st.warning("⚠️ No date column found in bank statement")

    # 3. Find and standardize Contract Number column -> 'contract_number'
    contract_col = None
    for col in df_enhanced.columns:
        col_lower = str(col).lower()
        if any(keyword in col_lower for keyword in ['договор', 'contract', 'номер']):
            contract_col = col
            break

    # 4. Find and standardize Amount column -> 'amount'
    # Prioritize the correctly processed 'Amount' column from bank_statement_processor
    if 'Amount' in df_enhanced.columns:
        df_enhanced['amount'] = df_enhanced['Amount']
    else:
        amount_col = None
        for col in df_enhanced.columns:
            col_lower = str(col).lower()
            if any(keyword in col_lower for keyword in ['amount', 'сумма', 'turnover']):
                amount_col = col
                break

        if amount_col:
            df_enhanced['amount'] = df_enhanced[amount_col].apply(clean_numeric_value)
        else:
            # Try to combine debit/credit columns with enhanced patterns
            debit_col = next((col for col in df_enhanced.columns if any(keyword in str(col).lower() for keyword in ['debit', 'дебет', 'дебету'])), None)
            credit_col = next((col for col in df_enhanced.columns if any(keyword in str(col).lower() for keyword in ['credit', 'кредит', 'кредиту'])), None)

            if debit_col and credit_col:
                debit_vals = df_enhanced[debit_col].apply(clean_numeric_value)
                credit_vals = df_enhanced[credit_col].apply(clean_numeric_value)
                # Credits positive (revenue), Debits negative (expense)
                df_enhanced['amount'] = credit_vals - debit_vals
            else:
                df_enhanced['amount'] = 0

    # 5. Also keep the original columns for display purposes
    # Try to find and standardize Account Name column
    name_col = None
    for col in df_enhanced.columns:
        col_lower = str(col).lower()
        if any(keyword in col_lower for keyword in ['наименование', 'account name', 'название', 'имя', 'name']):
            name_col = col
            break

    if name_col and 'Account Name' not in df_enhanced.columns:
        df_enhanced['Account Name'] = df_enhanced[name_col]
    elif 'Account Name' not in df_enhanced.columns:
        df_enhanced['Account Name'] = ''

    # Try to find and standardize Payment Purpose column
    purpose_col = None
    for col in df_enhanced.columns:
        col_lower = str(col).lower()
        if any(keyword in col_lower for keyword in ['назначение', 'payment purpose', 'purpose', 'цель']):
            purpose_col = col
            break

    if purpose_col and 'Payment Purpose' not in df_enhanced.columns:
        df_enhanced['Payment Purpose'] = df_enhanced[purpose_col]
    elif 'Payment Purpose' not in df_enhanced.columns:
        df_enhanced['Payment Purpose'] = ''

    # Ensure Transaction Type exists
    if 'Transaction Type' not in df_enhanced.columns:
        # Try to determine from amount columns
        if 'amount' in df_enhanced.columns:
            df_enhanced['Transaction Type'] = df_enhanced['amount'].apply(
                lambda x: 'Incoming' if pd.notna(x) and x > 0 else 'Outgoing' if pd.notna(x) and x < 0 else 'Unknown'
            )
        else:
            df_enhanced['Transaction Type'] = 'Unknown'

    # Ensure Amount exists
    if 'Amount' not in df_enhanced.columns:
        # Try to create from debit/credit columns
        debit_col = next((col for col in df_enhanced.columns if 'debit' in str(col).lower() or 'дебет' in str(col).lower()), None)
        credit_col = next((col for col in df_enhanced.columns if 'credit' in str(col).lower() or 'кредит' in str(col).lower()), None)

        if debit_col and credit_col:
            debit_vals = pd.to_numeric(df_enhanced[debit_col], errors='coerce').fillna(0)
            credit_vals = pd.to_numeric(df_enhanced[credit_col], errors='coerce').fillna(0)
            # Credits positive (revenue), Debits negative (expense)
            df_enhanced['Amount'] = credit_vals - debit_vals
        else:
            df_enhanced['Amount'] = 0

    # Debug info
    required_columns = [
        'inn', 'date', 'amount', 'Taxpayer ID (INN)',
        'Account Name', 'Payment Purpose', 'Transaction Type', 'Amount'
    ]
    for col in required_columns:
        if col not in df_enhanced.columns:
            st.warning(f"  ❌ {col}: Missing")

    return df_enhanced

def combine_dataframes(dfs_list, file_type):
    """Combine multiple DataFrames of the same type, keeping only essential columns"""
    if not dfs_list:
        return None

    if len(dfs_list) == 1:
        # Even for single files, clean up the columns
        return clean_dataframe_columns(dfs_list[0], file_type)

    try:
        # Clean each DataFrame before combining
        cleaned_dfs = [clean_dataframe_columns(df, file_type) for df in dfs_list]

        # Combine all cleaned DataFrames
        combined_df = pd.concat(cleaned_dfs, ignore_index=True)

        # Remove duplicates based on key columns
        duplicate_cols = []

        if file_type == 'bank_statements':
            # For bank statements, remove duplicates based on date, amount, and purpose
            # Prefer standardized columns when available
            if 'date' in combined_df.columns:
                duplicate_cols.append('date')
            elif 'Document Date' in combined_df.columns:
                duplicate_cols.append('Document Date')

            if 'amount' in combined_df.columns:
                duplicate_cols.append('amount')
            elif 'Amount' in combined_df.columns:
                duplicate_cols.append('Amount')

            if 'Payment Purpose' in combined_df.columns:
                duplicate_cols.append('Payment Purpose')

            if duplicate_cols:
                combined_df = combined_df.drop_duplicates(subset=duplicate_cols, keep='first')
        elif file_type in ['invoices_in', 'invoices_out']:
            # For invoices, remove duplicates based on invoice number and date
            duplicate_cols = []
            if 'Document Number' in combined_df.columns:
                duplicate_cols.append('Document Number')
            elif 'Номер документ' in combined_df.columns:
                duplicate_cols.append('Номер документ')

            if 'Document Date' in combined_df.columns:
                duplicate_cols.append('Document Date')
            elif 'Дата документ' in combined_df.columns:
                duplicate_cols.append('Дата документ')

        elif file_type in ['invoices_in', 'invoices_out']:
            # For invoices, remove duplicates based on document number and date
            if 'Document Number' in combined_df.columns:
                duplicate_cols.append('Document Number')
            elif 'Номер документ' in combined_df.columns:
                duplicate_cols.append('Номер документ')

            if 'Document Date' in combined_df.columns:
                duplicate_cols.append('Document Date')
            elif 'Дата документ' in combined_df.columns:
                duplicate_cols.append('Дата документ')
            elif 'date' in combined_df.columns:
                duplicate_cols.append('date')

        if duplicate_cols:
            combined_df = combined_df.drop_duplicates(subset=duplicate_cols, keep='first')
        # Sort by date if available
        date_cols = ['Document Date', 'Дата документ', 'Date']
        for date_col in date_cols:
            if date_col in combined_df.columns:
                try:
                    # Convert to datetime for sorting
                    combined_df[date_col] = pd.to_datetime(combined_df[date_col], errors='coerce')
                    combined_df = combined_df.sort_values(date_col).reset_index(drop=True)
                    break
                except:
                    continue

        return combined_df

    except Exception as e:
        st.error(f"❌ Error combining files: {str(e)}")
        return None

def clean_dataframe_columns(df, file_type):
    """Clean DataFrame by keeping only essential columns for analysis"""
    if df is None or df.empty:
        return df

    df_clean = df.copy()

    # Remove unnamed columns
    unnamed_cols = [col for col in df_clean.columns if 'Unnamed' in str(col)]
    if unnamed_cols:
        df_clean = df_clean.drop(columns=unnamed_cols)

    if file_type == 'bank_statements':
        # Essential columns for bank statements (keep only what's needed for cash flow analysis)
        essential_columns = [
            # Standardized columns created by enhance_bank_statement_for_cash_flow
            'date', 'inn', 'amount', 'contract_number',
            # Original date columns
            'Document Date', 'Дата документ', 'Дата платежа', 'Date',
            # INN columns
            'Taxpayer ID (INN)', 'ИНН', 'ИНН.1',
            # Name columns
            'Account Name', 'Наименование плательщика', 'Наименование получателя',
            # Purpose columns
            'Payment Purpose', 'Назначение платежа',
            # Amount columns
            'Amount', 'Сумма', 'Сумма платежа',
            # Transaction type
            'Transaction Type', 'Тип транзакции',
            # Contract columns
            'Contract Number', 'Номер договора',
            # Turnover columns
            'Debit Turnover', 'Credit Turnover',
            # Document columns
            'Document No.', '№док', 'Processing Date'
        ]

        # Keep only columns that exist in the DataFrame and are essential
        keep_columns = [col for col in df_clean.columns if col in essential_columns]

        if keep_columns:
            df_clean = df_clean[keep_columns]

    elif file_type in ['invoices_in', 'invoices_out']:
        # Essential columns for invoices
        essential_columns = [
            'date',  # Standardized date column
            'Document Date', 'Дата документ',
            'Document Number', 'Номер документ',
            'Seller (Tax ID or PINFL)', 'Продавец (ИНН или ПИНФЛ)',
            'Buyer (Tax ID or PINFL)', 'Покупатель (ИНН или ПИНФЛ)',
            'Seller (Name)', 'Продавец (наименование)',
            'Buyer (Name)', 'Покупатель (наименование)',
            'Supply Value (incl. VAT)', 'Стоимость поставки с учётом НДС',
            'Status', 'СТАТУС',
            'Contract Number', 'Договор номер',
            'Item Note (Goods/Works/Services)', 'Примечание к товару (работе, услуге)',
            'Quantity', 'Количество', 'Price', 'Цена',
            # Additional variations to ensure buyer INN column is preserved
            'Buyer (Tax ID)', 'Покупатель (ИНН)', 'ИНН покупателя'
        ]

        # Keep only columns that exist and are essential
        keep_columns = [col for col in df_clean.columns if col in essential_columns]

        if keep_columns:
            df_clean = df_clean[keep_columns]

    return df_clean

def show_invoice_bank_diagnostic():
    """Show diagnostic information to help debug invoice-bank matching issues"""
    try:
        st.info("🔍 **Diagnostic: Invoice vs Bank Statement Data Comparison**")

        # Check if we have the required data
        bank_df = st.session_state.get('bank_statements_processed')
        invoices_out_df = st.session_state.get('invoices_out_processed')
        invoices_in_df = st.session_state.get('invoices_in_processed')

        if bank_df is None:
            st.warning("❌ No bank statement data available")
            return

        # This function is for debugging - keeping minimal output
        st.info("🔍 Diagnostic data available in session state")

    except Exception as e:
        st.error(f"Error in diagnostic: {str(e)}")

# File processing functions
@st.cache_data
def process_invoice_file(file_data, file_type):
    """Process invoice files with product-level detail structure"""
    try:
        # Get file extension
        file_name = file_data.name.lower()
        
        # Read file based on extension
        if file_name.endswith('.csv'):
            # Try standard CSV reading first
            df = pd.read_csv(file_data)
            # Check if it's the new format with unnamed columns
            if df.columns.str.contains('Unnamed').any():
                file_data.seek(0)
                df = pd.read_csv(file_data, header=1)
        elif file_name.endswith(('.xlsx', '.xls')):
            # Read Excel file without header first to check structure
            df_check = pd.read_excel(file_data, header=None, nrows=5)
            file_data.seek(0)
            
            # Check if this is the new product-level format
            # The new format has headers at row 1 (index 1) and includes product-level details
            is_new_format = False
            if len(df_check) > 1:
                # Check for typical header indicators in row 1
                row1_str = ' '.join([str(cell).lower() for cell in df_check.iloc[1] if pd.notna(cell)])
                if 'вид документ' in row1_str and 'тип документ' in row1_str and 'происхождение товара' in row1_str:
                    is_new_format = True
            
            if is_new_format:
                # Process new format with product-level details
                df = process_product_level_invoice(file_data)
            else:
                # Use standard processing for old format
                df = pd.read_excel(file_data, header=0)
                # Apply invoice column mappings for old format
                df = apply_column_mappings(df, 'invoice')
        else:
            return None, "Unsupported file format"
        
        # Final validation and cleanup
        if df is not None and not df.empty:
            # Remove any completely empty rows
            df = df.dropna(how='all')
            
            # Convert numeric columns to proper types
            numeric_columns = ['Количество', 'Цена', 'Стоимость поставки', 
                              'НДС сумма', 'Стоимость поставки с учётом НДС',
                              'Ставка акциз', 'Сумма акциз']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Convert date columns
            date_columns = ['Дата документ', 'Договор дата', 'Дата отправки']
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)

            # Add standardized 'date' column from 'Дата документ'
            if 'Дата документ' in df.columns:
                df['date'] = df['Дата документ']

            return df, None
        else:
            return None, "Failed to process file - empty result"
            
    except Exception as e:
        return None, str(e)

def process_product_level_invoice(file_data):
    """Process invoice files with product-level detail structure (new format)"""
    try:
        # Read the Excel file with headers at row 1
        df_raw = pd.read_excel(file_data, header=1)
        
        # Drop the first unnamed column if it exists (usually an index column)
        if 'Unnamed: 0' in df_raw.columns:
            df_raw = df_raw.drop('Unnamed: 0', axis=1)
        
        # Define the expected columns based on the target format
        expected_columns = [
            '№', 'ID', 'Вид документ', 'ТИП документ', 'СТАТУС', 
            'Номер документ', 'Дата документ', 'Договор номер', 'Договор дата', 
            'Дата отправки', 'Продавец (ИНН или ПИНФЛ)', 'Продавец (наименование)', 
            'Продавец (код филиала)', 'Продавец (название филиала)', 
            'Покупатель (ИНН или ПИНФЛ)', 'Покупатель (наименование)', 
            'Покупатель (код филиала)', 'Покупатель (название филиала)', 
            'ПРИМЕЧАНИЕ', '№.1', 'Примечание к товару (работе, услуге)', 
            'Идентификационный код и название по Единому электронному национальному каталогу товаров (услуг)', 
            'Единица измерения', 'Маркировка', 'Количество', 'Цена', 
            'Ставка акциз', 'Сумма акциз', 'Стоимость поставки', 
            'НДС ставка', 'НДС сумма', 'Стоимость поставки с учётом НДС', 
            'Происхождение товара'
        ]
        
        # Process the data: expand invoice-level data to product-level rows
        processed_rows = []
        current_invoice = None
        
        for _, row in df_raw.iterrows():
            # Check if this is an invoice header row (has invoice-level data)
            # Invoice rows have data in the first columns (№, ID, etc.)
            if pd.notna(row.get('№')) and pd.notna(row.get('ID')):
                # This is an invoice header row
                current_invoice = {
                    '№': row.get('№'),
                    'ID': row.get('ID'),
                    'Вид документ': row.get('Вид документ'),
                    'ТИП документ': row.get('ТИП документ'),
                    'СТАТУС': row.get('СТАТУС'),
                    'Номер документ': row.get('Номер документ'),
                    'Дата документ': row.get('Дата документ'),
                    'Договор номер': row.get('Договор номер'),
                    'Договор дата': row.get('Договор дата'),
                    'Дата отправки': row.get('Дата отправки'),
                    'Продавец (ИНН или ПИНФЛ)': row.get('Продавец (ИНН или ПИНФЛ)'),
                    'Продавец (наименование)': row.get('Продавец (наименование)'),
                    'Продавец (код филиала)': row.get('Продавец (код филиала)'),
                    'Продавец (название филиала)': row.get('Продавец (название филиала)'),
                    'Покупатель (ИНН или ПИНФЛ)': row.get('Покупатель (ИНН или ПИНФЛ)'),
                    'Покупатель (наименование)': row.get('Покупатель (наименование)'),
                    'Покупатель (код филиала)': row.get('Покупатель (код филиала)'),
                    'Покупатель (название филиала)': row.get('Покупатель (название филиала)'),
                    'ПРИМЕЧАНИЕ': row.get('ПРИМЕЧАНИЕ')
                }
                
                # Check if this row also has product data (in columns 20+)
                if pd.notna(row.get('№.1')):
                    # This invoice has aggregated product data in the same row
                    product_row = current_invoice.copy()
                    product_row.update({
                        '№.1': row.get('№.1'),
                        'Примечание к товару (работе, услуге)': row.get('Примечание к товару (работе, услуге)'),
                        'Идентификационный код и название по Единому электронному национальному каталогу товаров (услуг)': row.get('Идентификационный код и название по Единому электронному национальному каталогу товаров (услуг)'),
                        'Единица измерения': row.get('Единица измерения'),
                        'Маркировка': row.get('Маркировка'),
                        'Количество': row.get('Количество'),
                        'Цена': row.get('Цена'),
                        'Ставка акциз': row.get('Ставка акциз'),
                        'Сумма акциз': row.get('Сумма акциз'),
                        'Стоимость поставки': row.get('Стоимость поставки'),
                        'НДС ставка': row.get('НДС ставка'),
                        'НДС сумма': row.get('НДС сумма'),
                        'Стоимость поставки с учётом НДС': row.get('Стоимость поставки с учётом НДС'),
                        'Происхождение товара': row.get('Происхождение товара')
                    })
                    processed_rows.append(product_row)
                    
            elif current_invoice and pd.notna(row.get('№.1')):
                # This is a product detail row for the current invoice
                product_row = current_invoice.copy()
                product_row.update({
                    '№.1': row.get('№.1'),
                    'Примечание к товару (работе, услуге)': row.get('Примечание к товару (работе, услуге)'),
                    'Идентификационный код и название по Единому электронному национальному каталогу товаров (услуг)': row.get('Идентификационный код и название по Единому электронному национальному каталогу товаров (услуг)'),
                    'Единица измерения': row.get('Единица измерения'),
                    'Маркировка': row.get('Маркировка'),
                    'Количество': row.get('Количество'),
                    'Цена': row.get('Цена'),
                    'Ставка акциз': row.get('Ставка акциз'),
                    'Сумма акциз': row.get('Сумма акциз'),
                    'Стоимость поставки': row.get('Стоимость поставки'),
                    'НДС ставка': row.get('НДС ставка'),
                    'НДС сумма': row.get('НДС сумма'),
                    'Стоимость поставки с учётом НДС': row.get('Стоимость поставки с учётом НДС'),
                    'Происхождение товара': row.get('Происхождение товара')
                })
                processed_rows.append(product_row)
        
        # Create DataFrame from processed rows
        df_processed = pd.DataFrame(processed_rows)
        
        # Remove rows where №.1 contains 'Общ.' (summary/total rows)
        if '№.1' in df_processed.columns:
            # Filter out rows where №.1 is 'Общ.' or contains 'Общ'
            df_processed = df_processed[~df_processed['№.1'].astype(str).str.contains('Общ', na=False)]
        
        # Ensure all expected columns are present
        for col in expected_columns:
            if col not in df_processed.columns:
                df_processed[col] = None
        
        # Reorder columns to match expected format
        df_processed = df_processed[expected_columns]
        
        return df_processed
        
    except Exception as e:
        st.error(f"Error processing product-level invoice: {str(e)}")
        return None

def find_header_row_in_excel(file_data):
    """Find the row that contains column headers for bank statement data"""
    try:
        # Read the entire Excel file without headers
        df_full = pd.read_excel(file_data, header=None)
        
        # Look for the row containing '№ пп' (traditional format)
        for idx, row in df_full.iterrows():
            if any('№ пп' in str(cell) for cell in row if pd.notna(cell)):
                return idx
        
        # IMPORTANT: Check for CBreport21 format specifically
        # These files have headers at row 3 or 4 with pattern: Дата, Cчет/ИНН, № док, Оп, МФО, Оборот Дебет, Оборот Кредит
        for idx in range(min(10, len(df_full))):  # Check first 10 rows
            row = df_full.iloc[idx]
            row_str = ' '.join([str(cell).lower() for cell in row if pd.notna(cell)])
            
            # Check for CBreport21 specific headers
            if ('дата' in row_str and 'счет' in row_str and 'оборот дебет' in row_str and 'оборот кредит' in row_str):
                return idx
            # Alternative check for individual cells
            if any('дата' == str(cell).strip().lower() for cell in row if pd.notna(cell)) and \
               any('оборот дебет' in str(cell).lower() for cell in row if pd.notna(cell)):
                return idx
        
        # Look for other common bank statement header indicators
        header_indicators = [
            'дата документа', 'дата обработки', 'документ', 'счет', 'инн',
            'дебет', 'кредит', 'сумма', 'назначение', 'плат', 'операц',
            'document date', 'processing date', 'account', 'debit', 'credit',
            'amount', 'purpose', 'payment'
        ]
        
        for idx, row in df_full.iterrows():
            row_str = ' '.join([str(cell).lower() for cell in row if pd.notna(cell)])
            # Check if this row contains multiple header indicators
            matches = sum(1 for indicator in header_indicators if indicator in row_str)
            if matches >= 3:  # At least 3 header-like words
                return idx
        
        # Look for rows that have many non-empty cells (likely header row)
        for idx, row in df_full.iterrows():
            non_empty_count = sum(1 for cell in row if pd.notna(cell) and str(cell).strip() != '')
            if non_empty_count >= 5:  # At least 5 non-empty cells
                # Additional check: headers usually don't contain very large numbers
                has_large_numbers = any(isinstance(cell, (int, float)) and abs(cell) > 1000000 for cell in row)
                if not has_large_numbers:
                    return idx
                
        return 0  # Fallback to first row
    except:
        return 0

def analyze_excel_structure(file_data):
    """Enhanced Excel structure analysis for complex bank statement formats"""
    try:
        # Read without header to see raw structure
        df_raw = pd.read_excel(file_data, header=None)

        analysis = {
            'total_rows': len(df_raw),
            'total_cols': len(df_raw.columns),
            'non_empty_rows': [],
            'potential_headers': [],
            'data_start_row': None,
            'best_header_row': None,
            'column_mapping': {}
        }

        # Enhanced bank statement keywords (Russian and English)
        bank_keywords = [
            'дата', 'документ', 'док', 'счет', 'счёт', 'инн', 'дебет', 'кредит',
            'сумма', 'назначение', 'операц', 'мфо', 'банк', 'наименование',
            'плательщик', 'получатель', 'оборот', 'платеж', 'номер', '№',
            'date', 'document', 'account', 'inn', 'debit', 'credit', 'amount',
            'purpose', 'payment', 'taxpayer', 'name', 'balance', 'turnover'
        ]

        # Find rows with significant data
        for idx, row in df_raw.iterrows():
            non_empty_count = sum(1 for cell in row if pd.notna(cell) and str(cell).strip() != '')
            if non_empty_count >= 3:
                # Get all non-empty content for analysis
                row_content = [str(cell).strip() for cell in row if pd.notna(cell) and str(cell).strip() != '']
                analysis['non_empty_rows'].append({
                    'row': idx,
                    'non_empty_count': non_empty_count,
                    'sample_content': row_content[:10],  # First 10 non-empty cells
                    'all_content': row_content
                })

        # Enhanced header detection
        best_score = 0
        for row_info in analysis['non_empty_rows']:
            row_idx = row_info['row']
            row_content_lower = ' '.join(row_info['all_content']).lower()

            # Count keyword matches
            keyword_matches = sum(1 for keyword in bank_keywords if keyword in row_content_lower)

            # Additional scoring criteria
            has_numbers = any(char.isdigit() for char in row_content_lower)
            has_currency_keywords = any(word in row_content_lower for word in ['сум', 'uzs', 'usd', 'eur'])
            has_inn_related = any(word in row_content_lower for word in ['инн', 'taxpayer', 'налог'])

            # Calculate score
            score = keyword_matches * 2
            if has_numbers: score -= 1  # Headers usually don't have many numbers
            if has_currency_keywords: score += 1
            if has_inn_related: score += 1
            if len(row_info['all_content']) >= 8: score += 1  # Many columns is good for header

            if score > best_score and keyword_matches >= 3:
                best_score = score
                analysis['best_header_row'] = row_idx

            if keyword_matches >= 2:
                analysis['potential_headers'].append({
                    'row': row_idx,
                    'keywords_found': keyword_matches,
                    'score': score,
                    'content': row_info['sample_content'],
                    'is_best': score == best_score and keyword_matches >= 3
                })

        # Try to create column mapping from the best header
        if analysis['best_header_row'] is not None:
            header_row = df_raw.iloc[analysis['best_header_row']]
            for col_idx, cell_value in enumerate(header_row):
                if pd.notna(cell_value) and str(cell_value).strip():
                    header_text = str(cell_value).strip()
                    analysis['column_mapping'][col_idx] = header_text

        return analysis
    except Exception as e:
        return {'error': str(e)}

@st.cache_data  
def process_bank_statement(file_data, statement_number):
    """Process bank statement files with automatic transaction type detection"""
    try:
        # Get file extension
        file_name = file_data.name.lower()
        
        # Read file based on extension with different handling for Excel
        if file_name.endswith('.csv'):
            df = pd.read_csv(file_data)
        elif file_name.endswith(('.xlsx', '.xls')):
            # For Excel files, analyze structure first
            file_data.seek(0)
            analysis = analyze_excel_structure(file_data)

            # Use the enhanced analysis to find the best header row
            if 'best_header_row' in analysis and analysis['best_header_row'] is not None:
                header_row = analysis['best_header_row']
            else:
                # Fallback to the old method
                file_data.seek(0)
                header_row = find_header_row_in_excel(file_data)

            # Reset file pointer and read from the correct header row
            file_data.seek(0)
            df = pd.read_excel(file_data, header=header_row)
            
            # Remove any completely empty rows
            df = df.dropna(how='all')

            # Enhanced column reconstruction using analysis results
            if df.columns.str.contains('Unnamed').any() and 'column_mapping' in analysis:

                # Apply column mapping from analysis
                new_columns = list(df.columns)
                for col_idx, col_name in analysis['column_mapping'].items():
                    if col_idx < len(new_columns):
                        new_columns[col_idx] = col_name

                df.columns = new_columns

            # Handle unnamed columns by trying to reconstruct from structure
            elif df.columns.str.contains('Unnamed').any():
                # Try different approaches for complex Excel files
                file_data.seek(0)
                
                # Try reading with different parameters
                try:
                    # Method 1: Read with multiple header rows
                    df_alt1 = pd.read_excel(file_data, header=[header_row, header_row+1])
                    if not df_alt1.empty and df_alt1.columns.str.contains('Unnamed').sum() < len(df.columns.str.contains('Unnamed').sum()):
                        df = df_alt1
                except:
                    pass
                
                file_data.seek(0)
                try:
                    # Method 2: Skip more rows and find better header
                    for skip_rows in [header_row+1, header_row+2, header_row+3]:
                        df_alt2 = pd.read_excel(file_data, header=skip_rows)
                        unnamed_count = df_alt2.columns.str.contains('Unnamed').sum()
                        if unnamed_count < df.columns.str.contains('Unnamed').sum():
                            df = df_alt2
                            break
                        file_data.seek(0)
                except:
                    pass
        else:
            return None, "Unsupported file format"
            
        # Try traditional column mappings first
        df_mapped = apply_column_mappings(df, 'bank_statement')
        
        # Check what columns we have after mapping
        
        # Check if we need advanced processing (format fixer)
        equals_original = df_mapped.equals(df)
        has_unnamed = df_mapped.columns.str.contains('Unnamed').any()
        has_account_inn_eng = any('account/inn' in col.lower() for col in df_mapped.columns)
        has_account_inn_rus = any('счет/инн' in col.lower() for col in df_mapped.columns)
        
        needs_format_fixing = (
            equals_original or  # Traditional mapping failed completely
            has_unnamed or  # Has unnamed columns
            has_account_inn_eng or  # Has mixed Account/INN column (case insensitive)
            has_account_inn_rus  # Has mixed account column in Russian (case insensitive)
        )
        
        if needs_format_fixing:
            # Try the robust format fixer first (simpler, more reliable)
            df_fixed = fix_complex_bank_format(df_mapped)
            
            # Check if format fixer worked better
            fixed_standard_cols = [col for col in df_fixed.columns if not col.startswith('Unnamed') and 'Cчет:' not in col]
            original_standard_cols = [col for col in df_mapped.columns if not col.startswith('Unnamed') and 'Cчет:' not in col]
            
            if len(fixed_standard_cols) > len(original_standard_cols):
                df_mapped = df_fixed
            else:
                # Fallback to smart mapping
                df_smart_mapped, detected_columns = apply_smart_column_mapping(df_mapped)
                
                if detected_columns:
                    df_mapped = df_smart_mapped

            # Ensure standard mappings are applied after fixers
            df_mapped = apply_column_mappings(df_mapped, 'bank_statement')
        
        # Apply advanced processing: cleaning + transaction type detection
        df_processed, processing_summary = process_bank_statement_with_types(df_mapped)

        # Enhanced processing for cash flow analysis compatibility
        df_processed = enhance_bank_statement_for_cash_flow(df_processed)       
        return df_processed, None
    except Exception as e:
        return None, str(e)

# File upload section
st.subheader("📁 " + ("Загрузка файлов" if lang == "ru" else "File Upload"))
st.info("💡 " + ("Теперь вы можете загружать несколько файлов одного типа - они будут автоматически объединены" if lang == "ru" else "You can now upload multiple files of the same type - they will be automatically combined"))

# Create 3 columns for file uploads (combined invoices uploader)
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 📋 " + ("Счета-фактуры" if lang == "ru" else "Invoices"))

    invoice_files = st.file_uploader(
        ("Выберите файлы счетов (тип определится автоматически)" if lang == "ru" else "Choose Invoice files (type will be auto-detected)"),
        type=['csv', 'xlsx', 'xls'],
        key="invoices_uploader",
        accept_multiple_files=True
    )

    if invoice_files:
        # Check if we already have one type uploaded - if so, new files are the opposite type
        has_invoices_in = len(st.session_state.get('invoices_in_uploaded', [])) > 0
        has_invoices_out = len(st.session_state.get('invoices_out_uploaded', [])) > 0

        if has_invoices_in and not has_invoices_out:
            # Already have invoices_in, so these must be invoices_out
            st.session_state.invoices_out_uploaded = invoice_files
            file_type_label = "📤 Исходящие (auto: opposite of existing)" if lang == "ru" else "📤 Invoices Out (auto: opposite of existing)"
            total_size = sum(len(f.getvalue()) for f in invoice_files) / (1024 * 1024)
            st.success(f"✅ {file_type_label}: {len(invoice_files)} " + ("файлов" if lang == "ru" else "files"))
            for f in invoice_files:
                file_size = len(f.getvalue()) / (1024 * 1024)
                st.caption(f"📄 {f.name} ({file_size:.2f} MB)")
        elif has_invoices_out and not has_invoices_in:
            # Already have invoices_out, so these must be invoices_in
            st.session_state.invoices_in_uploaded = invoice_files
            file_type_label = "📥 Входящие (auto: opposite of existing)" if lang == "ru" else "📥 Invoices In (auto: opposite of existing)"
            total_size = sum(len(f.getvalue()) for f in invoice_files) / (1024 * 1024)
            st.success(f"✅ {file_type_label}: {len(invoice_files)} " + ("файлов" if lang == "ru" else "files"))
            for f in invoice_files:
                file_size = len(f.getvalue()) / (1024 * 1024)
                st.caption(f"📄 {f.name} ({file_size:.2f} MB)")
        else:
            # No existing files or both exist - detect each file individually
            invoices_in_files = []
            invoices_out_files = []

            for uploaded_file in invoice_files:
                uploaded_file.seek(0)  # Reset file pointer

                try:
                    # Try different header rows to find the correct one
                    test_df = None
                    header_row = 0

                    for header_attempt in [0, 1, 2, 3, 4]:
                        try:
                            if uploaded_file.name.lower().endswith('.csv'):
                                df_temp = pd.read_csv(uploaded_file, header=header_attempt, nrows=100)
                            else:
                                df_temp = pd.read_excel(uploaded_file, header=header_attempt, nrows=100)

                            uploaded_file.seek(0)  # Reset file pointer

                            # Check if columns look valid (not all unnamed)
                            unnamed_count = sum(1 for col in df_temp.columns if 'unnamed' in str(col).lower())
                            if unnamed_count < len(df_temp.columns) * 0.5:  # Less than 50% unnamed
                                test_df = df_temp
                                header_row = header_attempt
                                break
                        except:
                            uploaded_file.seek(0)
                            continue

                    if test_df is None:
                        # Fallback to header=0 if nothing worked
                        if uploaded_file.name.lower().endswith('.csv'):
                            test_df = pd.read_csv(uploaded_file, nrows=100)
                        else:
                            test_df = pd.read_excel(uploaded_file, nrows=100)

                    uploaded_file.seek(0)  # Reset file pointer

                    # Look for seller INN column
                    seller_col = None
                    for col in test_df.columns:
                        col_lower = str(col).lower()
                        if 'продавец' in col_lower and ('инн' in col_lower or 'пинфл' in col_lower):
                            seller_col = col
                            break

                    # Determine type based on column values
                    if seller_col is not None:
                        # Check if seller INN is the same for first 100 rows
                        seller_values = test_df[seller_col].dropna().unique()

                        if len(seller_values) == 1:
                            # Same seller INN = Invoices Out (we are the seller)
                            invoices_out_files.append(uploaded_file)
                        else:
                            # Multiple seller INNs = Invoices In (we are the buyer)
                            invoices_in_files.append(uploaded_file)
                    else:
                        # Default to Invoices Out
                        invoices_out_files.append(uploaded_file)

                except Exception as e:
                    # If detection fails, default to Invoices Out
                    invoices_out_files.append(uploaded_file)

            # Update session state
            st.session_state.invoices_in_uploaded = invoices_in_files
            st.session_state.invoices_out_uploaded = invoices_out_files

            # Show summary
            if invoices_out_files:
                total_size = sum(len(f.getvalue()) for f in invoices_out_files) / (1024 * 1024)
                st.success(f"✅ 📤 Invoices Out: {len(invoices_out_files)} " + ("файлов" if lang == "ru" else "files"))
                for f in invoices_out_files:
                    file_size = len(f.getvalue()) / (1024 * 1024)
                    st.caption(f"📄 {f.name} ({file_size:.2f} MB)")

            if invoices_in_files:
                total_size = sum(len(f.getvalue()) for f in invoices_in_files) / (1024 * 1024)
                st.success(f"✅ 📥 Invoices In: {len(invoices_in_files)} " + ("файлов" if lang == "ru" else "files"))
                for f in invoices_in_files:
                    file_size = len(f.getvalue()) / (1024 * 1024)
                    st.caption(f"📄 {f.name} ({file_size:.2f} MB)")
    else:
        # Clear both when no files uploaded
        st.session_state.invoices_in_uploaded = []
        st.session_state.invoices_out_uploaded = []

with col2:
    st.markdown("### 🏦 " + ("Банковские выписки" if lang == "ru" else "Bank Statements"))
    bank_statement_files = st.file_uploader(
        ("Выберите банковские выписки" if lang == "ru" else "Choose Bank Statement files"),
        type=['csv', 'xlsx', 'xls'],
        key="bank_statements_uploader",
        accept_multiple_files=True
    )

    if bank_statement_files:
        st.session_state.bank_statements_uploaded = bank_statement_files
        total_size = sum(len(f.getvalue()) for f in bank_statement_files) / (1024 * 1024)
        st.success(f"✅ {len(bank_statement_files)} " + ("файлов" if lang == "ru" else "files"))
        for f in bank_statement_files:
            file_size = len(f.getvalue()) / (1024 * 1024)
            st.caption(f"📄 {f.name} ({file_size:.2f} MB)")

with col3:
    st.markdown("### 📑 " + ("Отчет сверки" if lang == "ru" else "Reconciliation Reports"))

    # st.markdown("##### " + ("Дебиторская задолженность (AR)" if lang == "ru" else "Accounts Receivable (AR)"))
    reconciliation_ar_file = st.file_uploader(
        ("Отчет сверки - Покупатели" if lang == "ru" else "Reconciliation - Customers"),
        type=['csv', 'xlsx', 'xls'],
        key="reconciliation_ar_uploader"
    )

    if reconciliation_ar_file is not None:
        st.session_state.reconciliation_ar_uploaded = reconciliation_ar_file
        file_size = len(reconciliation_ar_file.getvalue()) / (1024 * 1024)
        st.success(f"✅ AR: {reconciliation_ar_file.name}")
        st.caption(f"📊 {file_size:.2f} MB")
    else:
        st.session_state.reconciliation_ar_uploaded = None

    # st.markdown("##### " + ("Кредиторская задолженность (AP)" if lang == "ru" else "Accounts Payable (AP)"))
    reconciliation_ap_file = st.file_uploader(
        ("Отчет сверки - Поставщики" if lang == "ru" else "Reconciliation - Suppliers"),
        type=['csv', 'xlsx', 'xls'],
        key="reconciliation_ap_uploader"
    )

    if reconciliation_ap_file is not None:
        st.session_state.reconciliation_ap_uploaded = reconciliation_ap_file
        file_size = len(reconciliation_ap_file.getvalue()) / (1024 * 1024)
        st.success(f"✅ AP: {reconciliation_ap_file.name}")
        st.caption(f"📊 {file_size:.2f} MB")
    else:
        st.session_state.reconciliation_ap_uploaded = None

# Processing section
st.divider()
st.subheader("⚙️ " + ("Обработка данных" if lang == "ru" else "Data Processing"))

# Check which files are uploaded (with null safety)
files_uploaded = {
    'invoices_in': len(st.session_state.get('invoices_in_uploaded', []) or []) > 0,
    'invoices_out': len(st.session_state.get('invoices_out_uploaded', []) or []) > 0,
    'bank_statements': len(st.session_state.get('bank_statements_uploaded', []) or []) > 0,
    'reconciliation_ar': (1 if st.session_state.get('reconciliation_ar_uploaded', []) is not None else 0) > 0,
    'reconciliation_ap': (1 if st.session_state.get('reconciliation_ap_uploaded', []) is not None else 0) > 0
}

uploaded_count = sum(files_uploaded.values())

# Count total files (with null safety)
total_files = (
    len(st.session_state.get('invoices_in_uploaded', []) or []) +
    len(st.session_state.get('invoices_out_uploaded', []) or []) +
    len(st.session_state.get('bank_statements_uploaded', []) or []) +
    (1 if st.session_state.get('reconciliation_ar_uploaded') is not None else 0) +
    (1 if st.session_state.get('reconciliation_ap_uploaded') is not None else 0)
)

# Processing button
col_process1, col_process2, col_process3 = st.columns([1, 2, 1])
with col_process2:
    if uploaded_count > 0:
        if st.button(
            f"🚀 " + ("Обработать файлы" if lang == "ru" else "Process Files") + f" ({total_files} " + ("файлов" if lang == "ru" else "files") + f" в {uploaded_count} " + ("категориях" if lang == "ru" else "categories") + ")",
            use_container_width=True,
            type="primary"
        ):
            progress_bar = st.progress(0)
            status_text = st.empty()

            processed_categories = 0
            errors = []

            # Get current user info
            current_username = st.session_state.get('username', 'anonymous')
            current_user_id = st.session_state.get('user_id', 'anonymous')

            # Process Invoices In (multiple files)
            if files_uploaded['invoices_in']:
                status_text.text("📥 Processing Invoices In...")
                invoices_in_files = st.session_state.get('invoices_in_uploaded', []) or []

                processed_dfs = []
                for i, file in enumerate(invoices_in_files):
                    df_processed, error = process_invoice_file(file, 'invoices_in')
                    if error is None:
                        processed_dfs.append((df_processed, file.name))
                    else:
                        errors.append(f"Invoices In [{file.name}]: {error}")

                if processed_dfs:
                    total_inserted = 0
                    for df, filename in processed_dfs:
                        store_invoices(df, 'IN')
                        total_inserted += len(df)
                    processed_categories += 1
                    total_in_session = len(st.session_state.get('invoices_in_processed', pd.DataFrame()))
                    st.success(f"✅ Loaded {total_inserted} invoice(s). Total in session: {total_in_session}")

                progress_bar.progress(0.20)

            # Process Invoices Out (multiple files)
            if files_uploaded['invoices_out']:
                status_text.text("📤 Processing Invoices Out...")
                invoices_out_files = st.session_state.get('invoices_out_uploaded', []) or []

                processed_dfs = []
                for file in invoices_out_files:
                    df_processed, error = process_invoice_file(file, 'invoices_out')
                    if error is None:
                        processed_dfs.append((df_processed, file.name))
                    else:
                        errors.append(f"Invoices Out [{file.name}]: {error}")

                if processed_dfs:
                    total_inserted = 0
                    for df, filename in processed_dfs:
                        store_invoices(df, 'OUT')
                        total_inserted += len(df)
                    processed_categories += 1
                    total_in_session = len(st.session_state.get('invoices_out_processed', pd.DataFrame()))
                    st.success(f"✅ Loaded {total_inserted} invoice(s). Total in session: {total_in_session}")

                progress_bar.progress(0.40)

            # Process Bank Statements (multiple files)
            if files_uploaded['bank_statements']:
                status_text.text("🏦 Processing Bank Statements...")
                bank_statement_files = st.session_state.get('bank_statements_uploaded', []) or []

                processed_dfs = []
                for i, file in enumerate(bank_statement_files):
                    df_processed, error = process_bank_statement(file, i+1)
                    if error is None:
                        processed_dfs.append((df_processed, file.name))
                    else:
                        errors.append(f"Bank Statement [{file.name}]: {error}")

                if processed_dfs:
                    total_inserted = 0
                    for df, filename in processed_dfs:
                        store_bank_transactions(df)
                        total_inserted += len(df)
                    processed_categories += 1
                    total_in_session = len(st.session_state.get('bank_statements_processed', pd.DataFrame()))
                    st.success(f"✅ Loaded {total_inserted} transaction(s). Total in session: {total_in_session}")

                progress_bar.progress(0.60)

            # Process AR Reconciliation Report (single file)
            if files_uploaded['reconciliation_ar']:
                status_text.text("📑 Processing Reconciliation AR...")
                reconciliation_file = st.session_state.get('reconciliation_ar_uploaded', []) or []

                df_processed, error = process_reconciliation_report(reconciliation_file)
                if error is None and df_processed is not None:
                    store_reconciliation(df_processed, 'AR')
                    processed_categories += 1
                    st.success(f"✅ Loaded {len(df_processed)} reconciliation record(s).")
                else:
                    errors.append(f"Reconciliation Report [{reconciliation_file.name}]: {error}")

                progress_bar.progress(0.80)

            # Process AP Reconciliation Report (single file)
            if files_uploaded['reconciliation_ap']:
                status_text.text("📑 Processing Reconciliation AP...")
                reconciliation_file = st.session_state.get('reconciliation_ap_uploaded', []) or []

                df_processed, error = process_reconciliation_report(reconciliation_file)
                if error is None and df_processed is not None:
                    store_reconciliation(df_processed, 'AP')
                    processed_categories += 1
                    st.success(f"✅ Loaded {len(df_processed)} reconciliation record(s).")
                else:
                    errors.append(f"Reconciliation Report [{reconciliation_file.name}]: {error}")

                progress_bar.progress(1.0)

            # Clear status and show completion
            status_text.text("✅ Processing complete!")

            # Show errors if any
            if errors:
                st.error("❌ Some errors occurred during processing:")
                for error in errors:
                    st.error(f"  • {error}")
            else:
                st.success(f"✅ Successfully processed {processed_categories} data categories (stored in session)!")

# Processing status
st.divider()
st.subheader("📊 " + ("Состояние обработки" if lang == "ru" else "Processing Status"))

status_col1, status_col2, status_col3, status_col4, status_col5 = st.columns(5)

with status_col1:
    if st.session_state.get('invoices_in_processed') is not None:
        df = st.session_state.invoices_in_processed
        files_count = len(st.session_state.get('invoices_in_uploaded', []) or [])
        st.metric(
            "📥 " + ("Входящие счета" if lang == "ru" else "Invoices In"),
            f"{len(df):,} " + ("записей" if lang == "ru" else "records"),
            delta=f"✅ {files_count} " + ("файлов объединено" if lang == "ru" else "files combined")
        )
    else:
        files_count = len(st.session_state.get('invoices_in_uploaded', []) or [])
        if files_count > 0:
            st.metric("📥 " + ("Входящие счета" if lang == "ru" else "Invoices In"), f"{files_count} " + ("файлов загружено" if lang == "ru" else "files uploaded"), delta="⏳")
        else:
            st.metric("📥 " + ("Входящие счета" if lang == "ru" else "Invoices In"), "—", delta="⏳")

with status_col2:
    if st.session_state.get('invoices_out_processed') is not None:
        df = st.session_state.invoices_out_processed
        files_count = len(st.session_state.get('invoices_out_uploaded', []) or [])
        st.metric(
            "📤 " + ("Исходящие счета" if lang == "ru" else "Invoices Out"),
            f"{len(df):,} " + ("записей" if lang == "ru" else "records"),
            delta=f"✅ {files_count} " + ("файлов объединено" if lang == "ru" else "files combined")
        )
    else:
        files_count = len(st.session_state.get('invoices_out_uploaded', []) or [])
        if files_count > 0:
            st.metric("📤 " + ("Исходящие счета" if lang == "ru" else "Invoices Out"), f"{files_count} " + ("файлов загружено" if lang == "ru" else "files uploaded"), delta="⏳")
        else:
            st.metric("📤 " + ("Исходящие счета" if lang == "ru" else "Invoices Out"), "—", delta="⏳")

with status_col3:
    if st.session_state.get('bank_statements_processed') is not None:
        df = st.session_state.bank_statements_processed
        files_count = len(st.session_state.get('bank_statements_uploaded', []) or [])
        st.metric(
            "🏦 " + ("Банковские выписки" if lang == "ru" else "Bank Statements"),
            f"{len(df):,} " + ("записей" if lang == "ru" else "records"),
            delta=f"✅ {files_count} " + ("файлов объединено" if lang == "ru" else "files combined")
        )
    else:
        files_count = len(st.session_state.get('bank_statements_uploaded', []) or [])
        if files_count > 0:
            st.metric("🏦 " + ("Банковские выписки" if lang == "ru" else "Bank Statements"), f"{files_count} " + ("файлов загружено" if lang == "ru" else "files uploaded"), delta="⏳")
        else:
            st.metric("🏦 " + ("Банковские выписки" if lang == "ru" else "Bank Statements"), "—", delta="⏳")

with status_col4:
    if st.session_state.get('reconciliation_ar_processed') is not None:
        df = st.session_state.reconciliation_ar_processed
        st.metric(
            "📑 " + ("Отчет сверки" if lang == "ru" else "Reconciliation"),
            f"{len(df):,} " + ("записей" if lang == "ru" else "records"),
            delta="✅ " + ("обработан" if lang == "ru" else "processed")
        )
    else:
        files_count = 1 if st.session_state.get('reconciliation_ar_uploaded') is not None else 0
        if files_count > 0:
            st.metric("📑 " + ("Отчет сверки AR" if lang == "ru" else "Reconciliation AR"), f"{files_count} " + ("файлов загружено" if lang == "ru" else "files uploaded"), delta="⏳")
        else:
            st.metric("📑 " + ("Отчет сверки AR" if lang == "ru" else "Reconciliation AR"), "—", delta="⏳")
    

with status_col5:
    if st.session_state.get('reconciliation_ap_processed') is not None:
        df = st.session_state.reconciliation_ap_processed
        st.metric(
            "📑 " + ("Отчет сверки AP" if lang == "ru" else "Reconciliation AP"),
            f"{len(df):,} " + ("записей" if lang == "ru" else "records"),
            delta="✅ " + ("обработан" if lang == "ru" else "processed")
        )
    else:
        files_count = 1 if st.session_state.get('reconciliation_ap_uploaded') is not None else 0
        if files_count > 0:
            st.metric("📑 " + ("Отчет сверки AP" if lang == "ru" else "Reconciliation AP"), f"{files_count} " + ("файлов загружено" if lang == "ru" else "files uploaded"), delta="⏳")
        else:
            st.metric("📑 " + ("Отчет сверки AP" if lang == "ru" else "Reconciliation AP"), "—", delta="⏳")

# Check if any files have been processed
processed_any = any([
    st.session_state.get('invoices_in_processed', None) is not None,
    st.session_state.get('invoices_out_processed', None) is not None,
    st.session_state.get('bank_statements_processed', None) is not None,
    st.session_state.get('reconciliation_ar_processed', None) is not None,
    st.session_state.get('reconciliation_ap_processed', None) is not None
])

# Download processed files section
if processed_any:
    st.divider()
    st.subheader("📥 " + ("Скачать обработанные файлы" if lang == "ru" else "Download Processed Files"))
    
    # Format selection
    format_col1, format_col2, format_col3 = st.columns([1, 2, 1])
    with format_col2:
        download_format = st.radio(
            "Выберите формат" if lang == "ru" else "Select format",
            ["CSV", "Excel (.xlsx)"],
            horizontal=True,
            key="download_format"
        )
    
    st.write("")  # Add some spacing
    download_cols = st.columns(5)

    with download_cols[0]:
        if st.session_state.get('invoices_in_processed') is not None:
            df = st.session_state.invoices_in_processed
            if download_format == "CSV":
                data = df.to_csv(index=False)
                mime = "text/csv"
                extension = "csv"
            else:
                data = to_excel(df)
                mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                extension = "xlsx"

            st.download_button(
                label="📥 " + ("Входящие счета" if lang == "ru" else "Invoices In"),
                data=data,
                file_name=f"invoices_in_combined.{extension}",
                mime=mime,
                use_container_width=True
            )

    with download_cols[1]:
        if st.session_state.get('invoices_out_processed') is not None:
            df = st.session_state.invoices_out_processed
            if download_format == "CSV":
                data = df.to_csv(index=False)
                mime = "text/csv"
                extension = "csv"
            else:
                data = to_excel(df)
                mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                extension = "xlsx"

            st.download_button(
                label="📥 " + ("Исходящие счета" if lang == "ru" else "Invoices Out"),
                data=data,
                file_name=f"invoices_out_combined.{extension}",
                mime=mime,
                use_container_width=True
            )

    with download_cols[2]:
        if st.session_state.get('bank_statements_processed') is not None:
            df = st.session_state.bank_statements_processed
            if download_format == "CSV":
                data = df.to_csv(index=False)
                mime = "text/csv"
                extension = "csv"
            else:
                data = to_excel(df)
                mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                extension = "xlsx"

            st.download_button(
                label="📥 " + ("Банковские выписки" if lang == "ru" else "Bank Statements"),
                data=data,
                file_name=f"bank_statements_combined.{extension}",
                mime=mime,
                use_container_width=True
            )

    with download_cols[3]:
        if st.session_state.get('reconciliation_ar_processed') is not None:
            df = st.session_state.reconciliation_ar_processed
            if download_format == "CSV":
                data = df.to_csv(index=False)
                mime = "text/csv"
                extension = "csv"
            else:
                data = to_excel(df)
                mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                extension = "xlsx"

            st.download_button(
                label="📥 " + ("Отчет сверки AR" if lang == "ru" else "Reconciliation AR"),
                data=data,
                file_name=f"reconciliation_report.{extension}",
                mime=mime,
                use_container_width=True
            )

    with download_cols[4]:
        if st.session_state.get('reconciliation_ap_processed') is not None:
            df = st.session_state.reconciliation_ap_processed
            if download_format == "CSV":
                data = df.to_csv(index=False)
                mime = "text/csv"
                extension = "csv"
            else:
                data = to_excel(df)
                mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                extension = "xlsx"

            st.download_button(
                label="📥 " + ("Отчет сверки AP" if lang == "ru" else "Reconciliation AP"),
                data=data,
                file_name=f"reconciliation_report.{extension}",
                mime=mime,
                use_container_width=True
            )

# Upload History section
st.divider()
st.subheader("📋 " + ("История загрузок" if lang == "ru" else "Upload History"))

with st.expander("📊 " + ("Показать историю загрузок" if lang == "ru" else "Show Upload History")):
    rows = []
    for key, label in [
        ('invoices_in_processed', '📥 Invoices In'),
        ('invoices_out_processed', '📤 Invoices Out'),
        ('bank_statements_processed', '🏦 Bank Statement'),
        ('reconciliation_ar_processed', '📑 Reconciliation AR'),
        ('reconciliation_ap_processed', '📑 Reconciliation AP'),
    ]:
        df = st.session_state.get(key)
        if df is not None and not df.empty:
            rows.append({'Type': label, 'Records': len(df), 'Status': '✅ In session'})
    if rows:
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
    else:
        st.info("📭 " + ("Нет загруженных данных в сессии" if lang == "ru" else "No data uploaded in this session"))

# Data Management section
st.divider()
st.subheader("🗂️ " + ("Управление данными" if lang == "ru" else "Data Management"))

management_col1, management_col2 = st.columns(2)

with management_col1:
    st.markdown("##### " + ("Сбросить данные" if lang == "ru" else "Clear Session Data"))
    st.info("⚠️ " + ("Данные хранятся только в сессии браузера" if lang == "ru" else "Data is stored in your browser session only — upload files again after clearing."))

    if st.session_state.get('confirm_delete'):
        st.warning("⚠️ " + ("Вы уверены? Это очистит все загруженные данные!" if lang == "ru" else "Are you sure? This will clear ALL uploaded data from the session!"))
        col_yes, col_no = st.columns(2)
        with col_yes:
            if st.button("✅ " + ("Да, очистить" if lang == "ru" else "Yes, clear"), type="primary", use_container_width=True):
                file_keys = [
                    'invoices_in_uploaded', 'invoices_in_processed',
                    'invoices_out_uploaded', 'invoices_out_processed',
                    'bank_statements_uploaded', 'bank_statements_processed',
                    'reconciliation_uploaded', 'reconciliation_processed',
                    'reconciliation_ar_uploaded', 'reconciliation_ar_processed',
                    'reconciliation_ap_uploaded', 'reconciliation_ap_processed',
                    'invoice_items_processed', 'confirm_delete',
                ]
                for key in file_keys:
                    if key in st.session_state:
                        del st.session_state[key]
                st.cache_data.clear()
                st.success("✅ " + ("Данные очищены!" if lang == "ru" else "Session data cleared!"))
                st.rerun()
        with col_no:
            if st.button("❌ " + ("Отмена" if lang == "ru" else "Cancel"), use_container_width=True):
                del st.session_state.confirm_delete
                st.rerun()
    else:
        if st.button(
            "🗑️ " + ("Очистить все данные сессии" if lang == "ru" else "Clear All Session Data"),
            use_container_width=True,
            type="secondary",
        ):
            st.session_state.confirm_delete = True
            st.rerun()

with management_col2:
    pass

# Navigation info
if processed_any:
    st.divider()
    st.info("✅ " + ("Файлы обработаны! Используйте боковое меню для навигации к страницам анализа." if lang == "ru" else "Files processed! Use the sidebar menu to navigate to analysis pages."))