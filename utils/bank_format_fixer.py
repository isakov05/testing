"""
Simple but effective bank statement format fixer for complex Excel layouts
"""
import pandas as pd
import re
from typing import Dict, Optional

def fix_complex_bank_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix complex bank statement formats with intelligent column detection
    
    Args:
        df: Raw DataFrame from complex Excel file
        
    Returns:
        DataFrame with properly named columns
    """
    if df.empty:
        return df
    
    # Create a copy
    df_fixed = df.copy()
    
    # Step 1: Check for and split mixed columns (like account info)
    df_fixed = _split_mixed_columns(df_fixed)

    # Columns that are already standard and should not be remapped
    standard_columns_to_preserve = [
        'Document Date', 'Processing Date', 'Document No.', 'Account Name', 
        'Taxpayer ID (INN)', 'Account No.', 'Bank Code (MFO)', 'Debit Turnover', 
        'Credit Turnover', 'Payment Purpose', 'Operation Code', 'Transaction Type', 'Amount'
    ]

    # Step 2: Detect and fix column by position and content
    new_columns = {}

    for i, col_name in enumerate(df_fixed.columns):
        # Skip columns that are already mapped to standard names
        if col_name in standard_columns_to_preserve:
            continue

        # Analyze column content to determine type
        col_data = df_fixed[col_name].dropna()
        
        if len(col_data) == 0:
            continue
            
        # Convert to string for analysis
        str_data = col_data.astype(str)
        
        # Pattern 1: Date column (dates in various formats)
        if _contains_dates(str_data):
            if 'Document Date' not in new_columns.values():
                new_columns[col_name] = 'Document Date'
            else:
                new_columns[col_name] = 'Processing Date'
            continue
        
        # Pattern 2: Company/Person names (text, not too long)
        if _contains_names(str_data):
            new_columns[col_name] = 'Account Name'
            continue
        
        # Pattern 3: INN numbers (9-12 digit numbers)
        if _contains_inns(str_data):
            new_columns[col_name] = 'Taxpayer ID (INN)'
            continue
            
        # Pattern 4: Serial numbers (must precede amount checks to avoid mislabeling)
        if _contains_serial_numbers(col_data):
            new_columns[col_name] = 'Serial No.'
            continue

        # Pattern 5: Large amounts (credit - typically bigger amounts, less frequent non-zeros)
        if _contains_large_amounts(col_data) and _has_sparse_nonzeros(col_data):
            new_columns[col_name] = 'Credit Turnover'
            continue
            
        # Pattern 6: Regular amounts (debit - more frequent, smaller amounts)  
        if _contains_amounts(col_data) and not _has_sparse_nonzeros(col_data):
            new_columns[col_name] = 'Debit Turnover'
            continue
        
        # Pattern 7: Payment descriptions (long text)
        if _contains_descriptions(str_data):
            new_columns[col_name] = 'Payment Purpose'
            continue
        
    
    # Apply the new column names
    df_fixed = df_fixed.rename(columns=new_columns)

    # Handle duplicate column names by keeping only the first occurrence
    df_fixed = df_fixed.loc[:, ~df_fixed.columns.duplicated()]

    # Make sure we don't lose already-mapped columns during format fixing
    for col in df.columns:
        if col in standard_columns_to_preserve and col not in df_fixed.columns:
            df_fixed[col] = df[col]
    
    # Add Transaction Type and Amount columns if we have the necessary data
    df_fixed = _add_derived_columns(df_fixed)
    
    return df_fixed

def _contains_dates(str_data: pd.Series) -> bool:
    """Check if column contains dates"""
    try:
        # Look for date patterns
        date_patterns = [
            r'\d{1,2}[./\-]\d{1,2}[./\-]\d{2,4}',  # DD/MM/YYYY, DD-MM-YY, etc.
            r'\d{2,4}[./\-]\d{1,2}[./\-]\d{1,2}',  # YYYY/MM/DD, etc.
        ]
        
        matches = 0
        for pattern in date_patterns:
            matches += str_data.str.contains(pattern, regex=True, na=False).sum()
        
        return matches >= len(str_data) * 0.5  # At least 50% look like dates
    except:
        return False

def _contains_names(str_data: pd.Series) -> bool:
    """Check if column contains company/person names"""
    try:
        # Names typically contain letters and are not too short/long
        avg_length = str_data.str.len().mean()
        has_letters = str_data.str.contains(r'[а-яА-Я]|[a-zA-Z]', regex=True, na=False).sum()
        
        return (5 <= avg_length <= 100) and has_letters >= len(str_data) * 0.7
    except:
        return False

def _contains_inns(str_data: pd.Series) -> bool:
    """Check if column contains INN numbers"""
    try:
        # INN is 9-12 digits
        inn_matches = str_data.str.match(r'^\d{9,12}$', na=False).sum()
        return inn_matches >= len(str_data) * 0.5
    except:
        return False

def _contains_amounts(col_data: pd.Series) -> bool:
    """Check if column contains monetary amounts"""
    try:
        # Convert to numeric
        numeric_data = pd.to_numeric(col_data, errors='coerce')
        non_null_count = numeric_data.notna().sum()
        
        return non_null_count >= len(col_data) * 0.7
    except:
        return False

def _contains_large_amounts(col_data: pd.Series) -> bool:
    """Check if column contains large monetary amounts (typically credit)"""
    try:
        numeric_data = pd.to_numeric(col_data, errors='coerce')
        non_zero_amounts = numeric_data[numeric_data > 0]
        
        if len(non_zero_amounts) == 0:
            return False
            
        # Large amounts typically > 1M
        large_amounts = (non_zero_amounts > 1_000_000).sum()
        return large_amounts >= len(non_zero_amounts) * 0.3
    except:
        return False

def _has_sparse_nonzeros(col_data: pd.Series) -> bool:
    """Check if column has sparse non-zero values (typical for credit)"""
    try:
        numeric_data = pd.to_numeric(col_data, errors='coerce').fillna(0)
        non_zero_count = (numeric_data != 0).sum()
        total_count = len(numeric_data)
        
        # Credit transactions are typically less frequent than debit
        return non_zero_count <= total_count * 0.6
    except:
        return False

def _contains_descriptions(str_data: pd.Series) -> bool:
    """Check if column contains payment descriptions"""
    try:
        # Descriptions are typically long and contain Russian text
        avg_length = str_data.str.len().mean()
        has_cyrillic = str_data.str.contains(r'[а-яА-Я]', regex=True, na=False).sum()
        
        return avg_length >= 20 and has_cyrillic >= len(str_data) * 0.8
    except:
        return False

def _contains_serial_numbers(col_data: pd.Series) -> bool:
    """Check if column contains serial numbers"""
    try:
        numeric_data = pd.to_numeric(col_data, errors='coerce')
        non_null_count = numeric_data.notna().sum()
        
        if non_null_count < len(col_data) * 0.7:
            return False
            
        # Serial numbers typically increment
        sorted_values = numeric_data.dropna().sort_values()
        if len(sorted_values) > 1:
            diffs = sorted_values.diff().dropna()
            sequential = (diffs == 1).sum() / len(diffs)
            return sequential > 0.5
            
        return False
    except:
        return False

def _add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns like Amount and Transaction Type"""
    df_enhanced = df.copy()
    
    # Robust numeric parser (handles spaces, NBSP, commas, currency text)
    def _to_numeric_robust(series: pd.Series) -> pd.Series:
        s = series.astype(str)
        s = s.str.replace('\u00A0', '', regex=False)
        s = s.str.replace(' ', '', regex=False)
        s = s.str.replace(',', '.', regex=False)
        s = s.str.replace(r'[^0-9\.-]', '', regex=True)
        return pd.to_numeric(s, errors='coerce').fillna(0)
    
    # Add unified Amount column
    if 'Credit Turnover' in df.columns and 'Debit Turnover' in df.columns:
        credit_vals = _to_numeric_robust(df['Credit Turnover'])
        debit_vals = _to_numeric_robust(df['Debit Turnover'])
        df_enhanced['Amount'] = credit_vals + debit_vals
    
    # Add basic Transaction Type
    if 'Credit Turnover' in df.columns and 'Debit Turnover' in df.columns:
        def get_transaction_type(row):
            credit = _to_numeric_robust(pd.Series([row.get('Credit Turnover', 0)])).iloc[0]
            debit = _to_numeric_robust(pd.Series([row.get('Debit Turnover', 0)])).iloc[0]
            
            if credit > 0:
                return 'Приход'
            elif debit > 0:
                return 'Расход'
            else:
                return 'Неопределено'
        
        df_enhanced['Transaction Type'] = df_enhanced.apply(get_transaction_type, axis=1)
    
    return df_enhanced

def _split_mixed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect and split columns that contain mixed account information
    
    Args:
        df: DataFrame with potentially mixed columns
        
    Returns:
        DataFrame with split columns
    """
    df_split = df.copy()
    columns_to_drop = []
    new_columns = {}
    
    for col_name in df.columns:
        # Some Excel files may contain duplicate column names; selecting by name
        # can return a DataFrame instead of a Series. Normalize to Series.
        raw_col = df[col_name]
        if isinstance(raw_col, pd.DataFrame):
            series_data = raw_col.iloc[:, 0]
        else:
            series_data = raw_col

        col_data = series_data.dropna().astype(str)
        
        if len(col_data) == 0:
            continue
        
        # Check if this column contains mixed account information
        mixed_info = _detect_mixed_account_info(col_data)
        
        if mixed_info:
            # Split the mixed column
            split_data = _parse_mixed_account_column(col_data)
            
            # Add new columns
            for new_col_name, new_col_data in split_data.items():
                # Extend data to match original DataFrame length
                full_data = pd.Series(index=df.index, dtype='object')
                full_data.loc[col_data.index] = new_col_data
                # Use a unique name to avoid conflicts with existing columns
                unique_col_name = f"{new_col_name}_parsed" if new_col_name in df.columns else new_col_name
                new_columns[unique_col_name] = full_data
            
            # Mark original column for removal
            columns_to_drop.append(col_name)
    
    # Add new columns to DataFrame
    for new_col_name, new_col_data in new_columns.items():
        df_split[new_col_name] = new_col_data
    
    # Remove original mixed columns
    df_split = df_split.drop(columns=columns_to_drop)
    
    return df_split

def _detect_mixed_account_info(col_data: pd.Series) -> bool:
    """
    Check if a column contains mixed account information that needs splitting
    
    Args:
        col_data: Series with string data
        
    Returns:
        bool: True if column contains mixed account info
    """
    # Ensure we operate on a Series even if a DataFrame slipped through
    if isinstance(col_data, pd.DataFrame):
        col_series = col_data.iloc[:, 0]
    else:
        col_series = col_data

    # Look for patterns indicating mixed account information
    sample_data = col_series.head(5).tolist()
    
    mixed_patterns = 0
    
    for value in sample_data:
        value_str = str(value)
        
        # Pattern 1: Long digits followed by organization name
        # Example: "202080080069417300120445098510OO 'FARM SHIFO MARADI'"
        if re.search(r'\d{15,}[A-Z\s]+', value_str):
            mixed_patterns += 1
            continue
        
        # Pattern 2: Account number + INN + organization name all in one
        # Example: "20208000005270482001 OOO \"ARASHAN\" ИНН : 307672496"
        if re.search(r'\d{15,}.*?(ООО|OOO|ИНН|MCHJ).*?\d{9,12}', value_str):
            mixed_patterns += 1
            continue
        
        # Pattern 4: Account/INN format from CBreport21
        # Example: "20208000800694473001/304450893/ООО \"FARM SHIFO MADAD\""
        if re.search(r'\d{15,}/\d{9,12}/.+', value_str):
            mixed_patterns += 1
            continue
        
        # Pattern 3: Multiple numeric codes with text
        if re.search(r'\d{9,}\s*\d{9,}.*?[А-Я]', value_str):
            mixed_patterns += 1
            continue
    
    # If more than half of samples show mixed patterns, it's a mixed column
    return mixed_patterns >= len(sample_data) * 0.5

def _parse_mixed_account_column(col_data: pd.Series) -> Dict[str, pd.Series]:
    """
    Parse mixed account column into separate components
    
    Args:
        col_data: Series with mixed account information
        
    Returns:
        Dictionary of new column name -> Series data
    """
    parsed_data = {
        'Account No.': [],
        'Taxpayer ID (INN)': [],
        'Account Name': []
    }
    
    for value in col_data:
        value_str = str(value).strip()
        
        # Initialize extracted values
        account_no = ""
        inn = ""
        account_name = ""
        
        # Pattern 4: CBreport21 format "20208000800694473001/304450893/ООО \"FARM SHIFO MADAD\"" (check this first!)
        if '/' in value_str and len(value_str.split('/')) >= 3:
            parts = value_str.split('/')
            
            # First part should be account number (long numeric)
            if len(parts[0]) >= 15 and parts[0].isdigit():
                account_no = parts[0].strip()
                
                # Second part should be INN (9-12 digits)  
                if len(parts[1]) >= 9 and parts[1].isdigit():
                    inn = parts[1].strip()
                    
                    # Everything after second slash is account name
                    account_name = '/'.join(parts[2:]).strip()
                    account_name = re.sub(r'^/+', '', account_name)  # Remove leading slashes
                    account_name = re.sub(r'["\']', '', account_name)  # Remove quotes
                    account_name = account_name.strip()
            
            # Fallback for cases that don't match expected pattern
            if not account_no and not inn:
                account_no = parts[0].strip() if len(parts) > 0 else ""
                if len(parts) > 1:
                    # Try to find INN in remaining parts
                    for part in parts[1:]:
                        if part.isdigit() and 9 <= len(part) <= 12:
                            inn = part.strip()
                            break
        
        # Pattern 1: "202080080069417300120445098510OO 'FARM SHIFO MARADI'" (only if no slashes)
        elif not '/' in value_str:
            match1 = re.match(r'(\d{20,})(.+)', value_str)
            if match1:
                numbers_part = match1.group(1)
                text_part = match1.group(2).strip()
                
                # Try to split the numbers part into account and INN
                if len(numbers_part) >= 29:  # Long enough for both account and INN
                    account_no = numbers_part[:20]  # First 20 digits for account
                    inn = numbers_part[20:29]       # Next 9 digits for INN
                elif len(numbers_part) >= 24:  # Account + partial INN
                    account_no = numbers_part[:20]
                    remaining = numbers_part[20:]
                    # Extract INN from remaining digits + text
                    inn_match = re.search(r'(\d{9,12})', remaining + text_part)
                    if inn_match:
                        inn = inn_match.group(1)
                else:
                    account_no = numbers_part
                
                # Clean up organization name from text part
                clean_text = re.sub(r'^\d+', '', text_part)  # Remove leading digits
                clean_text = re.sub(r'["\']', '', clean_text)  # Remove quotes
                account_name = clean_text.strip()
        
        # Pattern 2: "Cчет: 20208000005270482001 OOO \"ARASHAN\" ИНН : 307672496"
        elif 'счет' in value_str.lower() or 'cчет' in value_str.lower():
            # Extract account number
            account_match = re.search(r'(?:счет|cчет):\s*(\d{15,25})', value_str, re.IGNORECASE)
            if account_match:
                account_no = account_match.group(1)
            
            # Extract INN
            inn_match = re.search(r'инн\s*:?\s*(\d{9,12})', value_str, re.IGNORECASE)
            if inn_match:
                inn = inn_match.group(1)
            
            # Extract organization name (between account and INN)
            name_match = re.search(r'\d{15,25}\s+(.*?)\s+инн', value_str, re.IGNORECASE)
            if name_match:
                account_name = name_match.group(1).strip()
            elif inn_match:
                # If no clear name section, extract everything before INN
                before_inn = value_str[:inn_match.start()].strip()
                name_start = re.search(r'\d{15,25}\s+', before_inn)
                if name_start:
                    account_name = before_inn[name_start.end():].strip()
        
        # Pattern 3: Multiple numbers with organization name (fallback)
        else:
            # Extract all number sequences
            numbers = re.findall(r'\d{9,}', value_str)
            text_parts = re.split(r'\d{9,}', value_str)
            text_parts = [part.strip() for part in text_parts if part.strip()]
            
            if numbers:
                # Longest number is likely account number
                numbers.sort(key=len, reverse=True)
                if len(numbers[0]) >= 15:
                    account_no = numbers[0]
                
                # Look for INN (9-12 digits)
                for num in numbers[1:]:
                    if 9 <= len(num) <= 12:
                        inn = num
                        break
            
            if text_parts:
                # Clean up organization name
                account_name = ' '.join(text_parts)
                account_name = re.sub(r'["\']', '', account_name)  # Remove quotes
                account_name = account_name.strip()
        
        # Clean up extracted values
        parsed_data['Account No.'].append(account_no.strip() if account_no else "")
        parsed_data['Taxpayer ID (INN)'].append(inn.strip() if inn else "")
        parsed_data['Account Name'].append(account_name.strip() if account_name else "")
    
    # Convert to Series
    result = {}
    for col_name, data_list in parsed_data.items():
        result[col_name] = pd.Series(data_list, index=col_data.index)
    
    return result