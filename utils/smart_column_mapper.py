"""
Smart column mapping for various bank statement formats
"""
import pandas as pd
import re
from typing import Dict, List, Tuple, Optional

def detect_bank_columns(df: pd.DataFrame) -> Dict[str, str]:
    """
    Intelligently detect bank statement columns based on content patterns and column names
    
    Args:
        df: DataFrame with bank statement data
        
    Returns:
        Dict mapping detected column types to actual column names
    """
    if df.empty:
        return {}
    
    column_mapping = {}
    
    # Define patterns for different column types
    patterns = {
        'serial_no': {
            'patterns': [r'№\s*пп', r'№\s*п/п', r'serial', r'номер\s*п'],
            'content_check': lambda col: _is_serial_column(df[col])
        },
        'document_date': {
            'patterns': [r'дата\s*документа', r'дата\s*док', r'document\s*date', r'дата'],
            'content_check': lambda col: _is_date_column(df[col])
        },
        'processing_date': {
            'patterns': [r'дата\s*обработки', r'processing\s*date', r'обработ'],
            'content_check': lambda col: _is_date_column(df[col])
        },
        'document_no': {
            'patterns': [r'№\s*док', r'document\s*no', r'номер\s*документа'],
            'content_check': lambda col: _is_document_number_column(df[col])
        },
        'account_name': {
            'patterns': [r'наименование\s*счёта', r'account\s*name', r'наименование', r'контрагент', r'организация'],
            'content_check': lambda col: _is_text_column(df[col])
        },
        'taxpayer_id': {
            'patterns': [r'инн', r'taxpayer\s*id', r'пинфл'],
            'content_check': lambda col: _is_inn_column(df[col])
        },
        'account_no': {
            'patterns': [r'№\s*счёта', r'account\s*no', r'счет'],
            'content_check': lambda col: _is_account_number_column(df[col])
        },
        'bank_code': {
            'patterns': [r'мфо', r'bank\s*code', r'банк\s*код'],
            'content_check': lambda col: _is_numeric_code_column(df[col])
        },
        'debit_turnover': {
            'patterns': [r'обороты\s*по\s*дебету', r'оборот\s*дебет', r'дебет', r'debit\s*turnover', r'debit'],
            'content_check': lambda col: _is_amount_column(df[col])
        },
        'credit_turnover': {
            'patterns': [r'обороты\s*по\s*кредиту', r'оборот\s*кредит', r'кредит', r'credit\s*turnover', r'credit'],
            'content_check': lambda col: _is_amount_column(df[col])
        },
        'amount': {
            'patterns': [r'сумма', r'amount', r'итого'],
            'content_check': lambda col: _is_amount_column(df[col])
        },
        'payment_purpose': {
            'patterns': [r'назначение\s*платежа', r'payment\s*purpose', r'назначение', r'цель\s*платежа', r'описание'],
            'content_check': lambda col: _is_text_column(df[col], min_length=10)
        }
    }
    
    # First pass: Check column names against patterns
    for col_name in df.columns:
        col_name_clean = str(col_name).lower().strip()
        col_name_clean = re.sub(r'\s+', ' ', col_name_clean)  # Normalize spaces
        
        for col_type, criteria in patterns.items():
            # Skip if we already found this column type
            if col_type in column_mapping:
                continue
                
            # Check pattern match in column name
            pattern_match = any(
                re.search(pattern, col_name_clean, re.IGNORECASE | re.UNICODE) 
                for pattern in criteria['patterns']
            )
            
            if pattern_match:
                # Verify with content check
                try:
                    if criteria['content_check'](col_name):
                        column_mapping[col_type] = col_name
                        break
                except:
                    # If content check fails, still use pattern match as fallback
                    column_mapping[col_type] = col_name
                    break
    
    # Second pass: For unnamed/generic columns, use pure content analysis
    # This handles cases where column names are "Unnamed: 1", etc.
    unnamed_columns = [col for col in df.columns if 'unnamed' in str(col).lower() or str(col).strip() == '']
    
    for col_name in unnamed_columns:
        for col_type, criteria in patterns.items():
            # Skip if we already found this column type
            if col_type in column_mapping:
                continue
                
            # Pure content-based detection for unnamed columns
            try:
                if criteria['content_check'](col_name):
                    column_mapping[col_type] = col_name
                    break
            except:
                continue
    
    # Third pass: For complex column names (like account info in header), extract dates
    for col_name in df.columns:
        if col_name not in column_mapping.values():
            # Check if this could be a date column even with a complex name
            if 'document_date' not in column_mapping:
                try:
                    if _is_date_column(df[col_name]):
                        column_mapping['document_date'] = col_name
                except:
                    continue
    
    return column_mapping

def _is_serial_column(series: pd.Series) -> bool:
    """Check if column contains serial numbers"""
    try:
        # Convert to numeric, should be mostly integers
        numeric_series = pd.to_numeric(series, errors='coerce')
        non_null_count = numeric_series.notna().sum()
        
        if non_null_count < len(series) * 0.5:  # Less than 50% are numbers
            return False
            
        # Check if it's incrementing (mostly)
        numeric_values = numeric_series.dropna().sort_values()
        if len(numeric_values) > 1:
            diffs = numeric_values.diff().dropna()
            mostly_ones = (diffs == 1).sum() / len(diffs) > 0.7
            return mostly_ones
            
        return True
    except:
        return False

def _is_date_column(series: pd.Series) -> bool:
    """Check if column contains dates"""
    try:
        # Try to convert to datetime
        date_series = pd.to_datetime(series, errors='coerce', dayfirst=True)
        non_null_count = date_series.notna().sum()
        
        # At least 30% should be valid dates
        return non_null_count >= len(series) * 0.3
    except:
        return False

def _is_document_number_column(series: pd.Series) -> bool:
    """Check if column contains document numbers"""
    try:
        # Document numbers are usually alphanumeric
        non_empty = series.dropna().astype(str)
        if len(non_empty) == 0:
            return False
        
        # Check for alphanumeric pattern
        alphanumeric_count = non_empty.str.match(r'^[a-zA-Z0-9\-_/\\]+$').sum()
        return alphanumeric_count >= len(non_empty) * 0.5
    except:
        return False

def _is_text_column(series: pd.Series, min_length: int = 3) -> bool:
    """Check if column contains text data"""
    try:
        non_empty = series.dropna().astype(str)
        if len(non_empty) == 0:
            return False
        
        # Check average length
        avg_length = non_empty.str.len().mean()
        return avg_length >= min_length
    except:
        return False

def _is_inn_column(series: pd.Series) -> bool:
    """Check if column contains INN/tax ID numbers"""
    try:
        non_empty = series.dropna().astype(str)
        if len(non_empty) == 0:
            return False
        
        # INN is typically 9-12 digits
        inn_pattern_count = non_empty.str.match(r'^\d{9,12}$').sum()
        return inn_pattern_count >= len(non_empty) * 0.3
    except:
        return False

def _is_account_number_column(series: pd.Series) -> bool:
    """Check if column contains account numbers"""
    try:
        non_empty = series.dropna().astype(str)
        if len(non_empty) == 0:
            return False
        
        # Account numbers are long numeric strings
        account_pattern_count = non_empty.str.match(r'^\d{15,25}$').sum()
        return account_pattern_count >= len(non_empty) * 0.3
    except:
        return False

def _is_numeric_code_column(series: pd.Series) -> bool:
    """Check if column contains numeric codes (like MFO)"""
    try:
        numeric_series = pd.to_numeric(series, errors='coerce')
        non_null_count = numeric_series.notna().sum()
        
        return non_null_count >= len(series) * 0.5
    except:
        return False

def _is_amount_column(series: pd.Series) -> bool:
    """Check if column contains monetary amounts"""
    try:
        # Try to convert to numeric (handling different formats)
        # Remove common currency symbols and spaces
        cleaned_series = series.astype(str).str.replace(r'[^\d.,\-]', '', regex=True)
        cleaned_series = cleaned_series.str.replace(',', '.')  # Handle comma decimal separator
        
        numeric_series = pd.to_numeric(cleaned_series, errors='coerce')
        non_null_count = numeric_series.notna().sum()
        
        if non_null_count < len(series) * 0.3:  # Less than 30% are numbers
            return False
        
        # Check if amounts are reasonable (not all zeros, some variation)
        non_zero_amounts = numeric_series[numeric_series != 0]
        if len(non_zero_amounts) == 0:
            return True  # All zeros could still be an amount column
        
        # Additional checks to exclude account numbers and other IDs
        # Account numbers are typically very long (15+ digits) and uniform in length
        str_values = series.astype(str).str.replace(r'[^\d]', '', regex=True)
        lengths = str_values.str.len()
        avg_length = lengths.mean()
        length_std = lengths.std()
        
        # If average length > 15 digits and low variation in length, likely account numbers
        if avg_length > 15 and length_std < 2:
            return False
            
        # If all values are very large (>1e15) and similar magnitude, likely IDs
        if len(non_zero_amounts) > 0:
            min_val = non_zero_amounts.min()
            max_val = non_zero_amounts.max()
            if min_val > 1e15 and (max_val / min_val) < 2:  # All very large and similar magnitude
                return False
        
        # Check for reasonable amount ranges (some variation in amounts)
        amount_std = non_zero_amounts.std()
        return amount_std > 0
    except:
        return False

def apply_smart_column_mapping(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Apply intelligent column mapping to bank statement DataFrame
    
    Args:
        df: Original DataFrame
        
    Returns:
        Tuple of (mapped_DataFrame, mapping_dict)
    """
    if df.empty:
        return df, {}
    
    # Detect columns
    detected_mapping = detect_bank_columns(df)
    
    # Create standard column names mapping
    standard_names = {
        'serial_no': 'Serial No.',
        'document_date': 'Document Date',
        'processing_date': 'Processing Date', 
        'document_no': 'Document No.',
        'account_name': 'Account Name',
        'taxpayer_id': 'Taxpayer ID (INN)',
        'account_no': 'Account No.',
        'bank_code': 'Bank Code (MFO)',
        'debit_turnover': 'Debit Turnover',
        'credit_turnover': 'Credit Turnover',
        'amount': 'Amount',
        'payment_purpose': 'Payment Purpose'
    }
    
    # Build rename mapping
    rename_mapping = {}
    for col_type, original_col in detected_mapping.items():
        if col_type in standard_names:
            rename_mapping[original_col] = standard_names[col_type]
    
    # Apply mapping
    df_mapped = df.rename(columns=rename_mapping)
    
    return df_mapped, detected_mapping