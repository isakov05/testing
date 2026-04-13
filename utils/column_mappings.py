"""
Utility functions for loading and applying column name mappings from dict_data/column_name_dict.json
"""
import json
import pandas as pd
import os
from typing import Dict, Optional, Union


def load_column_mappings() -> Dict:
    """
    Load column name mappings from dict_data/column_name_dict.json
    
    Returns:
        dict: Dictionary containing bank_statement_columns and invoice_columns mappings
    """
    try:
        mapping_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dict_data', 'column_name_dict.json')
        
        with open(mapping_path, 'r', encoding='utf-8') as f:
            mappings = json.load(f)
        
        return mappings
    except FileNotFoundError:
        print(f"Warning: Column mapping file not found at {mapping_path}")
        return {"bank_statement_columns": {}, "invoice_columns": {}}
    except json.JSONDecodeError as e:
        print(f"Warning: Error parsing column mapping file: {e}")
        return {"bank_statement_columns": {}, "invoice_columns": {}}


def apply_column_mappings(df: pd.DataFrame, mapping_type: str) -> pd.DataFrame:
    """
    Apply column name mappings to a DataFrame
    
    Args:
        df: DataFrame to apply mappings to
        mapping_type: Type of mapping to apply ('bank_statement' or 'invoice')
    
    Returns:
        DataFrame with renamed columns
    """
    if df is None or df.empty:
        return df
    
    mappings = load_column_mappings()
    
    # Select appropriate mapping
    if mapping_type == 'bank_statement':
        column_map = mappings.get("bank_statement_columns", {})
    elif mapping_type == 'invoice':
        column_map = mappings.get("invoice_columns", {})
    else:
        print(f"Warning: Unknown mapping type '{mapping_type}'. No mappings applied.")
        return df
    
    if not column_map:
        print(f"Warning: No column mappings found for type '{mapping_type}'")
        return df
    
    # Create a copy of the dataframe
    df_mapped = df.copy()
    
    # Apply mappings only for columns that exist in the DataFrame
    columns_to_rename = {}
    for russian_col, english_col in column_map.items():
        if russian_col in df_mapped.columns:
            columns_to_rename[russian_col] = english_col
    
    if columns_to_rename:
        df_mapped = df_mapped.rename(columns=columns_to_rename)
        print(f"Applied {len(columns_to_rename)} column mappings for {mapping_type}")
    else:
        print(f"No matching columns found for {mapping_type} mappings")
    
    return df_mapped


def read_csv_with_mappings(file_path: str, mapping_type: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """
    Read CSV file and apply column mappings
    
    Args:
        file_path: Path to CSV file
        mapping_type: Type of mapping to apply ('bank_statement' or 'invoice'), or None for no mapping
        **kwargs: Additional arguments to pass to pd.read_csv
    
    Returns:
        DataFrame with applied column mappings
    """
    # Read the CSV file
    df = pd.read_csv(file_path, **kwargs)
    
    # Apply mappings if specified
    if mapping_type:
        df = apply_column_mappings(df, mapping_type)
    
    return df


def get_mapped_column_name(original_column: str, mapping_type: str) -> str:
    """
    Get the mapped (English) column name for a given Russian column name
    
    Args:
        original_column: Russian column name
        mapping_type: Type of mapping ('bank_statement' or 'invoice')
    
    Returns:
        Mapped English column name, or original name if no mapping found
    """
    mappings = load_column_mappings()
    
    if mapping_type == 'bank_statement':
        column_map = mappings.get("bank_statement_columns", {})
    elif mapping_type == 'invoice':
        column_map = mappings.get("invoice_columns", {})
    else:
        return original_column
    
    return column_map.get(original_column, original_column)


def get_reverse_mapping(mapping_type: str) -> Dict[str, str]:
    """
    Get reverse mapping (English -> Russian) for easier lookups when working with mapped data
    
    Args:
        mapping_type: Type of mapping ('bank_statement' or 'invoice')
    
    Returns:
        Dictionary mapping English column names to Russian column names
    """
    mappings = load_column_mappings()
    
    if mapping_type == 'bank_statement':
        column_map = mappings.get("bank_statement_columns", {})
    elif mapping_type == 'invoice':
        column_map = mappings.get("invoice_columns", {})
    else:
        return {}
    
    # Reverse the mapping
    return {english: russian for russian, english in column_map.items()}