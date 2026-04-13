"""
Validation utilities for the FLOTT financial dashboard platform.
Provides data validation, error handling, and data quality checks.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import streamlit as st


def validate_dataframe(df, required_columns=None, data_type_checks=None):
    """
    Validate a DataFrame for basic data quality requirements.

    Args:
        df (pd.DataFrame): DataFrame to validate
        required_columns (list): List of column names that must be present
        data_type_checks (dict): Dictionary of {column_name: expected_type}

    Returns:
        dict: Validation results with 'is_valid', 'errors', 'warnings'
    """
    validation_result = {
        'is_valid': True,
        'errors': [],
        'warnings': [],
        'info': []
    }

    # Check if DataFrame is empty
    if df is None or df.empty:
        validation_result['is_valid'] = False
        validation_result['errors'].append("DataFrame is empty or None")
        return validation_result

    # Check for required columns
    if required_columns:
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            validation_result['is_valid'] = False
            validation_result['errors'].append(f"Missing required columns: {missing_columns}")

    # Check data types
    if data_type_checks:
        for col, expected_type in data_type_checks.items():
            if col in df.columns:
                if expected_type == 'numeric':
                    non_numeric = df[col].apply(lambda x: not pd.api.types.is_numeric_dtype(type(x)) if pd.notna(x) else False).sum()
                    if non_numeric > 0:
                        validation_result['warnings'].append(f"Column '{col}' contains {non_numeric} non-numeric values")
                elif expected_type == 'datetime':
                    try:
                        pd.to_datetime(df[col], errors='coerce')
                    except Exception:
                        validation_result['warnings'].append(f"Column '{col}' contains invalid date values")

    # Check for duplicate rows
    duplicate_count = df.duplicated().sum()
    if duplicate_count > 0:
        validation_result['warnings'].append(f"Found {duplicate_count} duplicate rows")

    # Check for missing values in key columns
    if required_columns:
        for col in required_columns:
            if col in df.columns:
                missing_count = df[col].isnull().sum()
                if missing_count > 0:
                    missing_pct = (missing_count / len(df)) * 100
                    if missing_pct > 50:
                        validation_result['errors'].append(f"Column '{col}' has {missing_pct:.1f}% missing values")
                    elif missing_pct > 10:
                        validation_result['warnings'].append(f"Column '{col}' has {missing_pct:.1f}% missing values")

    # Add info about DataFrame shape
    validation_result['info'].append(f"DataFrame shape: {df.shape[0]} rows, {df.shape[1]} columns")

    return validation_result


def validate_financial_data(df, amount_col='amount', date_col='date'):
    """
    Validate financial data for common issues.

    Args:
        df (pd.DataFrame): DataFrame with financial data
        amount_col (str): Name of the amount column
        date_col (str): Name of the date column

    Returns:
        dict: Validation results
    """
    validation_result = validate_dataframe(df, [amount_col, date_col])

    if not validation_result['is_valid']:
        return validation_result

    # Check for unrealistic amounts
    if amount_col in df.columns:
        amounts = pd.to_numeric(df[amount_col], errors='coerce')

        # Check for extremely large amounts (potential data entry errors)
        large_amounts = amounts[amounts.abs() > 1000000]  # > 1 million
        if not large_amounts.empty:
            validation_result['warnings'].append(f"Found {len(large_amounts)} transactions over 1M")

        # Check for zero amounts
        zero_amounts = amounts[amounts == 0]
        if not zero_amounts.empty:
            zero_pct = (len(zero_amounts) / len(amounts)) * 100
            if zero_pct > 10:
                validation_result['warnings'].append(f"{zero_pct:.1f}% of transactions have zero amounts")

    # Check date range
    if date_col in df.columns:
        dates = pd.to_datetime(df[date_col], errors='coerce')
        valid_dates = dates.dropna()

        if not valid_dates.empty:
            date_range_days = (valid_dates.max() - valid_dates.min()).days
            validation_result['info'].append(f"Date range: {date_range_days} days ({valid_dates.min().date()} to {valid_dates.max().date()})")

            # Check for future dates
            future_dates = valid_dates[valid_dates > datetime.now()]
            if not future_dates.empty:
                validation_result['warnings'].append(f"Found {len(future_dates)} transactions with future dates")

    return validation_result


def display_validation_results(validation_result):
    """
    Display validation results using Streamlit components.

    Args:
        validation_result (dict): Results from validation functions
    """
    if not validation_result['is_valid']:
        st.error("❌ Data validation failed")
        for error in validation_result['errors']:
            st.error(f"• {error}")
        return False

    # Show warnings
    if validation_result['warnings']:
        st.warning("⚠️ Data quality warnings:")
        for warning in validation_result['warnings']:
            st.warning(f"• {warning}")

    # Show info
    if validation_result['info']:
        with st.expander("ℹ️ Data Information", expanded=False):
            for info in validation_result['info']:
                st.info(info)

    if not validation_result['warnings'] and not validation_result['errors']:
        st.success("✅ Data validation passed")

    return True


def safe_numeric_conversion(series, default_value=0):
    """
    Safely convert a pandas Series to numeric values.

    Args:
        series (pd.Series): Series to convert
        default_value: Value to use for non-convertible entries

    Returns:
        pd.Series: Converted series
    """
    try:
        return pd.to_numeric(series, errors='coerce').fillna(default_value)
    except Exception:
        return pd.Series([default_value] * len(series), index=series.index)


def safe_datetime_conversion(series):
    """
    Safely convert a pandas Series to datetime values.

    Args:
        series (pd.Series): Series to convert

    Returns:
        pd.Series: Converted series
    """
    try:
        return pd.to_datetime(series, errors='coerce')
    except Exception:
        return pd.Series([pd.NaT] * len(series), index=series.index)


def check_data_freshness(df, date_col='date', max_age_days=90):
    """
    Check if data is fresh (not too old).

    Args:
        df (pd.DataFrame): DataFrame to check
        date_col (str): Name of the date column
        max_age_days (int): Maximum age in days for data to be considered fresh

    Returns:
        dict: Freshness check results
    """
    result = {
        'is_fresh': True,
        'latest_date': None,
        'age_days': None,
        'message': ''
    }

    if df.empty or date_col not in df.columns:
        result['is_fresh'] = False
        result['message'] = "Cannot determine data freshness"
        return result

    dates = safe_datetime_conversion(df[date_col])
    valid_dates = dates.dropna()

    if valid_dates.empty:
        result['is_fresh'] = False
        result['message'] = "No valid dates found"
        return result

    latest_date = valid_dates.max()
    age_days = (datetime.now() - latest_date).days

    result['latest_date'] = latest_date
    result['age_days'] = age_days

    if age_days > max_age_days:
        result['is_fresh'] = False
        result['message'] = f"Data is {age_days} days old (latest: {latest_date.date()})"
    else:
        result['message'] = f"Data is fresh (latest: {latest_date.date()})"

    return result


def validate_inn_format(inn_series):
    """
    Validate INN (taxpayer ID) format for Uzbekistan.

    Args:
        inn_series (pd.Series): Series containing INN values

    Returns:
        dict: Validation results
    """
    result = {
        'valid_count': 0,
        'invalid_count': 0,
        'total_count': 0,
        'invalid_examples': []
    }

    if inn_series.empty:
        return result

    # Convert to string and remove NaN values
    inn_clean = inn_series.astype(str).replace('nan', '').replace('', pd.NA).dropna()
    result['total_count'] = len(inn_clean)

    for inn in inn_clean:
        # INN should be 9 digits for organizations in Uzbekistan
        if inn.isdigit() and len(inn) == 9:
            result['valid_count'] += 1
        else:
            result['invalid_count'] += 1
            if len(result['invalid_examples']) < 5:  # Keep only first 5 examples
                result['invalid_examples'].append(inn)

    return result