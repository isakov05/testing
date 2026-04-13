"""
Bank statement processing utilities for automatic transaction type detection and data cleaning
"""
import pandas as pd
import numpy as np
import re
from datetime import datetime

def _to_numeric_robust(series: pd.Series) -> pd.Series:
    """Convert strings like '686 600 000.00' or '1 234,56 UZS' to numbers.
    Handles NBSP, spaces, commas as decimal separators, and strips non-numeric chars.
    """
    if series is None:
        return pd.Series(dtype=float)
    s = series.astype(str)
    s = s.str.replace('\u00A0', '', regex=False)  # NBSP
    s = s.str.replace(' ', '', regex=False)        # normal spaces
    s = s.str.replace(',', '.', regex=False)       # decimal comma -> dot
    s = s.str.replace(r'[^0-9\.-]', '', regex=True)  # strip currency/text
    return pd.to_numeric(s, errors='coerce').fillna(0)

def clean_bank_statement_data(df):
    """
    Clean bank statement data by removing summary rows, empty rows, and formatting issues
    
    Args:
        df: Raw bank statement DataFrame
        
    Returns:
        DataFrame: Cleaned bank statement data
    """
    if df.empty:
        return df
    
    # Make a copy to avoid modifying original
    df_clean = df.copy()
    
    # Remove rows with "Итого" (summary rows)
    if 'Дата\nдокумента' in df_clean.columns:
        date_col = 'Дата\nдокумента'
    elif 'Document Date' in df_clean.columns:
        date_col = 'Document Date'
    else:
        # Try to find any date column
        date_col = next((col for col in df_clean.columns if 'дата' in col.lower() or 'date' in col.lower()), None)
    
    if date_col:
        # Remove summary rows (contain "Итого", "Остаток", etc.)
        summary_patterns = ['итого', 'остаток', 'сальдо', 'баланс']
        mask = df_clean[date_col].astype(str).str.lower().str.contains('|'.join(summary_patterns), na=False)
        df_clean = df_clean[~mask]
        
        # Remove rows where date column is NaN or empty
        df_clean = df_clean[df_clean[date_col].notna()]
        df_clean = df_clean[df_clean[date_col].astype(str).str.strip() != '']
    
    # Remove completely empty rows
    df_clean = df_clean.dropna(how='all')
    
    # Remove rows where both debit and credit are empty/zero
    debit_col = None
    credit_col = None
    
    for col in df_clean.columns:
        if 'дебет' in col.lower() or 'debit' in col.lower():
            debit_col = col
        elif 'кредит' in col.lower() or 'credit' in col.lower():
            credit_col = col
    
    if debit_col and credit_col:
        print(f"DEBUG: Found debit_col='{debit_col}', credit_col='{credit_col}'")
        print(f"DEBUG: Sample debit values: {df_clean[debit_col].head(3).tolist()}")
        print(f"DEBUG: Sample credit values: {df_clean[credit_col].head(3).tolist()}")
        
        # Convert to numeric robustly (handles spaces/NBSP/commas/currency), replacing NaN with 0
        df_clean[debit_col] = _to_numeric_robust(df_clean[debit_col])
        df_clean[credit_col] = _to_numeric_robust(df_clean[credit_col])
        
        print(f"DEBUG: After conversion - sample debit: {df_clean[debit_col].head(3).tolist()}")
        print(f"DEBUG: After conversion - sample credit: {df_clean[credit_col].head(3).tolist()}")
        print(f"DEBUG: Rows with non-zero amounts: {((df_clean[debit_col] != 0) | (df_clean[credit_col] != 0)).sum()}/{len(df_clean)}")
        
        # Prefer 'Amount' column if present; otherwise use debit/credit
        if 'Amount' in df_clean.columns:
            amount_vals = _to_numeric_robust(df_clean['Amount'])
        else:
            amount_vals = pd.Series(0, index=df_clean.index)

        base_keep_mask = (amount_vals != 0) | (df_clean[debit_col] != 0) | (df_clean[credit_col] != 0)
        kept_base = int(base_keep_mask.sum())
        total_rows = len(df_clean)
        print(f"DEBUG: Non-zero base keep: {kept_base}/{total_rows}")

        if total_rows > 0 and kept_base / total_rows < 0.2:
            # Fallback: keep rows with any meaningful text in payment purpose
            purpose_col = next((c for c in df_clean.columns if 'purpose' in c.lower() or 'назначение' in c.lower()), None)
            if purpose_col:
                text_mask = df_clean[purpose_col].astype(str).str.strip().ne('')
                # Also keep rows having a document number
                doc_col = next((c for c in df_clean.columns if 'document no' in c.lower() or '№' in c or 'док' in c.lower()), None)
                doc_mask = df_clean[doc_col].astype(str).str.strip().ne('') if doc_col else pd.Series(False, index=df_clean.index)
                relaxed_mask = text_mask | doc_mask | base_keep_mask
                print(f"DEBUG: Applying relaxed keep mask (purpose/doc). Kept: {int(relaxed_mask.sum())}/{total_rows}")
                df_clean = df_clean[relaxed_mask]
            else:
                df_clean = df_clean[base_keep_mask]
        else:
            df_clean = df_clean[base_keep_mask]
    
    return df_clean

def detect_transaction_type(row):
    """
    Automatically detect transaction type based on payment purpose and amount direction
    
    Args:
        row: DataFrame row containing transaction data
        
    Returns:
        str: Transaction type classification
    """
    # Find the payment purpose column
    purpose_text = ""
    for col in row.index:
        if 'purpose' in col.lower() or 'назначение' in col.lower():
            purpose_text = str(row[col]).lower()
            break
    
    # Find debit/credit amounts
    debit_amount = 0
    credit_amount = 0
    
    for col in row.index:
        if 'дебет' in col.lower() or 'debit' in col.lower():
            debit_amount = pd.to_numeric(row[col], errors='coerce') or 0
        elif 'кредит' in col.lower() or 'credit' in col.lower():
            credit_amount = pd.to_numeric(row[col], errors='coerce') or 0
    
    # Primary classification: Incoming (Credit) vs Outgoing (Debit)
    if credit_amount > 0:
        # Incoming transactions
        if any(keyword in purpose_text for keyword in [
            'оплата', 'платеж', 'поступление', 'возврат', 'возмещение'
        ]):
            return "Приход - Оплата от клиентов"
        elif any(keyword in purpose_text for keyword in [
            'заем', 'кредит', 'ссуда'
        ]):
            return "Приход - Заемные средства"
        elif any(keyword in purpose_text for keyword in [
            'взнос', 'вклад', 'депозит'
        ]):
            return "Приход - Взносы/Депозиты"
        else:
            return "Приход - Прочие поступления"
    
    elif debit_amount > 0:
        # Outgoing transactions
        if any(keyword in purpose_text for keyword in [
            'заработная плата', 'зарплата', 'оплата труда', 'заработ'
        ]):
            return "Расход - Заработная плата"
        elif any(keyword in purpose_text for keyword in [
            'подоходный налог', 'ндс', 'налог на добавленную', 'налог'
        ]):
            return "Расход - Налоги"
        elif any(keyword in purpose_text for keyword in [
            'соц.взнос', 'социальный взнос', 'пенсионный фонд', 'социальн'
        ]):
            return "Расход - Социальные взносы"
        elif any(keyword in purpose_text for keyword in [
            'комиссия', 'банковские услуги', 'обслуживание счета'
        ]):
            return "Расход - Банковские комиссии"
        elif any(keyword in purpose_text for keyword in [
            'коммунальные', 'электроэнергия', 'газ', 'вода', 'отопление'
        ]):
            return "Расход - Коммунальные услуги"
        elif any(keyword in purpose_text for keyword in [
            'аренда', 'арендная плата', 'наем'
        ]):
            return "Расход - Аренда"
        elif any(keyword in purpose_text for keyword in [
            'товар', 'материал', 'поставка', 'закуп'
        ]):
            return "Расход - Закупки/Поставки"
        elif any(keyword in purpose_text for keyword in [
            'услуги', 'консультации', 'обслуживание'
        ]):
            return "Расход - Услуги"
        elif any(keyword in purpose_text for keyword in [
            'штраф', 'пеня', 'неустойка', 'админ'
        ]):
            return "Расход - Штрафы/Пени"
        elif any(keyword in purpose_text for keyword in [
            'реклама', 'маркетинг', 'продвижение'
        ]):
            return "Расход - Реклама/Маркетинг"
        elif any(keyword in purpose_text for keyword in [
            'канцелярия', 'офис', 'хозяйственные'
        ]):
            return "Расход - Хозяйственные расходы"
        else:
            return "Расход - Прочие расходы"
    
    else:
        return "Неопределено"

def add_transaction_types(df):
    """
    Add transaction type column to bank statement DataFrame
    
    Args:
        df: Cleaned bank statement DataFrame
        
    Returns:
        DataFrame: DataFrame with added 'Transaction Type' column
    """
    if df.empty:
        return df
    
    df_processed = df.copy()
    
    # Apply transaction type detection to each row
    df_processed['Transaction Type'] = df_processed.apply(detect_transaction_type, axis=1)
    
    return df_processed

def add_amount_column(df):
    """
    Add a unified 'Amount' column that combines debit and credit amounts
    
    Args:
        df: Bank statement DataFrame
        
    Returns:
        DataFrame: DataFrame with added 'Amount' column
    """
    if df.empty:
        return df
    
    df_processed = df.copy()
    
    # Find debit and credit columns
    debit_col = None
    credit_col = None
    
    for col in df_processed.columns:
        if 'дебет' in col.lower() or 'debit' in col.lower():
            debit_col = col
        elif 'кредит' in col.lower() or 'credit' in col.lower():
            credit_col = col
    
    if debit_col and credit_col:
        # Ensure numeric format
        debit_vals = pd.to_numeric(df_processed[debit_col], errors='coerce').fillna(0)
        credit_vals = pd.to_numeric(df_processed[credit_col], errors='coerce').fillna(0)
        
        # Create amount column (positive for credits, negative for debits for accounting purposes)
        # Credits (incoming money) = positive, Debits (outgoing money) = negative
        df_processed['Amount'] = credit_vals - debit_vals
    
    return df_processed

def process_bank_statement_with_types(df):
    """
    Complete processing pipeline for bank statement:
    1. Clean data
    2. Add transaction types
    3. Add unified amount column
    
    Args:
        df: Raw bank statement DataFrame
        
    Returns:
        tuple: (processed_DataFrame, processing_summary)
    """
    if df.empty:
        return df, {"error": "Empty DataFrame"}
    
    original_rows = len(df)
    
    # Step 1: Clean data
    df_clean = clean_bank_statement_data(df)
    cleaned_rows = len(df_clean)
    
    # Step 2: Add transaction types
    df_with_types = add_transaction_types(df_clean)
    
    # Step 3: Add unified amount column
    df_processed = add_amount_column(df_with_types)
    
    # Generate processing summary
    summary = {
        "original_rows": original_rows,
        "cleaned_rows": cleaned_rows,
        "removed_rows": original_rows - cleaned_rows,
        "transaction_types": df_processed['Transaction Type'].value_counts().to_dict() if 'Transaction Type' in df_processed.columns else {},
        "date_range": None,
        "total_credit": 0,
        "total_debit": 0
    }
    
    # Add date range if possible
    date_cols = [col for col in df_processed.columns if 'date' in col.lower() or 'дата' in col.lower()]
    if date_cols:
        try:
            dates = pd.to_datetime(df_processed[date_cols[0]], errors='coerce').dropna()
            if not dates.empty:
                summary["date_range"] = (dates.min().strftime('%Y-%m-%d'), dates.max().strftime('%Y-%m-%d'))
        except:
            pass
    
    # Add totals if possible
    for col in df_processed.columns:
        if 'кредит' in col.lower() or 'credit' in col.lower():
            summary["total_credit"] = pd.to_numeric(df_processed[col], errors='coerce').sum()
        elif 'дебет' in col.lower() or 'debit' in col.lower():
            summary["total_debit"] = pd.to_numeric(df_processed[col], errors='coerce').sum()
    
    return df_processed, summary