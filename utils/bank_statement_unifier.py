"""
Utilities to unify diverse bank statements into a single target template.

This module is isolated from existing logic. It:
- Reads multiple input files (xlsx/xls/csv)
- Detects bank type heuristically from filename (optional)
- Applies smart column detection and complex layout fixes
- Aligns the result to the provided template's column set
- Combines all normalized DataFrames
"""

from __future__ import annotations

import io
import os
from typing import Dict, List, Optional, Sequence, Tuple
import re

import pandas as pd

from .smart_column_mapper import apply_smart_column_mapping
from .bank_format_fixer import fix_complex_bank_format


# ----------------------------- File Reading -----------------------------

def read_any_bank_file(path_or_buffer, filename: str) -> pd.DataFrame:
    """
    Read a bank statement file in xlsx/xls/csv format into a DataFrame.

    - xlsx: openpyxl engine
    - xls: xlrd engine
    - csv: auto-detect encoding; fallback to cp1251
    """
    name_lower = (filename or "").lower()

    # Always try to preserve the raw header rows so we can find the '№' header line
    # that marks the start of the data table
    if name_lower.endswith(".xlsx"):
        # Try engine-less first (lets pandas pick), then openpyxl
        for kwargs in (
            {"header": None},
            {"engine": "openpyxl", "header": None},
        ):
            try:
                return pd.read_excel(path_or_buffer, **kwargs)
            except Exception:
                continue
        return pd.DataFrame()

    if name_lower.endswith(".xls"):
        # Legacy xls; xlrd is typical engine
        for kwargs in (
            {"engine": "xlrd", "header": None},
            {"header": None},
        ):
            try:
                return pd.read_excel(path_or_buffer, **kwargs)
            except Exception:
                continue
        return pd.DataFrame()

    if name_lower.endswith(".csv"):
        # CSV: keep header=None so trim can set proper headers later
        for kwargs in (
            {"header": None},
            {"encoding": "utf-8", "header": None},
            {"encoding": "cp1251", "sep": ",", "header": None},
        ):
            try:
                return pd.read_csv(path_or_buffer, **kwargs)
            except Exception:
                continue
        return pd.DataFrame()

    # Unknown extension: attempt excel then csv as generic fallbacks
    for reader, kwargs in (
        (pd.read_excel, {"header": None}),
        (pd.read_csv, {"header": None}),
    ):
        try:
            return reader(path_or_buffer, **kwargs)
        except Exception:
            continue
    return pd.DataFrame()


# ----------------------------- Table Trimming -----------------------------

def trim_to_numbered_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Many bank statements include preamble/footer rows. This trims to the core table:
    - Find the header row that contains a cell starting with "№"
    - Set that row as column headers
    - Keep only rows where the serial column (header contains "№") has numeric values
    """
    if df is None or df.empty:
        return df

    df_str = df.astype(str)

    # 1) Locate header row containing a cell that begins with serial marker OR contains typical bank column headers
    header_row_idx = None
    header_markers = ['№', 'No', 'Nº', 'N°', '#']
    column_keywords = ['дата документа', 'наименование', 'дебет', 'кредит', 'назначение платежа', 'оборот']
    
    # Track best candidate
    best_idx = None
    best_score = 0
    
    for idx in range(len(df_str)):
        row = df_str.iloc[idx]
        row_norm = row.astype(str).str.replace('\n', ' ', regex=False).str.strip()
        
        # Calculate a score for this row being a header
        score = 0
        
        # Check for serial markers at the beginning (high confidence)
        if row_norm.apply(lambda v: any(str(v).startswith(m + ' пп') or str(v) == m for m in header_markers)).any():
            score += 10
        elif row_norm.apply(lambda v: any(str(v).startswith(m) for m in header_markers)).any():
            score += 5
        
        # Check for typical bank column headers
        row_text_combined = ' '.join(row_norm.astype(str).str.lower())
        keyword_count = sum(1 for keyword in column_keywords if keyword in row_text_combined)
        score += keyword_count * 3
        
        # Strong indicator: has both debit and credit keywords
        if 'дебет' in row_text_combined and 'кредит' in row_text_combined:
            score += 10
            
        # Update best candidate
        if score > best_score:
            best_score = score
            best_idx = idx
    
    # Use the best candidate if score is high enough
    if best_score >= 8:
        header_row_idx = best_idx
    else:
        # Fallback to original simple check
        for idx in range(len(df_str)):
            row = df_str.iloc[idx]
            row_norm = row.astype(str).str.replace('\n', ' ', regex=False).str.strip()
            if row_norm.apply(lambda v: any(str(v).startswith(m) for m in header_markers)).any():
                header_row_idx = idx
                break

    if header_row_idx is None:
        return df

    # 2) Build column names from the header row, with optional merge from the next row
    header_row = (
        df_str.iloc[header_row_idx]
        .astype(str)
        .str.replace('\n', ' ', regex=False)
        .str.strip()
    )

    # Detect if the next row is part of a split header (e.g., "Обороты по" / "дебету")
    next_is_header = False
    next_row = None
    if header_row_idx + 1 < len(df_str):
        next_row_candidate = (
            df_str.iloc[header_row_idx + 1]
            .astype(str)
            .str.replace('\n', ' ', regex=False)
            .str.strip()
        )
        header_keywords = ['дебет', 'кредит', 'назначение', 'инн', 'мфо', 'счет', 'счёт', 'док']
        
        # Check if next row contains header keywords AND is not a data row
        contains_keywords = next_row_candidate.str.lower().apply(lambda v: any(k in str(v).lower() for k in header_keywords)).any()
        
        # Additional check: make sure it's not a data row (data rows start with numbers)
        first_cell = str(next_row_candidate.iloc[0]).strip()
        is_data_row = first_cell.isdigit() or (first_cell.replace('.', '').replace(',', '').isdigit())
        
        if contains_keywords and not is_data_row:
            next_is_header = True
            next_row = next_row_candidate

    new_cols: List[str] = []
    for i in range(len(header_row)):
        top = str(header_row.iloc[i]) if i < len(header_row) else ''
        bot = str(next_row.iloc[i]) if (next_is_header and next_row is not None and i < len(next_row)) else ''
        top_clean = top.strip()
        bot_clean = bot.strip()

        # Combine typical split headers like "Обороты по" + "дебету"/"кредиту"
        if top_clean and any(word in top_clean.lower() for word in ['оборот', 'обороты']) and bot_clean:
            combined = f"{top_clean} {bot_clean}".strip()
        elif top_clean and top_clean.lower() not in ['nan', 'none', '']:
            combined = top_clean
        elif bot_clean and bot_clean.lower() not in ['nan', 'none', '']:
            combined = bot_clean
        else:
            combined = f"col_{i}"

        new_cols.append(combined)

    # Drop rows above header rows (1 row if single header, 2 rows if merged header detected)
    start_idx = header_row_idx + (2 if next_is_header else 1)
    
    df_core = df.iloc[start_idx:].copy()
    df_core.columns = new_cols

    # 3) Identify the serial column (header contains a serial marker)
    serial_cols = [c for c in df_core.columns if ('№' in str(c)) or str(c).strip().startswith(('No', 'Nº', 'N°', '#'))]
    if not serial_cols:
        return df_core

    serial_col = serial_cols[0]

    # 4) Keep only rows where serial is numeric (drop footer summaries and blanks)
    serial_numeric = pd.to_numeric(
        df_core[serial_col].astype(str).str.replace(r'[^0-9]', '', regex=True),
        errors='coerce'
    )
    keep_mask = serial_numeric.notna()
    
    
    trimmed = df_core.loc[keep_mask].copy()
    
    # 5) Additional filtering: Remove total/summary rows
    # Check for total keywords in all text columns
    total_keywords = ['итого', 'всего', 'total', 'баланс', 'остаток', 'оборот за период', 'summary']
    
    def is_not_total_row(row):
        """Check if row is NOT a total/summary row"""
        row_text = ' '.join(str(val).lower() for val in row.values if pd.notna(val))
        # If row contains total keywords but no proper transaction data, filter it out
        if any(keyword in row_text for keyword in total_keywords):
            # Check if it has a valid account number or other transaction identifiers
            # If it's just a summary row, it won't have these
            has_valid_data = False
            for val in row.values:
                val_str = str(val).strip()
                # Check for account numbers (typically 20 digits) or transaction IDs
                if val_str and val_str not in ['nan', 'None', ''] and len(val_str) >= 15 and val_str.replace('.', '').replace(',', '').isdigit():
                    has_valid_data = True
                    break
            return has_valid_data
        return True
    
    # Apply the filter
    if not trimmed.empty:
        # Apply the total row filtering
        
        trimmed = trimmed[trimmed.apply(is_not_total_row, axis=1)].reset_index(drop=True)

    return trimmed


# ----------------------------- Bank Detection -----------------------------

def detect_bank_from_filename(filename: str) -> str:
    """
    Heuristic bank detection by filename keywords (purely informational).
    """
    name = (filename or "").lower()
    if any(k in name for k in ["алока", "aloka"]):
        return "Aloka Bank"
    if any(k in name for k in ["ипак", "ipak", "yuli"]):
        return "Ipak Yuli"
    if any(k in name for k in ["капитал", "kapital"]):
        return "Kapital Bank"
    if any(k in name for k in ["выписка", "kazakhstan", "kz"]):
        return "Kazakhstan"
    if any(k in name for k in ["янги", "yangi"]):
        return "Yangi Bank"
    return "Unknown"


# ----------------------------- Template Handling -----------------------------

def load_template_columns(template_path: str) -> List[str]:
    """
    Load the template and return the list of expected column names.
    Strategy:
    - Attempt to read as Excel and use header row columns directly
    - Drop fully unnamed columns
    - Strip/normalize whitespace
    """
    try:
        df = pd.read_excel(template_path, engine="openpyxl")
    except Exception:
        try:
            df = pd.read_excel(template_path)
        except Exception:
            return []

    cols: List[str] = []
    for c in df.columns.tolist():
        cname = str(c).strip()
        if cname and not cname.lower().startswith("unnamed"):
            cols.append(cname)
    return cols


def _normalize_name(name: str) -> str:
    return " ".join(str(name).strip().lower().replace("\n", " ").split())


def build_standard_to_template_map(template_cols: Sequence[str]) -> Dict[str, str]:
    """
    Map our standard names to closest template column names by fuzzy-ish normalization.
    If no close match, keep the standard name (it will later be created and left empty).
    """
    standard_names = {
        "Serial No.": ["serial", "№", "номер"],
        "Document Date": ["document date", "дата документа", "дата"],
        "Processing Date": ["processing date", "дата обработки"],
        "Document No.": ["document no", "№ док", "номер документа"],
        "Account Name": ["account name", "наименование", "контрагент", "организация"],
        "Taxpayer ID (INN)": ["inn", "taxpayer id", "пинфл", "инн"],
        "Account No.": ["account", "счет", "счёт"],
        "Bank Code (MFO)": ["mfo", "bank code", "мфо"],
        "Debit Turnover": ["debit", "оборот по дебету", "дебет", "Обороты по дебету"],
        "Credit Turnover": ["credit", "оборот по кредиту", "кредит", "Обороты по кредиту"],
        "Payment Purpose": ["purpose", "назначение", "описание"],
        "Operation Code": ["operation code", "код операции"],
        "Transaction Type": ["transaction type", "тип транзакции"],
        "Amount": ["amount", "сумма", "итого"],
    }

    normalized_template = { _normalize_name(c): c for c in template_cols }
    mapping: Dict[str, str] = {}

    for std, keys in standard_names.items():
        chosen = None
        for k in keys + [std]:
            k_norm = _normalize_name(k)
            # direct or contains match
            for t_norm, t_orig in normalized_template.items():
                if k_norm == t_norm or k_norm in t_norm or t_norm in k_norm:
                    chosen = t_orig
                    break
            if chosen:
                break
        mapping[std] = chosen or std
    return mapping


# ----------------------------- Normalization Pipeline -----------------------------

def normalize_to_standard(df: pd.DataFrame) -> pd.DataFrame:
    """
    Use existing smart/format-fixer utilities to map columns to our standard names.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # Trim to the numbered table if present (e.g., columns start with '№')
    df = trim_to_numbered_table(df)

    # Check if the data already has proper column names (not generic like 0, 1, 2...)
    has_proper_names = any(isinstance(col, str) and len(str(col)) > 3 and not str(col).isdigit() for col in df.columns)
    
    if has_proper_names:
        # If columns already have proper names, skip complex format fixing and go straight to smart mapping
        mapped, _ = apply_smart_column_mapping(df)
    else:
        # For files with generic column names, apply complex format fixing first
        fixed = fix_complex_bank_format(df)
        mapped, _ = apply_smart_column_mapping(fixed)
    
    return mapped


def align_to_template_columns(df_standard: pd.DataFrame, template_cols: Sequence[str]) -> pd.DataFrame:
    """
    Align a standardized DataFrame to the exact template column order and names.
    - Create missing columns as empty strings
    - Reorder columns to match template
    - Keep extra columns at the end for traceability
    """
    if df_standard is None or df_standard.empty:
        # Return empty frame with template columns
        aligned = pd.DataFrame(columns=list(template_cols))
        return aligned

    # Build map from our standard names to template names to preserve naming
    std_to_tpl = build_standard_to_template_map(template_cols)

    # Rename any columns that match standard names into their template counterparts
    rename_map: Dict[str, str] = {}
    for col in df_standard.columns:
        col_norm = _normalize_name(col)
        for std, tpl in std_to_tpl.items():
            if col_norm == _normalize_name(std):
                rename_map[col] = tpl
                break

    df_aligned = df_standard.rename(columns=rename_map).copy()

    # Ensure all template columns exist
    for c in template_cols:
        if c not in df_aligned.columns:
            df_aligned[c] = ""

    # Reorder
    front_cols = [c for c in template_cols]
    extra_cols = [c for c in df_aligned.columns if c not in front_cols]
    df_aligned = df_aligned[front_cols + extra_cols]

    return df_aligned


# ----------------------------- Needed Format Conversion -----------------------------

def _to_numeric_amount(series: pd.Series) -> pd.Series:
    """Robustly parse monetary strings with mixed separators.

    Rules per value:
    - Remove NBSP and spaces
    - If both comma and dot present → commas are thousands separators → remove commas
    - If only comma present → treat comma as decimal point
    - Keep a single leading minus and digits/dot
    """
    if series is None:
        return pd.Series(dtype=float)

    def parse_one(val) -> float:
        s = str(val)
        if not s or s.lower() in {"nan", "none"}:
            return 0.0
        s = s.replace('\u00A0', '').replace(' ', '')
        has_comma = ',' in s
        has_dot = '.' in s
        if has_comma and has_dot:
            # Example: 21,500.00 → remove comma thousands
            s = s.replace(',', '')
        elif has_comma and not has_dot:
            # Example: 1 234,56 → 1234.56
            s = s.replace(',', '.')
        # Strip all except digits, minus, and dot; also collapse multiple dots
        s = re.sub(r'[^0-9\.-]', '', s)
        # If multiple dots remain, keep the last one as decimal separator
        if s.count('.') > 1:
            parts = s.split('.')
            s = ''.join(parts[:-1]) + '.' + parts[-1]
        try:
            return float(s)
        except Exception:
            return 0.0

    return series.apply(parse_one).fillna(0.0)


def _looks_like_index(series: pd.Series) -> bool:
    """Heuristic: detect columns that are row indices (1,2,3,...) rather than amounts.
    Criteria: mostly small integers, low magnitude, often sequential.
    """
    try:
        vals = pd.to_numeric(series, errors='coerce')
        vals = vals.dropna()
        if vals.empty:
            return False
        # Mostly integers and small magnitude
        int_like = (vals % 1 == 0).mean() >= 0.9
        max_val = float(vals.max())
        unique_ratio = vals.nunique() / max(len(vals), 1)
        # Sequential check on sorted unique values
        diffs = vals.sort_values().diff().dropna()
        sequential_ratio = (diffs == 1).mean() if not diffs.empty else 0
        return (int_like and max_val <= max(100.0, len(vals) * 1.5)) or sequential_ratio >= 0.7 or unique_ratio >= 0.8
    except Exception:
        return False


def _format_date(series: pd.Series) -> pd.Series:
    try:
        dt = pd.to_datetime(series, errors='coerce', dayfirst=True)
        return dt.dt.strftime('%d.%m.%Y')
    except Exception:
        return series.astype(str)


def extract_legal_entity_from_preamble(df_raw: pd.DataFrame) -> str:
    """
    Inspect the first ~30 rows for a line like "Наименование счёта <ORG>" and extract <ORG>.
    """
    try:
        head = df_raw.head(30).astype(str)
        text = ' '.join(head.apply(lambda r: ' '.join(r.values.tolist()), axis=1).tolist())
        text = text.replace('\n', ' ')
    except Exception:
        return ""

    import re
    patterns = [
        r'наименовани[её]\s*сч[её]та[:\s]+([^\n\r]+?)(?:\s{2,}|конец|остаток|за\s)'
    ]
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            # clean quotes
            name = name.strip('"\'\'”’` ')
            return name
    return ""


def convert_to_needed_format(df_std: pd.DataFrame, detected_bank: str, legal_entity: str, df_raw_trim: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Build a DataFrame that matches the needed format columns:
    ["Дата операции","Сумма операции","Статья","Назначение платежа","Проект",
     "Контрагент","Банковский счёт","Дата начисления","Юридическое лицо"]
    """
    needed_cols = [
        "Дата операции",
        "Сумма операции",
        "Статья",
        "Назначение платежа",
        "Проект",
        "Контрагент",
        "ИНН",
        "Номер счета",
        "Банковский счёт",
        "Дата начисления",
        "Юридическое лицо",
    ]

    if df_std is None or df_std.empty:
        return pd.DataFrame(columns=needed_cols)
    df = df_std.copy()
    
    # Ensure a clean, contiguous index FIRST so all constructed arrays align
    df = df.reset_index(drop=True)
    
    # IMPORTANT: Merge in the raw trimmed columns if available
    # This ensures we don't lose the original Дебет/Кредит columns
    if df_raw_trim is not None and not df_raw_trim.empty:
        # Also reset the raw trimmed DataFrame's index
        df_raw_trim = df_raw_trim.reset_index(drop=True)
        
        # Copy over any Дебет/Кредит columns from raw that might be missing OR replace empty ones
        for col in df_raw_trim.columns:
            col_lower = str(col).lower()
            if 'дебет' in col_lower or 'кредит' in col_lower:
                # Check if the column exists but is empty/all NaN in df_std
                if col in df.columns:
                    # Check if it's all NaN or zeros
                    if df[col].isna().all() or (pd.to_numeric(df[col], errors='coerce').fillna(0) == 0).all():
                        # Replace with raw values using .values to avoid index issues
                        try:
                            if len(df) == len(df_raw_trim):
                                df[col] = df_raw_trim[col].values
                        except Exception as e:
                            pass
                elif col not in df.columns:
                    # Add the column from raw using .values to avoid index issues
                    try:
                        if len(df) == len(df_raw_trim):
                            df[col] = df_raw_trim[col].values
                    except Exception as e:
                        pass
    # Source columns (standard names set by our mappers)
    def _norm(s: str) -> str:
        return str(s or '').strip().lower().replace('\n', ' ')

    # Date columns
    doc_date_col = None
    proc_date_col = None
    for c in df.columns:
        cn = _norm(c)
        if doc_date_col is None and (cn == 'document date' or 'дата' in cn):
            doc_date_col = c
        if proc_date_col is None and (cn == 'processing date'):
            proc_date_col = c

    # Choose best debit/credit columns by non-zero counts
    def _best_amount_col(candidates: list[str]) -> Optional[str]:
        best_name = None
        best_nonzero = -1
        for c in candidates:
            try:
                vals = _to_numeric_amount(df[c])
                nonzero = int((vals != 0).sum())
                if nonzero > best_nonzero:
                    best_nonzero = nonzero
                    best_name = c
            except Exception:
                continue
        return best_name

    debit_candidates = [c for c in df.columns if ('дебет' in _norm(c)) or (_norm(c) == 'debit turnover')]
    credit_candidates = [c for c in df.columns if ('кредит' in _norm(c)) or (_norm(c) == 'credit turnover')]
    debit_col = _best_amount_col(debit_candidates)
    credit_col = _best_amount_col(credit_candidates)

    # Guard: if chosen amount column looks like a row index/serial, discard it
    if debit_col is not None:
        try:
            if _looks_like_index(df[debit_col]):
                debit_col = None
        except Exception:
            pass
    if credit_col is not None:
        try:
            if _looks_like_index(df[credit_col]):
                credit_col = None
        except Exception:
            pass

    # Fallback: try detect directly from raw trimmed headers to avoid mis-mapping
    if (debit_col is None or credit_col is None) and df_raw_trim is not None and not df_raw_trim.empty:
        raw_cols = list(df_raw_trim.columns)
        debit_raw_cands = [c for c in raw_cols if 'дебет' in _norm(c)]
        credit_raw_cands = [c for c in raw_cols if 'кредит' in _norm(c)]
        if debit_col is None and debit_raw_cands:
            debit_col = debit_raw_cands[0]
            try:
                # Bring over this column into df for unified processing
                df[debit_col] = df_raw_trim[debit_col].reindex(df.index)
            except Exception:
                pass
        if credit_col is None and credit_raw_cands:
            credit_col = credit_raw_cands[0]
            try:
                df[credit_col] = df_raw_trim[credit_col].reindex(df.index)
            except Exception:
                pass


    # Payment purpose: prefer explicit keyword; fallback to the rightmost long-text column
    purpose_candidates = [c for c in df.columns if ('назначение' in _norm(c)) or (_norm(c) == 'payment purpose')]
    if not purpose_candidates:
        tail_cols = list(df.columns)[-4:]
        avg_len = -1
        chosen = None
        for c in tail_cols:
            try:
                s = df[c].astype(str)
                cur = float(s.str.len().mean())
                if cur > avg_len:
                    avg_len = cur
                    chosen = c
            except Exception:
                continue
        if chosen is not None:
            purpose_candidates = [chosen]
    purpose_col = purpose_candidates[0] if purpose_candidates else None

    # Counterparty: prefer explicit keywords and avoid picking the same as purpose
    counterparty_candidates = [c for c in df.columns if ('наименован' in _norm(c)) or ('контраг' in _norm(c)) or (_norm(c) == 'account name')]
    counterparty_col = None
    for c in counterparty_candidates:
        if c != purpose_col:
            counterparty_col = c
            break

    # INN: detect various INN column formats
    inn_candidates = []
    for c in df.columns:
        col_lower = _norm(c)
        if any(keyword in col_lower for keyword in ['инн', 'иин', 'taxpayer id', 'бин']):
            inn_candidates.append(c)
    inn_col = inn_candidates[0] if inn_candidates else None
    
    # Account Number: detect various account number column formats  
    account_candidates = []
    for c in df.columns:
        col_lower = _norm(c)
        if any(keyword in col_lower for keyword in [
            '№ счёта', 'номер счёта', 'счёт', 'счет', 'номер счета', 'account no', 'отправителя', 'получателя'
        ]):
            account_candidates.append(c)
    account_col = account_candidates[0] if account_candidates else None
    
    # Special case handling for combined and structured formats
    combined_account_inn_col = None
    structured_requisites_col = None
    
    # Look for combined "Счет/ИИН" format (like in Алока банк)
    # Check both processed df and raw trimmed df
    for c in df.columns:
        col_lower = _norm(c)
        if ('счет' in col_lower or 'счёт' in col_lower) and ('иин' in col_lower or 'инн' in col_lower):
            combined_account_inn_col = c
            break
    
    # If not found in processed df, check raw trimmed df
    if not combined_account_inn_col and df_raw_trim is not None:
        for c in df_raw_trim.columns:
            col_lower = _norm(c)
            if ('счет' in col_lower or 'счёт' in col_lower) and ('иин' in col_lower or 'инн' in col_lower):
                combined_account_inn_col = c
                break
    
    # Look for structured "Реквизиты контрагента" format (like in Янги банк)
    for c in df.columns:
        col_lower = _norm(c)
        if 'реквизиты' in col_lower or 'requisites' in col_lower:
            structured_requisites_col = c
            break
    
    # If not found in processed df, check raw trimmed df
    if not structured_requisites_col and df_raw_trim is not None:
        for c in df_raw_trim.columns:
            col_lower = _norm(c)
            if 'реквизиты' in col_lower or 'requisites' in col_lower:
                structured_requisites_col = c
                break

    # Dates
    date_oper = _format_date(df[doc_date_col]) if doc_date_col else pd.Series([''] * len(df), index=df.index)
    date_accr = _format_date(df[proc_date_col]) if proc_date_col else date_oper

    # Amount sign: debit negative (outgoing), credit positive (incoming)
    if debit_col:
        debit_vals = _to_numeric_amount(df[debit_col])
    else:
        debit_vals = pd.Series([0] * len(df), index=df.index)
    
    if credit_col:
        credit_vals = _to_numeric_amount(df[credit_col])
    else:
        credit_vals = pd.Series([0] * len(df), index=df.index)
    # Fix: In Kazakhstan bank format, Дебет is outgoing (expenses), Кредит is incoming (income)
    # So the amount should be: positive for income (credit), negative for expenses (debit)
    amount = credit_vals - debit_vals

    # Fallback: if selected debit/credit columns are present but all zeros/NaN, try raw trimmed headers
    if df_raw_trim is not None and not df_raw_trim.empty:
        def _maybe_replace_from_raw(kind: str, col_name: Optional[str], current_vals: pd.Series) -> tuple[Optional[str], pd.Series]:
            if col_name is None:
                return col_name, current_vals
            try:
                total_abs = float(current_vals.abs().sum())
            except Exception:
                total_abs = 0.0
            if total_abs > 0:
                return col_name, current_vals
            # Find candidate in raw trim
            raw_cols = [c for c in df_raw_trim.columns if kind in _norm(c)]
            if raw_cols:
                raw_c = raw_cols[0]
                try:
                    series = _to_numeric_amount(df_raw_trim[raw_c]).reindex(df.index).fillna(0.0)
                    if float(series.abs().sum()) > 0:
                        # bring over for downstream and debugging
                        df[raw_c] = df_raw_trim[raw_c].reindex(df.index)
                        return raw_c, series
                except Exception:
                    pass
            return col_name, current_vals

        debit_col, debit_vals = _maybe_replace_from_raw('дебет', debit_col, debit_vals)
        credit_col, credit_vals = _maybe_replace_from_raw('кредит', credit_col, credit_vals)
        amount = credit_vals - debit_vals

    # Fallback: if all amounts are zero, try to detect amount columns heuristically
    if (amount.abs().sum() == 0) or ((credit_vals.abs().sum() == 0) and (debit_vals.abs().sum() == 0)):
        # Consider all columns as potential amount columns
        candidate_scores: list[tuple[str, int, float]] = []  # (col, nonzero_count, median_abs)
        for c in df.columns:
            try:
                vals = _to_numeric_amount(df[c])
            except Exception:
                continue
            nonzero = int((vals != 0).sum())
            if nonzero == 0:
                continue
            # Avoid picking obvious index/serial columns: mostly small integers
            # Skip columns that look like indices or contain footer markers
            try:
                if _looks_like_index(df[c]):
                    continue
            except Exception:
                pass
            cname_norm = _norm(c)
            if any(k in cname_norm for k in ['страниц', 'page', '№']):
                continue
            median_abs = float(vals.abs().replace(0, pd.NA).median() or 0)
            max_abs = float(vals.abs().max() or 0)
            # Heuristic filter: amounts typically larger than 10 and with some variance
            if max_abs <= 10:
                continue
            candidate_scores.append((c, nonzero, median_abs))

        # Sort by nonzero desc, then median magnitude desc
        candidate_scores.sort(key=lambda t: (t[1], t[2]), reverse=True)

        if candidate_scores:
            first_col = candidate_scores[0][0]
            second_col = candidate_scores[1][0] if len(candidate_scores) > 1 else None
            first_vals = _to_numeric_amount(df[first_col])
            second_vals = _to_numeric_amount(df[second_col]) if second_col else pd.Series([0] * len(df), index=df.index)

            # Try to assign credit/debit direction by keywords; else per-row sign from which column is non-zero
            def pick_amount(i: int) -> float:
                f = float(first_vals.iloc[i] or 0)
                s = float(second_vals.iloc[i] or 0)
                if (f != 0) and (s != 0):
                    # If both non-zero, assume f is credit if its header hints 'кред', else pick larger as credit
                    f_is_credit_hint = ('кредит' in _norm(first_col)) or ('credit' in _norm(first_col))
                    s_is_credit_hint = ('кредит' in _norm(second_col or '')) or ('credit' in _norm(second_col or ''))
                    if f_is_credit_hint and not s_is_credit_hint:
                        return f - s
                    if s_is_credit_hint and not f_is_credit_hint:
                        return s - f
                    return max(f, s) - min(f, s)
                if f != 0:
                    # Determine sign by header hint
                    if ('дебет' in _norm(first_col)) or ('debit' in _norm(first_col)):
                        return -f
                    return f
                if s != 0:
                    if ('дебет' in _norm(second_col or '')) or ('debit' in _norm(second_col or '')):
                        return -s
                    return s
                return 0.0

            amount = pd.Series([pick_amount(i) for i in range(len(df))], index=df.index)

    # Article classification
    purpose_text = df[purpose_col].astype(str) if purpose_col else pd.Series([''] * len(df), index=df.index)
    def classify(row_idx: int) -> str:
        desc = purpose_text.iloc[row_idx].lower()
        if debit_vals.iloc[row_idx] > 0:
            if any(k in desc for k in ['зарплата', 'заработная плата', 'оплата труда']):
                return 'Зарплата'
            if any(k in desc for k in ['налог', 'ндс', 'подоходный']):
                return 'Налоги'
            return 'Расход'
        if credit_vals.iloc[row_idx] > 0:
            if any(k in desc for k in ['оплата', 'поступление', 'выручка', 'оплата от']):
                return 'Выручка'
            return 'Приход'
        return ''
    article = [classify(i) for i in range(len(df))]

    # Extract INN and Account Number values
    def extract_inn_and_account(row_idx):
        """Extract INN and account number from various column formats"""
        inn_value = ""
        account_value = ""
        
        # Try direct columns first
        if inn_col and pd.notna(df[inn_col].iloc[row_idx]):
            inn_value = str(df[inn_col].iloc[row_idx]).strip()
            
        if account_col and pd.notna(df[account_col].iloc[row_idx]):
            account_value = str(df[account_col].iloc[row_idx]).strip()
        
        # Handle combined "Счет/ИИН" format - Алока банк specific structure:
        # Line 0: Account number (20208000005571676002)
        # Line 1: INN (309859996) 
        # Line 2: Company name
        if combined_account_inn_col:
            # Use df_raw_trim if the column is found there, otherwise use df
            source_df = df_raw_trim if df_raw_trim is not None and combined_account_inn_col in df_raw_trim.columns else df
            if row_idx < len(source_df) and pd.notna(source_df[combined_account_inn_col].iloc[row_idx]):
                combined_text = str(source_df[combined_account_inn_col].iloc[row_idx])
                lines = combined_text.split('\n')
                
                # Clean lines and remove empty ones
                clean_lines = []
                for line in lines:
                    line = line.strip()
                    if line:  # Keep even empty-looking lines for indexing
                        clean_lines.append(line)
                
                if len(clean_lines) >= 1:
                    # First line: Account number
                    first_line = clean_lines[0].strip()
                    if first_line and not account_value:
                        account_value = first_line
                    
                    # Second line: INN (if exists and is numeric)
                    if len(clean_lines) >= 2:
                        second_line = clean_lines[1].strip()
                        if second_line and second_line.isdigit() and 9 <= len(second_line) <= 12:
                            if not inn_value:
                                inn_value = second_line
        
        # Handle structured "Реквизиты контрагента" format
        if structured_requisites_col and pd.notna(df[structured_requisites_col].iloc[row_idx]):
            requisites_text = str(df[structured_requisites_col].iloc[row_idx])
            
            # Extract МФО, Счет, ИНН using regex patterns
            import re
            
            # Extract account from "Счет: 16401000005571676001"
            account_match = re.search(r'Счет:\s*(\d+)', requisites_text, re.IGNORECASE)
            if account_match and not account_value:
                account_value = account_match.group(1)
                
            # Extract INN from "ИНН: 309859996"  
            inn_match = re.search(r'ИНН:\s*(\d+)', requisites_text, re.IGNORECASE)
            if inn_match and not inn_value:
                inn_value = inn_match.group(1)
        
        return inn_value, account_value

    # Build INN and Account series
    inn_series = []
    account_series = []
    
    for i in range(len(df)):
        inn_val, account_val = extract_inn_and_account(i)
        inn_series.append(inn_val)
        account_series.append(account_val)
    
    inn_series = pd.Series(inn_series, index=df.index)
    account_series = pd.Series(account_series, index=df.index)
    
    # Compose needed frame
    counterparty_series = df[counterparty_col].astype(str) if counterparty_col else pd.Series([''] * len(df), index=df.index)

    needed = pd.DataFrame({
        "Дата операции": date_oper.tolist(),
        "Сумма операции": amount.tolist(),
        "Статья": list(article),
        "Назначение платежа": purpose_text.tolist(),
        "Проект": [''] * len(df),
        "Контрагент": counterparty_series.tolist(),
        "ИНН": inn_series.tolist(),
        "Номер счета": account_series.tolist(),
        "Банковский счёт": [detected_bank] * len(df),
        "Дата начисления": date_accr.tolist(),
        "Юридическое лицо": [legal_entity] * len(df),
    })


    # Drop rows that carry no value (e.g., stray footer, totals, empty accounts)
    def _is_nonblank(val: str) -> bool:
        s = str(val).strip().lower()
        return s not in {'', 'nan', 'none'}
    
    def _is_not_total_row(row: pd.Series) -> bool:
        """Check if row is NOT a total/summary row"""
        # Check common total keywords in various columns
        total_keywords = ['итого', 'всего', 'total', 'баланс', 'остаток', 'оборот за период', 'summary']
        
        # Check in counterparty/account name
        if pd.notna(row.get('Контрагент')):
            text = str(row['Контрагент']).lower()
            if any(keyword in text for keyword in total_keywords):
                return False
        
        # Check in payment purpose
        if pd.notna(row.get('Назначение платежа')):
            text = str(row['Назначение платежа']).lower()
            if any(keyword in text for keyword in total_keywords):
                return False
        
        # Also check if this looks like a total row (no date but has amount)
        has_date = _is_nonblank(str(row.get('Дата операции', '')))
        has_amount = pd.notna(row.get('Сумма операции')) and row['Сумма операции'] != 0
        if not has_date and has_amount:
            # Likely a total row
            return False
            
        return True

    # Create mask for valid rows
    keep_mask = (
        # Must have at least date or purpose or non-zero amount
        (needed["Дата операции"].apply(_is_nonblank) |
         needed["Назначение платежа"].apply(_is_nonblank) |
         (pd.to_numeric(needed["Сумма операции"], errors='coerce').fillna(0) != 0)) &
        # AND must not be a total row
        needed.apply(_is_not_total_row, axis=1)
    )
    
    needed = needed.loc[keep_mask].reset_index(drop=True)

    return needed


def unify_files(
    files: Sequence[Tuple[str, io.BytesIO]],
    template_path: str,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Unify multiple uploaded files to the template columns.

    Args:
        files: sequence of (filename, bytes_io)
        template_path: path to the template Excel file

    Returns:
        (combined_df, stats)
        stats: per-file row counts after normalization
    """
    template_cols = load_template_columns(template_path)
    if not template_cols:
        # As a fallback, use our standard names order
        template_cols = [
            "Serial No.",
            "Document Date",
            "Processing Date",
            "Document No.",
            "Account Name",
            "Taxpayer ID (INN)",
            "Account No.",
            "Bank Code (MFO)",
            "Debit Turnover",
            "Credit Turnover",
            "Payment Purpose",
            "Operation Code",
            "Transaction Type",
            "Amount",
        ]

    unified_frames: List[pd.DataFrame] = []
    stats: Dict[str, int] = {}

    for filename, buffer in files:
        df_raw = read_any_bank_file(buffer, filename)
        if df_raw is None or df_raw.empty:
            stats[filename] = 0
            continue

        detected_bank = detect_bank_from_filename(filename)
        legal_entity = extract_legal_entity_from_preamble(df_raw)

        # Keep a trimmed copy before any mapping to preserve native headers
        df_raw_trim = trim_to_numbered_table(df_raw)
        df_std = normalize_to_standard(df_raw)

        # Convert to the exact needed format
        df_needed = convert_to_needed_format(df_std, detected_bank, legal_entity, df_raw_trim=df_raw_trim)

        # Ensure column order per template
        ordered_cols = [c for c in template_cols if c in df_needed.columns]
        keep_cols = ordered_cols + [c for c in df_needed.columns if c not in ordered_cols]
        df_needed = df_needed[keep_cols]

        # Add metadata for traceability
        df_needed["Source File"] = filename

        unified_frames.append(df_needed)
        stats[filename] = len(df_needed)

    if unified_frames:
        combined = pd.concat(unified_frames, ignore_index=True, sort=False)
    else:
        combined = pd.DataFrame(columns=list(template_cols))

    return combined, stats


def save_unified_outputs(
    df: pd.DataFrame,
    output_dir: str,
    base_name: str = "unified_bank_statements",
) -> Tuple[str, str]:
    """
    Save unified DataFrame to CSV and XLSX in processed_data.
    Returns (csv_path, xlsx_path).
    """
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, f"{base_name}.csv")
    xlsx_path = os.path.join(output_dir, f"{base_name}.xlsx")

    try:
        df.to_csv(csv_path, index=False)
    except Exception:
        pass

    try:
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
    except Exception:
        pass

    return csv_path, xlsx_path


