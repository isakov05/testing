"""
Database operations for financial data (invoices, bank transactions, reconciliation)
Handles saving and loading data from PostgreSQL
"""
import pandas as pd
import uuid
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any
import psycopg2
from psycopg2.extras import execute_batch
from .db_helper import get_db_connection, get_db_engine


# ============================================================================
# INVOICE OPERATIONS
# ============================================================================

def save_invoices_to_db(
    df: pd.DataFrame,
    user_id,  # Can be int or str, will be converted to str
    invoice_type: str,
    filename: str,
    uploaded_by: str
) -> Tuple[int, int, Optional[str], Optional[pd.DataFrame]]:
    """
    Save invoices to database with duplicate detection.

    Args:
        df: DataFrame with processed invoice data
        user_id: User identifier (int or str, will be converted to str)
        invoice_type: 'IN' or 'OUT'
        filename: Source filename
        uploaded_by: Username who uploaded

    Returns:
        (records_inserted, duplicates_skipped, error_message, duplicates_df)
    """
    if df is None or df.empty:
        return 0, 0, "Empty DataFrame", None

    # Convert user_id to string for VARCHAR column
    user_id = str(user_id)

    conn = None
    batch_id = str(uuid.uuid4())  # Convert UUID to string for psycopg2
    records_inserted = 0
    duplicates_skipped = 0
    duplicates_details: List[dict] = []

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Check if this file has already been uploaded by this user
        cur.execute("""
            SELECT id, records_count FROM upload_batches
            WHERE user_id = %s AND filename = %s AND data_type = %s AND processing_status = 'completed'
            ORDER BY upload_date DESC
            LIMIT 1
        """, (user_id, filename, f'invoice_{invoice_type.lower()}'))

        existing_upload = cur.fetchone()
        if existing_upload:
            # File already uploaded, skip entire file
            return 0, df.shape[0], None, None

        # Create upload batch record
        cur.execute("""
            INSERT INTO upload_batches (id, user_id, filename, data_type, uploaded_by, processing_status)
            VALUES (%s, %s, %s, %s, %s, 'processing')
        """, (batch_id, user_id, filename, f'invoice_{invoice_type.lower()}', uploaded_by))

        # Prepare invoice records
        def _s(val: str, max_len: int) -> str:
            """Convert to string, handling None, NaN, and 'nan' string"""
            if val is None or pd.isna(val):
                return None
            s = str(val).strip()
            # Don't store 'nan' string
            if s.lower() == 'nan' or s == '':
                return None
            return s[:max_len]

        def _date(val):
            ts = pd.to_datetime(val, errors='coerce')
            if pd.isna(ts):
                return None
            try:
                return ts.date()
            except Exception:
                return None

        def _num(val):
            n = pd.to_numeric(val, errors='coerce')
            if pd.isna(n):
                return None
            return float(n)

        # Normalize column names to handle different file formats
        col_aliases = {
            # Document number
            'Номер документа': 'Номер документ',
            'номер документа': 'Номер документ',
            'Document Number': 'Номер документ',
            # Document date
            'Дата документа': 'Дата документ',
            'дата документа': 'Дата документ',
            'Document Date': 'Дата документ',
            # Seller INN
            'Продавец (ИНН/ПИНФЛ)': 'Продавец (ИНН или ПИНФЛ)',
            'Продавец(ИНН/ПИНФЛ)': 'Продавец (ИНН или ПИНФЛ)',
            'Seller (Tax ID or PINFL)': 'Продавец (ИНН или ПИНФЛ)',
            'Продавец (ИНН)': 'Продавец (ИНН или ПИНФЛ)',
            # Seller name
            'Seller (Name)': 'Продавец (наименование)',
            'Продавец(наименование)': 'Продавец (наименование)',
            # Seller branch
            'Продавец (код филиала)': 'Продавец (код филиала)',
            'Продавец(код филиала)': 'Продавец (код филиала)',
            'Продавец (название филиала)': 'Продавец (название филиала)',
            'Продавец(название филиала)': 'Продавец (название филиала)',
            # Buyer INN
            'Покупатель (ИНН/ПИНФЛ)': 'Покупатель (ИНН или ПИНФЛ)',
            'Покупатель(ИНН/ПИНФЛ)': 'Покупатель (ИНН или ПИНФЛ)',
            'Buyer (Tax ID or PINFL)': 'Покупатель (ИНН или ПИНФЛ)',
            'Покупатель (ИНН)': 'Покупатель (ИНН или ПИНФЛ)',
            # Buyer name
            'Buyer (Name)': 'Покупатель (наименование)',
            'Покупатель(наименование)': 'Покупатель (наименование)',
            # Buyer branch
            'Покупатель (код филиала)': 'Покупатель (код филиала)',
            'Покупатель(код филиала)': 'Покупатель (код филиала)',
            'Покупатель (название филиала)': 'Покупатель (название филиала)',
            'Покупатель(название филиала)': 'Покупатель (название филиала)',
            # Amounts
            'Supply Value': 'Стоимость поставки',
            'VAT Amount': 'НДС сумма',
            'НДС': 'НДС сумма',
            'Supply Value (incl. VAT)': 'Стоимость поставки с учётом НДС',
            'Сумма с НДС': 'Стоимость поставки с учётом НДС',
            'Сумма к оплате': 'Стоимость поставки с учётом НДС',
            # Status
            'Статус': 'СТАТУС',
            'Status': 'СТАТУС',
            # Contract
            'Номер договора': 'Договор номер',
            'Contract Number': 'Договор номер',
            'Дата договора': 'Договор дата',
            'Contract Date': 'Договор дата',
            # Document type
            'Тип документа': 'ТИП документ',
            'Document Type': 'ТИП документ',
            'Вид документа': 'Вид документ',
            'Document Kind': 'Вид документ',
            # Other
            'Тип ЭСФ': 'ТИП документ',
            'Дата отправки': 'Дата отправки',
            'Send Date': 'Дата отправки',
            'Note': 'ПРИМЕЧАНИЕ',
            'Примечание': 'ПРИМЕЧАНИЕ',
        }

        # Apply column renaming (only rename if source exists and target doesn't)
        rename_map = {}
        taken = set(df.columns)
        for old, new in col_aliases.items():
            if old in df.columns and new not in taken:
                rename_map[old] = new
                taken.add(new)
        if rename_map:
            df = df.rename(columns=rename_map)
            print(f"DEBUG save_invoices: Renamed columns: {rename_map}")

        # Remove duplicate columns (keep first)
        df = df.loc[:, ~df.columns.duplicated()]
        print(f"DEBUG save_invoices: Final columns: {list(df.columns)[:20]}")

        invoice_records = []
        for _, row in df.iterrows():
            invoice_records.append((
                _s(user_id, 100),
                _s(row.get('Номер документ', row.get('Document Number', '')), 100),
                _date(row.get('Дата документ', row.get('Document Date'))),
                invoice_type,
                _s(row.get('Продавец (ИНН или ПИНФЛ)', row.get('Seller (Tax ID or PINFL)', '')), 20),
                _s(row.get('Продавец (наименование)', row.get('Seller (Name)', '')), 500),
                _s(row.get('Продавец (код филиала)', ''), 50),
                _s(row.get('Продавец (название филиала)', ''), 500),
                _s(row.get('Покупатель (ИНН или ПИНФЛ)', row.get('Buyer (Tax ID or PINFL)', '')), 20),
                _s(row.get('Покупатель (наименование)', row.get('Buyer (Name)', '')), 500),
                _s(row.get('Покупатель (код филиала)', ''), 50),
                _s(row.get('Покупатель (название филиала)', ''), 500),
                _num(row.get('Стоимость поставки', row.get('Supply Value', 0))),
                _num(row.get('НДС сумма', row.get('VAT Amount', 0))),
                _num(row.get('Стоимость поставки с учётом НДС', row.get('Supply Value (incl. VAT)', 0))),
                _s(row.get('СТАТУС', row.get('Status', '')), 100),
                _s(row.get('Договор номер', row.get('Contract Number', '')), 100),
                _date(row.get('Договор дата', row.get('Contract Date'))),
                _s(row.get('ТИП документ', row.get('Document Type', '')), 100),
                _s(row.get('Вид документ', row.get('Document Kind', '')), 100),
                _date(row.get('Дата отправки', row.get('Send Date'))),
                _s(row.get('Примечание к товару (работе, услуге)',
                           row.get('Item Note (Goods/Works/Services)',
                           row.get('ПРИМЕЧАНИЕ',
                           row.get('Note', None)))), 2000),
                _s(filename, 255),
                batch_id,
                _s(uploaded_by, 100)
            ))

        # Insert invoices - no duplicate checking since we check at file level
        insert_query = """
            INSERT INTO invoices (
                user_id, document_number, document_date, invoice_type,
                seller_inn, seller_name, seller_branch_code, seller_branch_name,
                buyer_inn, buyer_name, buyer_branch_code, buyer_branch_name,
                supply_value, vat_amount, total_amount,
                status, contract_number, contract_date,
                document_type, document_kind, send_date, note,
                source_filename, upload_batch_id, uploaded_by
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING id
        """

        def _none_if_nat(v):
            try:
                if v is None:
                    return None
                # Handle pandas NaT/NaN and string 'NaT'
                import pandas as _pd
                if isinstance(v, (_pd.Timestamp,)) and _pd.isna(v):
                    return None
                if isinstance(v, float) and (_pd.isna(v)):
                    return None
                if isinstance(v, str) and v.strip().lower() == 'nat':
                    return None
            except Exception:
                pass
            return v

        invoice_ids = []
        for idx, record in enumerate(invoice_records):
            # Clean NaT/NaN values
            cleaned = tuple(_none_if_nat(x) for x in record)

            # Skip records with NULL values in key fields
            # Fields: user_id(0), document_number(1), document_date(2), seller_inn(4), buyer_inn(8)
            if cleaned[1] is None or cleaned[2] is None or cleaned[4] is None or cleaned[8] is None:
                if idx == 0:
                    # Log first skipped row for debugging
                    print(f"SKIP row 0: doc_num={cleaned[1]}, doc_date={cleaned[2]}, seller_inn={cleaned[4]}, buyer_inn={cleaned[8]}")
                    print(f"  DataFrame columns: {list(df.columns)[:15]}")
                    print(f"  Row keys tried: 'Номер документ'={df.iloc[0].get('Номер документ', 'MISSING')}, 'Document Number'={df.iloc[0].get('Document Number', 'MISSING')}")
                duplicates_skipped += 1
                invoice_ids.append(None)
                continue

            try:
                cur.execute(insert_query, cleaned)
                result = cur.fetchone()
                if result:
                    invoice_ids.append(result[0])
                    records_inserted += 1
                else:
                    invoice_ids.append(None)
            except Exception as e:
                print(f"Error inserting invoice {idx}: {str(e)}")
                invoice_ids.append(None)

        # Now insert invoice items if we have the product-level columns
        if '№.1' in df.columns or 'Item Number' in df.columns:
            item_records = []
            for idx, row in df.iterrows():
                if idx < len(invoice_ids) and invoice_ids[idx]:
                    item_records.append((
                        invoice_ids[idx],
                        user_id,
                        pd.to_numeric(row.get('№.1', row.get('Item Number', 0)), errors='coerce'),
                        str(row.get('Примечание к товару (работе, услуге)', row.get('Item Note (Goods/Works/Services)', ''))),
                        str(row.get('Идентификационный код и название по Единому электронному национальному каталогу товаров (услуг)', '')),
                        pd.to_numeric(row.get('Количество', row.get('Quantity', 0)), errors='coerce'),
                        _s(row.get('Единица измерения', row.get('Unit of Measure', '')), 50),
                        pd.to_numeric(row.get('Цена', row.get('Price', 0)), errors='coerce'),
                        pd.to_numeric(row.get('Стоимость поставки', row.get('Supply Value', 0)), errors='coerce'),
                        pd.to_numeric(row.get('Ставка акциз', 0), errors='coerce'),
                        pd.to_numeric(row.get('Сумма акциз', 0), errors='coerce'),
                        pd.to_numeric(row.get('НДС ставка', 0), errors='coerce'),
                        pd.to_numeric(row.get('НДС сумма', 0), errors='coerce'),
                        pd.to_numeric(row.get('Стоимость поставки с учётом НДС', 0), errors='coerce'),
                        _s(row.get('Маркировка', ''), 100),
                        _s(row.get('Происхождение товара', ''), 100)
                    ))

            if item_records:
                item_insert_query = """
                    INSERT INTO invoice_items (
                        invoice_id, user_id, item_number, item_note, catalog_code,
                        quantity, unit_of_measure, unit_price, supply_value,
                        excise_rate, excise_amount, vat_rate, vat_amount, total_amount,
                        marking, origin_country
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                execute_batch(cur, item_insert_query, item_records, page_size=1000)

        # Update batch record
        cur.execute("""
            UPDATE upload_batches
            SET records_count = %s, duplicates_skipped = %s, processing_status = 'completed', completed_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (records_inserted, duplicates_skipped, batch_id))

        conn.commit()
        dup_df = pd.DataFrame(duplicates_details) if duplicates_details else None
        return records_inserted, duplicates_skipped, None, dup_df

    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Error saving invoices: {str(e)}"
        print(error_msg)

        # Try to update batch with error
        try:
            if conn:
                cur = conn.cursor()
                cur.execute("""
                    UPDATE upload_batches
                    SET processing_status = 'failed', error_message = %s, completed_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (error_msg, batch_id))
                conn.commit()
        except:
            pass

        dup_df = pd.DataFrame(duplicates_details) if duplicates_details else None
        return records_inserted, duplicates_skipped, error_msg, dup_df

    finally:
        if conn:
            conn.close()


def load_user_invoices(user_id, invoice_type: Optional[str] = None) -> pd.DataFrame:
    """
    Load all invoices for a user from database.

    Args:
        user_id: User identifier (int or str)
        invoice_type: Optional filter for 'IN' or 'OUT', None loads both

    Returns:
        DataFrame with invoice data
    """
    try:
        # Convert user_id to string for VARCHAR column
        user_id = str(user_id)

        engine = get_db_engine()

        query = """
            SELECT
                document_number as "Document Number",
                document_date as "Document Date",
                invoice_type,
                seller_inn as "Seller (Tax ID or PINFL)",
                seller_name as "Seller (Name)",
                buyer_inn as "Buyer (Tax ID or PINFL)",
                buyer_name as "Buyer (Name)",
                supply_value as "Supply Value",
                vat_amount as "VAT Amount",
                total_amount as "Supply Value (incl. VAT)",
                status as "Status",
                contract_number as "Contract Number",
                contract_date as "Contract Date",
                note as "product_note",
                source_filename,
                upload_date
            FROM invoices
            WHERE user_id = %(user_id)s
        """

        params = {'user_id': user_id}
        if invoice_type:
            query += " AND invoice_type = %(invoice_type)s"
            params['invoice_type'] = invoice_type

        query += " ORDER BY document_date DESC"

        df = pd.read_sql_query(query, engine, params=params)
        # Rename note column to Russian name AFTER loading to avoid PostgreSQL identifier length limit
        if 'product_note' in df.columns:
            df = df.rename(columns={'product_note': 'Примечание к товару (работе, услуге)'})
            print(f"DEBUG load_user_invoices: Renamed 'product_note' to 'Примечание к товару (работе, услуге)'")

        # Check if note column has data
        if 'Примечание к товару (работе, услуге)' in df.columns:
            non_empty_notes = df['Примечание к товару (работе, услуге)'].notna().sum()
            print(f"DEBUG load_user_invoices: Note column exists with {non_empty_notes} non-empty values out of {len(df)}")
        else:
            print("DEBUG load_user_invoices: WARNING - Note column NOT in result after rename!")

        return df

    except Exception as e:
        print(f"Error loading invoices: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def load_user_invoice_items(user_id, invoice_type: Optional[str] = None) -> pd.DataFrame:
    """
    Load all invoice items (line items) for a user from database.

    Args:
        user_id: User identifier (int or str)
        invoice_type: Optional filter for 'IN' or 'OUT', None loads both

    Returns:
        DataFrame with invoice item data including product names, quantities, prices
    """
    try:
        # Convert user_id to string for VARCHAR column
        user_id = str(user_id)

        engine = get_db_engine()

        query = """
            SELECT
                ii.invoice_id,
                i.document_number as "Document Number",
                i.document_date as "Document Date",
                i.invoice_type,
                i.seller_inn as "Seller (Tax ID or PINFL)",
                i.seller_name as "Seller (Name)",
                i.buyer_inn as "Buyer (Tax ID or PINFL)",
                i.buyer_name as "Buyer (Name)",
                ii.item_number as "Item Number",
                ii.item_note as "Product Name",
                ii.catalog_code as "Catalog Code",
                ii.quantity as "Quantity",
                ii.unit_of_measure as "Unit of Measure",
                ii.unit_price as "Unit Price",
                ii.supply_value as "Supply Value",
                ii.vat_rate as "VAT Rate",
                ii.vat_amount as "VAT Amount",
                ii.total_amount as "Total Amount",
                ii.marking as "Marking",
                ii.origin_country as "Origin Country",
                i.source_filename,
                i.upload_date
            FROM invoice_items ii
            JOIN invoices i ON ii.invoice_id = i.id
            WHERE ii.user_id = %(user_id)s
        """

        params = {'user_id': user_id}
        if invoice_type:
            query += " AND i.invoice_type = %(invoice_type)s"
            params['invoice_type'] = invoice_type

        query += " ORDER BY i.document_date DESC, ii.item_number"

        df = pd.read_sql_query(query, engine, params=params)
        return df

    except Exception as e:
        print(f"Error loading invoice items: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# BANK TRANSACTION OPERATIONS
# ============================================================================

def save_bank_transactions_to_db(
    df: pd.DataFrame,
    user_id: str,
    filename: str,
    uploaded_by: str
) -> Tuple[int, int, Optional[str], Optional[pd.DataFrame]]:
    """
    Save bank transactions to database with duplicate detection.

    Args:
        df: DataFrame with processed bank transaction data
        user_id: User identifier
        filename: Source filename
        uploaded_by: Username who uploaded

    Returns:
        (records_inserted, duplicates_skipped, error_message)
    """
    if df is None or df.empty:
        return 0, 0, "Empty DataFrame", None

    # Convert user_id to string for VARCHAR column
    user_id = str(user_id)

    conn = None
    batch_id = str(uuid.uuid4())  # Convert UUID to string for psycopg2
    records_inserted = 0
    duplicates_skipped = 0
    duplicates_details: List[dict] = []

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Check if this file has already been uploaded by this user
        cur.execute("""
            SELECT id, records_count FROM upload_batches
            WHERE user_id = %s AND filename = %s AND data_type = 'bank_statement' AND processing_status = 'completed'
            ORDER BY upload_date DESC
            LIMIT 1
        """, (user_id, filename))

        existing_upload = cur.fetchone()
        if existing_upload:
            # File already uploaded, skip entire file
            return 0, df.shape[0], None, None

        # Create upload batch record
        cur.execute("""
            INSERT INTO upload_batches (id, user_id, filename, data_type, uploaded_by, processing_status)
            VALUES (%s, %s, %s, 'bank_statement', %s, 'processing')
        """, (batch_id, user_id, filename, uploaded_by))

        # Prepare transaction records
        def _s(val: str, max_len: int) -> str:
            """Clamp string to max_len to satisfy VARCHAR constraints."""
            s = str(val) if val is not None else ''
            return s[:max_len]

        def _clean_date(v: Any):
            """PostgreSQL DATE columns need None instead of NaT."""
            if v is None:
                return None
            ts = pd.to_datetime(v, errors='coerce')
            if pd.isna(ts):
                return None
            return ts.date()

        def _clean_num(v: Any):
            """Use None for missing numeric values so DECIMAL accepts them."""
            x = pd.to_numeric(v, errors='coerce')
            if pd.isna(x):
                return None
            return float(x)

        def _normalize_tx_type(value: str) -> str:
            """Map various source labels to one of {'Incoming','Outgoing','Unknown'}."""
            if value is None:
                return 'Unknown'
            v = str(value).strip().lower()
            if not v:
                return 'Unknown'
            incoming_keywords = [
                'incoming', 'credit', 'receipt', 'deposit',
                'приход', 'поступ', 'кредит', 'зачисл'
            ]
            outgoing_keywords = [
                'outgoing', 'debit', 'payment', 'expense',
                'расход', 'списан', 'дебет', 'оплат'
            ]
            if any(k in v for k in incoming_keywords):
                return 'Incoming'
            if any(k in v for k in outgoing_keywords):
                return 'Outgoing'
            return 'Unknown'

        transaction_records = []
        for _, row in df.iterrows():
            transaction_records.append((
                user_id,
                _clean_date(row.get('date', row.get('Document Date'))),
                _s(row.get('Document No.', row.get('№док', '')), 100),
                _clean_date(row.get('Document Date', row.get('Дата документ'))),
                _clean_date(row.get('Processing Date', row.get('Дата обработки'))),
                _s(row.get('inn', row.get('Taxpayer ID (INN)', '')), 20),
                _s(row.get('Account Name', row.get('Наименование плательщика', '')), 500),
                _s(row.get('Account Number', ''), 50),
                _s(row.get('Bank Code', ''), 20),
                _clean_num(row.get('amount', row.get('Amount', 0))),
                _clean_num(row.get('Debit Turnover', 0)),
                _clean_num(row.get('Credit Turnover', 0)),
                _normalize_tx_type(row.get('Transaction Type', 'Unknown')),
                str(row.get('Payment Purpose', row.get('Назначение платежа', '')) or ''),
                _s(row.get('contract_number', row.get('Contract Number', '')), 100),
                filename,
                batch_id,
                uploaded_by
            ))

        # Insert transactions - no duplicate checking since we check at file level
        insert_query = """
            INSERT INTO bank_transactions (
                user_id, transaction_date, document_number, document_date, processing_date,
                counterparty_inn, counterparty_name, counterparty_account, counterparty_bank_code,
                amount, debit_amount, credit_amount, transaction_type,
                payment_purpose, contract_number,
                source_filename, upload_batch_id, uploaded_by
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING id
        """

        insert_errors = 0
        first_error_detail: Optional[str] = None

        for idx, record in enumerate(transaction_records):
            cur.execute("SAVEPOINT bank_tx_row")
            try:
                cur.execute(insert_query, record)
                result = cur.fetchone()
                if result:
                    records_inserted += 1
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT bank_tx_row")
                insert_errors += 1
                err_text = str(e).strip()
                if first_error_detail is None:
                    first_error_detail = f"row index {idx}: {err_text}"
                print(f"Error inserting transaction {idx}: {err_text}")
            finally:
                cur.execute("RELEASE SAVEPOINT bank_tx_row")

        batch_status = 'completed'
        batch_error: Optional[str] = None
        if insert_errors:
            batch_error = first_error_detail or f"{insert_errors} row(s) failed to insert"
            if records_inserted == 0:
                batch_status = 'failed'

        # Update batch record
        cur.execute("""
            UPDATE upload_batches
            SET records_count = %s, duplicates_skipped = %s, errors_count = %s,
                processing_status = %s, error_message = %s, completed_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (records_inserted, duplicates_skipped, insert_errors, batch_status, batch_error, batch_id))

        conn.commit()
        dup_df = pd.DataFrame(duplicates_details) if duplicates_details else None

        if batch_status == 'failed':
            return records_inserted, duplicates_skipped, batch_error, dup_df
        if insert_errors:
            warn = (
                f"{insert_errors} row(s) skipped due to DB errors."
                + (f" First: {first_error_detail}" if first_error_detail else "")
            )
            return records_inserted, duplicates_skipped, warn, dup_df
        return records_inserted, duplicates_skipped, None, dup_df

    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Error saving bank transactions: {str(e)}"
        print(error_msg)
        # Try to update batch with error
        try:
            if conn:
                cur = conn.cursor()
                cur.execute("""
                    UPDATE upload_batches
                    SET processing_status = 'failed', error_message = %s, completed_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (error_msg, batch_id))
                conn.commit()
        except:
            pass

        dup_df = pd.DataFrame(duplicates_details) if duplicates_details else None
        return records_inserted, duplicates_skipped, error_msg, dup_df

    finally:
        if conn:
            conn.close()


def load_user_bank_transactions(user_id) -> pd.DataFrame:
    """
    Load all bank transactions for a user from database.

    Args:
        user_id: User identifier (int or str)

    Returns:
        DataFrame with bank transaction data
    """
    try:
        # Convert user_id to string for VARCHAR column
        user_id = str(user_id)

        engine = get_db_engine()

        query = """
            SELECT
                transaction_date as "date",
                document_date as "Document Date",
                counterparty_inn as "inn",
                counterparty_inn as "Taxpayer ID (INN)",
                counterparty_name as "Account Name",
                amount as "amount",
                amount as "Amount",
                debit_amount as "Debit Turnover",
                credit_amount as "Credit Turnover",
                transaction_type as "Transaction Type",
                payment_purpose as "Payment Purpose",
                contract_number as "Contract Number",
                document_number as "Document No.",
                processing_date as "Processing Date",
                source_filename,
                upload_date
            FROM bank_transactions
            WHERE user_id = %(user_id)s
            ORDER BY transaction_date DESC
        """

        df = pd.read_sql_query(query, engine, params={'user_id': user_id})
        return df

    except Exception as e:
        print(f"Error loading bank transactions: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# RECONCILIATION OPERATIONS
# ============================================================================

def save_reconciliation_to_db(
    df: pd.DataFrame,
    user_id: str,
    record_type: str,
    filename: str,
    uploaded_by: str,
    report_date: Optional[datetime] = None
) -> Tuple[int, int, Optional[str], Optional[pd.DataFrame]]:
    """
    Save reconciliation records to database with duplicate detection.

    Args:
        df: DataFrame with processed reconciliation data
        user_id: User identifier
        record_type: 'IN' or 'OUT'
        filename: Source filename
        uploaded_by: Username who uploaded
        report_date: Optional report date

    Returns:
        (records_inserted, duplicates_skipped, error_message)
    """
    if df is None or df.empty:
        return 0, 0, "Empty DataFrame", None

    # Convert user_id to string for VARCHAR column
    user_id = str(user_id)

    conn = None
    batch_id = str(uuid.uuid4())  # Convert UUID to string for psycopg2
    records_inserted = 0
    duplicates_skipped = 0
    duplicates_details: List[dict] = []

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Check if this file has already been uploaded by this user
        cur.execute("""
            SELECT id, records_count FROM upload_batches
            WHERE user_id = %s AND filename = %s AND data_type = %s AND processing_status = 'completed'
            ORDER BY upload_date DESC
            LIMIT 1
        """, (user_id, filename, f'reconciliation_{record_type.lower()}'))

        existing_upload = cur.fetchone()
        if existing_upload:
            # File already uploaded, skip entire file
            return 0, df.shape[0], None, None

        # Create upload batch record
        cur.execute("""
            INSERT INTO upload_batches (id, user_id, filename, data_type, uploaded_by, processing_status)
            VALUES (%s, %s, %s, %s, %s, 'processing')
        """, (batch_id, user_id, filename, f'reconciliation_{record_type.lower()}', uploaded_by))

        # Use today's date if not provided
        if report_date is None:
            report_date = datetime.now().date()

        # Prepare reconciliation records
        def _s(val, max_len: int = None) -> str:
            """Convert to string, handling None and NaN"""
            if val is None or pd.isna(val):
                return ''
            s = str(val).strip()
            if s.lower() == 'nan':
                return ''
            if max_len:
                return s[:max_len]
            return s

        def _num(val):
            """Convert to float, handling None and NaN"""
            n = pd.to_numeric(val, errors='coerce')
            if pd.isna(n):
                return None
            return float(n)

        recon_records = []
        for _, row in df.iterrows():
            recon_records.append((
                user_id,
                _s(row.get('Customer_INN', ''), 20),
                _s(row.get('Original_INN_Column', ''), 500),
                _num(row.get('Outstanding_Amount', 0)),
                record_type,
                report_date,
                filename,
                batch_id,
                uploaded_by
            ))

        # Insert reconciliation records - no duplicate checking since we check at file level
        insert_query = """
            INSERT INTO reconciliation_records (
                user_id, counterparty_inn, counterparty_name, outstanding_amount,
                record_type, report_date,
                source_filename, upload_batch_id, uploaded_by
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING id
        """

        for idx, record in enumerate(recon_records):
            try:
                cur.execute(insert_query, record)
                result = cur.fetchone()
                if result:
                    records_inserted += 1
            except Exception as e:
                print(f"Error inserting reconciliation record {idx}: {str(e)}")

        # Update batch record
        cur.execute("""
            UPDATE upload_batches
            SET records_count = %s, duplicates_skipped = %s, processing_status = 'completed', completed_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (records_inserted, duplicates_skipped, batch_id))

        conn.commit()
        dup_df = pd.DataFrame(duplicates_details) if duplicates_details else None
        return records_inserted, duplicates_skipped, None, dup_df

    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Error saving reconciliation records: {str(e)}"
        print(error_msg)
        # Try to update batch with error
        try:
            if conn:
                cur = conn.cursor()
                cur.execute("""
                    UPDATE upload_batches
                    SET processing_status = 'failed', error_message = %s, completed_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (error_msg, batch_id))
                conn.commit()
        except:
            pass

        dup_df = pd.DataFrame(duplicates_details) if duplicates_details else None
        return records_inserted, duplicates_skipped, error_msg, dup_df

    finally:
        if conn:
            conn.close()


def load_user_reconciliation(user_id, record_type: Optional[str] = None) -> pd.DataFrame:
    """
    Load reconciliation records for a user from database.

    Args:
        user_id: User identifier (int or str)
        record_type: Optional filter for 'IN' or 'OUT', None loads both

    Returns:
        DataFrame with reconciliation data
    """
    try:
        # Convert user_id to string for VARCHAR column
        user_id = str(user_id)

        engine = get_db_engine()

        query = """
            SELECT
                counterparty_inn as "Customer_INN",
                counterparty_name as "Original_INN_Column",
                outstanding_amount as "Outstanding_Amount",
                record_type,
                report_date,
                source_filename,
                upload_date
            FROM reconciliation_records
            WHERE user_id = %(user_id)s
        """

        params = {'user_id': user_id}
        if record_type:
            query += " AND record_type = %(record_type)s"
            params['record_type'] = record_type

        query += " ORDER BY report_date DESC, outstanding_amount DESC"

        df = pd.read_sql_query(query, engine, params=params)
        return df

    except Exception as e:
        print(f"Error loading reconciliation records: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# PAYMENT MATCHING UTILITIES
# ============================================================================

def find_payments_for_inn(bank_df: pd.DataFrame, target_inn: str, transaction_type: str = 'incoming', debug: bool = False) -> pd.DataFrame:
    """
    Find payments for a given INN using two-stage matching:
    1. Direct counterparty_inn match
    2. Payment purpose INN reference (for third-party payments)

    This handles scenarios where:
    - Payments made through intermediaries/marketplaces
    - Agent payments on behalf of customer
    - Group company payments
    - Third-party payments with INN reference in purpose

    Args:
        bank_df: Bank transactions DataFrame
        target_inn: INN to search for (will be converted to string)
        transaction_type: 'incoming' (for AR) or 'outgoing' (for AP)
        debug: If True, print debug information

    Returns:
        DataFrame of matching payments (deduplicated)

    Example:
        # Customer 201178674 has payment from intermediary 200933985
        # Payment purpose: "...от ИНН: 201178674..."
        customer_payments = find_payments_for_inn(bank_df, '201178674', 'incoming')
    """
    import re

    if bank_df is None or bank_df.empty:
        if debug:
            print(f"DEBUG: bank_df is None or empty")
        return pd.DataFrame()

    # Convert target INN to string and clean it
    target_inn = str(target_inn).replace('.0', '').strip()

    if not target_inn or target_inn == '' or target_inn == 'nan':
        if debug:
            print(f"DEBUG: Invalid target_inn: {target_inn}")
        return pd.DataFrame()

    if debug:
        print(f"\n=== DEBUG: Searching for INN {target_inn} ({transaction_type}) ===")
        print(f"Bank DataFrame shape: {bank_df.shape}")
        print(f"Bank DataFrame columns: {list(bank_df.columns)}")

    # Stage 1: Direct INN match (current logic)
    direct_match_mask = pd.Series(False, index=bank_df.index)

    if 'inn' in bank_df.columns:
        bank_inn = bank_df['inn'].astype(str).str.replace('.0', '', regex=False).str.strip()
        direct_match_mask |= (bank_inn == target_inn)
        if debug:
            direct_matches = (bank_inn == target_inn).sum()
            print(f"Direct matches via 'inn' column: {direct_matches}")

    if 'Taxpayer ID (INN)' in bank_df.columns:
        bank_inn_alt = bank_df['Taxpayer ID (INN)'].astype(str).str.replace('.0', '', regex=False).str.strip()
        alt_matches_mask = (bank_inn_alt == target_inn)
        direct_match_mask |= alt_matches_mask
        if debug:
            print(f"Direct matches via 'Taxpayer ID (INN)' column: {alt_matches_mask.sum()}")

    if 'counterparty_inn' in bank_df.columns:
        bank_inn_cp = bank_df['counterparty_inn'].astype(str).str.replace('.0', '', regex=False).str.strip()
        cp_matches_mask = (bank_inn_cp == target_inn)
        direct_match_mask |= cp_matches_mask
        if debug:
            print(f"Direct matches via 'counterparty_inn' column: {cp_matches_mask.sum()}")

    if debug:
        print(f"Total direct matches: {direct_match_mask.sum()}")

    # Stage 2: Payment purpose search (for third-party payments)
    purpose_match_mask = pd.Series(False, index=bank_df.index)

    if 'payment_purpose' in bank_df.columns or 'Payment Purpose' in bank_df.columns:
        purpose_col = 'payment_purpose' if 'payment_purpose' in bank_df.columns else 'Payment Purpose'

        if debug:
            print(f"\nSearching in '{purpose_col}' column...")
            # Sample a few payment purposes to see the format
            sample_purposes = bank_df[purpose_col].dropna().head(3).tolist()
            print(f"Sample payment purposes: {sample_purposes[:3]}")

        # Pattern to match INN references in payment purpose
        # Matches: "ИНН: 201178674", "INN: 201178674", "от ИНН 201178674", etc.
        # Uses word boundaries to avoid partial matches
        inn_pattern = r'\b(?:ИНН|INN|инн|inn)[:\s]+' + re.escape(target_inn) + r'\b'

        if debug:
            print(f"Search pattern: {inn_pattern}")

        purpose_match_mask = bank_df[purpose_col].astype(str).str.contains(
            inn_pattern,
            case=False,
            na=False,
            regex=True
        )

        if debug:
            purpose_matches = purpose_match_mask.sum()
            print(f"Payment purpose matches: {purpose_matches}")
            if purpose_matches > 0:
                # Show matching payment purposes
                matching_purposes = bank_df[purpose_match_mask][purpose_col].head(3).tolist()
                print(f"Matching payment purposes: {matching_purposes}")
    else:
        if debug:
            print("No payment purpose column found!")

    # Combine both matching strategies
    combined_mask = direct_match_mask | purpose_match_mask

    if debug:
        print(f"\nTotal matches before amount filter: {combined_mask.sum()}")

    # Filter by transaction type (amount direction)
    if transaction_type == 'incoming':
        # For AR: look for positive amounts (incoming payments)
        if 'amount' in bank_df.columns:
            amount_mask = pd.to_numeric(bank_df['amount'], errors='coerce') > 0
            if debug:
                print(f"Filtering by positive 'amount': {amount_mask.sum()} rows")
            combined_mask &= amount_mask
        elif 'Credit Turnover' in bank_df.columns:
            credit_mask = pd.to_numeric(bank_df['Credit Turnover'], errors='coerce') > 0
            if debug:
                print(f"Filtering by positive 'Credit Turnover': {credit_mask.sum()} rows")
            combined_mask &= credit_mask
        else:
            if debug:
                print("WARNING: No amount or Credit Turnover column found for filtering!")
    elif transaction_type == 'outgoing':
        # For AP: look for negative amounts (outgoing payments)
        if 'amount' in bank_df.columns:
            amount_mask = pd.to_numeric(bank_df['amount'], errors='coerce') < 0
            if debug:
                print(f"Filtering by negative 'amount': {amount_mask.sum()} rows")
            combined_mask &= amount_mask
        elif 'Debit Turnover' in bank_df.columns:
            debit_mask = pd.to_numeric(bank_df['Debit Turnover'], errors='coerce') > 0
            if debug:
                print(f"Filtering by positive 'Debit Turnover': {debit_mask.sum()} rows")
            combined_mask &= debit_mask
        else:
            if debug:
                print("WARNING: No amount or Debit Turnover column found for filtering!")

    if debug:
        final_matches = combined_mask.sum()
        print(f"\n=== FINAL RESULT: {final_matches} matching payments ===")
        if final_matches > 0:
            print(f"Total amount: {bank_df[combined_mask]['amount'].sum() if 'amount' in bank_df.columns else 'N/A'}")

    # Return matching payments (already deduplicated by using combined mask)
    return bank_df[combined_mask].copy()


# ============================================================================
# COMBINED DATA FUNCTIONS (with Reconciliation)
# ============================================================================

def get_ar_with_reconciliation(user_id) -> dict:
    """
    Get all data needed for Accounts Receivable analysis including reconciliation.

    Args:
        user_id: User identifier (int or str)

    Returns:
        Dictionary with invoices_out, bank_transactions, and reconciliation_out DataFrames
    """
    user_id = str(user_id)

    return {
        'invoices_out': load_user_invoices(user_id, invoice_type='OUT'),
        'bank_transactions': load_user_bank_transactions(user_id),
        'reconciliation_out': load_user_reconciliation(user_id, record_type='OUT')
    }


def get_ap_with_reconciliation(user_id) -> dict:
    """
    Get all data needed for Accounts Payable analysis including reconciliation.

    Args:
        user_id: User identifier (int or str)

    Returns:
        Dictionary with invoices_in, bank_transactions, and reconciliation_in DataFrames
    """
    user_id = str(user_id)

    return {
        'invoices_in': load_user_invoices(user_id, invoice_type='IN'),
        'bank_transactions': load_user_bank_transactions(user_id),
        'reconciliation_in': load_user_reconciliation(user_id, record_type='IN')
    }


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_upload_history(user_id, limit: int = 50) -> pd.DataFrame:
    """
    Get upload history for a user.

    Args:
        user_id: User identifier (int or str)
        limit: Maximum number of records to return

    Returns:
        DataFrame with upload history
    """
    try:
        # Convert user_id to string for VARCHAR column
        user_id = str(user_id)

        engine = get_db_engine()

        query = """
            SELECT
                filename,
                data_type,
                records_count,
                duplicates_skipped,
                errors_count,
                processing_status,
                upload_date,
                completed_at
            FROM upload_batches
            WHERE user_id = %(user_id)s
            ORDER BY upload_date DESC
            LIMIT %(limit)s
        """

        df = pd.read_sql_query(query, engine, params={'user_id': user_id, 'limit': limit})
        return df

    except Exception as e:
        print(f"Error loading upload history: {str(e)}")
        return pd.DataFrame()


def delete_user_data(user_id: str, data_type: Optional[str] = None) -> Tuple[bool, Optional[str]]:
    """
    Delete all data for a user (or specific data type).

    Args:
        user_id: User identifier
        data_type: Optional filter for specific data type

    Returns:
        (success, error_message)
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        user_id_str = str(user_id)

        # if data_type:
        #     # Delete specific type
        #     if data_type in ['invoice_in', 'invoice_out']:
        #         invoice_type = 'IN' if data_type == 'invoice_in' else 'OUT'
        #         cur.execute("DELETE FROM invoices WHERE user_id = %s AND invoice_type = %s", (user_id_str, invoice_type))
        #     elif data_type == 'bank_statement':
        #         cur.execute("DELETE FROM bank_transactions WHERE user_id = %s", (user_id_str,))
        #     elif data_type in ['reconciliation_ar', 'reconciliation_ap']:
        #         record_type = 'AR' if data_type == 'reconciliation_ar' else 'AP'
        #         cur.execute("DELETE FROM reconciliation_records WHERE user_id = %s AND record_type = %s", (user_id_str, record_type))
        # else:
            # Delete all data for user
        cur.execute("DELETE FROM invoice_items WHERE user_id = %s", (user_id_str,))
        cur.execute("DELETE FROM invoices WHERE user_id = %s", (user_id_str,))
        cur.execute("DELETE FROM bank_transactions WHERE user_id = %s", (user_id_str,))
        cur.execute("DELETE FROM reconciliation_records WHERE user_id = %s", (user_id_str,))
        cur.execute("DELETE FROM upload_batches WHERE user_id = %s", (user_id_str,))

        conn.commit()
        return True, None

    except Exception as e:
        if conn:
            conn.rollback()
        return False, f"Error deleting data: {str(e)}"
    finally:
        if conn:
            conn.close()


# ============================================================================
# PD/ECL MODEL - CREDIT RISK FUNCTIONS
# ============================================================================

def get_monthly_aging_summary(user_id, use_current_date: bool = True) -> pd.DataFrame:
    """
    Get monthly aging buckets for PD/ECL model.

    IMPORTANT: For roll-rate analysis, this creates month-end snapshots showing
    how invoices aged AT THAT POINT IN TIME, not based on current date.

    Returns monthly exposure amounts grouped by aging buckets:
    - not_aged: 0-30 days past invoice date (as of month-end)
    - dpd_0_30: 31-60 days past invoice date (as of month-end)
    - dpd_31_60: 61-90 days past invoice date (as of month-end)
    - default: 90+ days past invoice date (as of month-end)

    Args:
        user_id: User identifier (int or str)
        use_current_date: If True, age from current date; if False, use month-end snapshots

    Returns:
        DataFrame with columns: date, not_aged, dpd_0_30, dpd_31_60, default
    """
    try:
        user_id = str(user_id)
        engine = get_db_engine()

        # Get date range from data
        date_query = """
        SELECT
            MIN(document_date) as min_date,
            MAX(document_date) as max_date
        FROM invoices
        WHERE user_id = %(user_id)s
          AND invoice_type = 'OUT'
          AND status IN ('Подписан', 'Signed')
        """
        date_df = pd.read_sql_query(date_query, engine, params={'user_id': user_id})

        if date_df.empty or pd.isna(date_df.iloc[0]['min_date']):
            return pd.DataFrame()

        start_date = pd.to_datetime(date_df.iloc[0]['min_date'])
        end_date = datetime.now()

        # Generate month-end dates for snapshots
        date_range = pd.date_range(start=start_date, end=end_date, freq='ME')

        results = []

        for snapshot_date in date_range:
            # For each month-end, calculate aging buckets as of that date
            query = """
            SELECT
                %(snapshot_date)s::date as date,

                -- Not Aged: 0-30 days old AS OF SNAPSHOT DATE
                COALESCE(SUM(CASE
                    WHEN (%(snapshot_date)s::date - document_date) <= 30 THEN total_amount
                    ELSE 0
                END), 0) as not_aged,

                -- 0-30 DPD: 31-60 days old AS OF SNAPSHOT DATE
                COALESCE(SUM(CASE
                    WHEN (%(snapshot_date)s::date - document_date) BETWEEN 31 AND 60 THEN total_amount
                    ELSE 0
                END), 0) as dpd_0_30,

                -- 31-60 DPD: 61-90 days old AS OF SNAPSHOT DATE
                COALESCE(SUM(CASE
                    WHEN (%(snapshot_date)s::date - document_date) BETWEEN 61 AND 90 THEN total_amount
                    ELSE 0
                END), 0) as dpd_31_60,

                -- Default: 90+ days old AS OF SNAPSHOT DATE
                COALESCE(SUM(CASE
                    WHEN (%(snapshot_date)s::date - document_date) > 90 THEN total_amount
                    ELSE 0
                END), 0) as default

            FROM invoices
            WHERE user_id = %(user_id)s
              AND invoice_type = 'OUT'
              AND status IN ('Подписан', 'Signed')
              AND total_amount IS NOT NULL
              AND total_amount > 0
              AND document_date <= %(snapshot_date)s::date  -- Only invoices existing at snapshot
            """

            snapshot_df = pd.read_sql_query(query, engine, params={
                'user_id': user_id,
                'snapshot_date': snapshot_date
            })

            if not snapshot_df.empty:
                results.append(snapshot_df)

        if results:
            df = pd.concat(results, ignore_index=True)
            df['date'] = pd.to_datetime(df['date'])
            return df
        else:
            return pd.DataFrame()

    except Exception as e:
        print(f"Error getting monthly aging summary: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def get_customer_level_aging(user_id, snapshot_date: Optional[datetime] = None) -> pd.DataFrame:
    """
    Get customer-level aging analysis for PD/ECL model.

    Returns outstanding amounts per customer grouped by aging buckets.
    This allows calculating PD at the customer level.

    Args:
        user_id: User identifier (int or str)
        snapshot_date: Optional snapshot date (default: current date)

    Returns:
        DataFrame with columns:
        - customer_inn: Customer tax ID
        - customer_name: Customer name
        - not_aged: Exposure 0-30 days
        - dpd_0_30: Exposure 31-60 days
        - dpd_31_60: Exposure 61-90 days
        - default: Exposure 90+ days
        - total_exposure: Total exposure
        - invoice_count: Number of invoices
    """
    try:
        user_id = str(user_id)
        engine = get_db_engine()

        # Use current date if snapshot_date not provided
        if snapshot_date is None:
            snapshot_date = datetime.now()

        query = """
        SELECT
            buyer_inn as customer_inn,
            buyer_name as customer_name,

            -- Not Aged: 0-30 days old
            COALESCE(SUM(CASE
                WHEN (%(snapshot_date)s::date - document_date) <= 30 THEN total_amount
                ELSE 0
            END), 0) as not_aged,

            -- 0-30 DPD: 31-60 days old
            COALESCE(SUM(CASE
                WHEN (%(snapshot_date)s::date - document_date) BETWEEN 31 AND 60 THEN total_amount
                ELSE 0
            END), 0) as dpd_0_30,

            -- 31-60 DPD: 61-90 days old
            COALESCE(SUM(CASE
                WHEN (%(snapshot_date)s::date - document_date) BETWEEN 61 AND 90 THEN total_amount
                ELSE 0
            END), 0) as dpd_31_60,

            -- Default: 90+ days old
            COALESCE(SUM(CASE
                WHEN (%(snapshot_date)s::date - document_date) > 90 THEN total_amount
                ELSE 0
            END), 0) as default,

            -- Total exposure
            COALESCE(SUM(total_amount), 0) as total_exposure,

            -- Invoice count
            COUNT(*) as invoice_count

        FROM invoices
        WHERE user_id = %(user_id)s
          AND invoice_type = 'OUT'
          AND status IN ('Подписан', 'Signed')
          AND total_amount IS NOT NULL
          AND total_amount > 0
          AND buyer_inn IS NOT NULL
        GROUP BY buyer_inn, buyer_name
        HAVING SUM(total_amount) > 0
        ORDER BY total_exposure DESC
        """

        df = pd.read_sql_query(query, engine, params={
            'user_id': user_id,
            'snapshot_date': snapshot_date
        })

        return df

    except Exception as e:
        print(f"Error getting customer-level aging: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def get_aging_time_series(user_id, start_date: Optional[datetime] = None,
                          end_date: Optional[datetime] = None,
                          frequency: str = 'ME') -> pd.DataFrame:
    """
    Get time series of aging buckets for multiple periods.

    This creates monthly snapshots showing how exposure aged over time,
    which is essential for calculating roll-rates.

    Args:
        user_id: User identifier (int or str)
        start_date: Start date for time series (default: earliest invoice)
        end_date: End date for time series (default: current date)
        frequency: Pandas frequency string ('ME' = month end, 'QE' = quarter end)

    Returns:
        DataFrame with columns: snapshot_date, not_aged, dpd_0_30, dpd_31_60, default
    """
    try:
        user_id = str(user_id)
        engine = get_db_engine()

        # Get date range from data if not provided
        if start_date is None or end_date is None:
            date_query = """
            SELECT
                MIN(document_date) as min_date,
                MAX(document_date) as max_date
            FROM invoices
            WHERE user_id = %(user_id)s
              AND invoice_type = 'OUT'
              AND status IN ('Подписан', 'Signed')
            """
            date_df = pd.read_sql_query(date_query, engine, params={'user_id': user_id})

            if date_df.empty or pd.isna(date_df.iloc[0]['min_date']):
                return pd.DataFrame()

            if start_date is None:
                start_date = pd.to_datetime(date_df.iloc[0]['min_date'])
            if end_date is None:
                end_date = datetime.now()

        # Generate date range for snapshots
        date_range = pd.date_range(start=start_date, end=end_date, freq=frequency)

        results = []

        for snapshot_date in date_range:
            # Query aging buckets as of this snapshot date
            query = """
            SELECT
                %(snapshot_date)s::date as snapshot_date,

                -- Not Aged: 0-30 days old
                COALESCE(SUM(CASE
                    WHEN (%(snapshot_date)s::date - document_date) <= 30 THEN total_amount
                    ELSE 0
                END), 0) as not_aged,

                -- 0-30 DPD: 31-60 days old
                COALESCE(SUM(CASE
                    WHEN (%(snapshot_date)s::date - document_date) BETWEEN 31 AND 60 THEN total_amount
                    ELSE 0
                END), 0) as dpd_0_30,

                -- 31-60 DPD: 61-90 days old
                COALESCE(SUM(CASE
                    WHEN (%(snapshot_date)s::date - document_date) BETWEEN 61 AND 90 THEN total_amount
                    ELSE 0
                END), 0) as dpd_31_60,

                -- Default: 90+ days old
                COALESCE(SUM(CASE
                    WHEN (%(snapshot_date)s::date - document_date) > 90 THEN total_amount
                    ELSE 0
                END), 0) as default

            FROM invoices
            WHERE user_id = %(user_id)s
              AND invoice_type = 'OUT'
              AND status IN ('Подписан', 'Signed')
              AND total_amount IS NOT NULL
              AND total_amount > 0
              AND document_date <= %(snapshot_date)s::date
            """

            snapshot_df = pd.read_sql_query(query, engine, params={
                'user_id': user_id,
                'snapshot_date': snapshot_date
            })

            if not snapshot_df.empty:
                results.append(snapshot_df)

        if results:
            df = pd.concat(results, ignore_index=True)
            df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
            # Rename to 'date' for compatibility with risk_model
            df = df.rename(columns={'snapshot_date': 'date'})
            return df
        else:
            return pd.DataFrame()

    except Exception as e:
        print(f"Error getting aging time series: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def get_data_diagnostics(user_id) -> Dict[str, any]:
    """
    Get diagnostic information about invoice data for debugging.

    Args:
        user_id: User identifier (int or str)

    Returns:
        Dictionary with diagnostic metrics:
        - total_invoices: Total count of invoices
        - total_customers: Unique customer count
        - distinct_months: Number of distinct months with data
        - date_range: Tuple of (earliest_date, latest_date)
        - total_exposure: Sum of all invoice amounts
        - by_status: Breakdown by invoice status
    """
    try:
        user_id = str(user_id)
        engine = get_db_engine()

        # Overall statistics
        query = """
        SELECT
            COUNT(*) as total_invoices,
            COUNT(DISTINCT buyer_inn) as total_customers,
            COUNT(DISTINCT DATE_TRUNC('month', document_date)) as distinct_months,
            MIN(document_date) as earliest_date,
            MAX(document_date) as latest_date,
            COALESCE(SUM(total_amount), 0) as total_exposure
        FROM invoices
        WHERE user_id = %(user_id)s
          AND invoice_type = 'OUT'
        """
        overall_df = pd.read_sql_query(query, engine, params={'user_id': user_id})

        # Status breakdown
        status_query = """
        SELECT
            status,
            COUNT(*) as invoice_count,
            COALESCE(SUM(total_amount), 0) as total_amount
        FROM invoices
        WHERE user_id = %(user_id)s
          AND invoice_type = 'OUT'
        GROUP BY status
        ORDER BY invoice_count DESC
        """
        status_df = pd.read_sql_query(status_query, engine, params={'user_id': user_id})

        # Invoices filtered for PD model
        filtered_query = """
        SELECT
            COUNT(*) as filtered_invoices,
            COUNT(DISTINCT DATE_TRUNC('month', document_date)) as filtered_months,
            COALESCE(SUM(total_amount), 0) as filtered_exposure
        FROM invoices
        WHERE user_id = %(user_id)s
          AND invoice_type = 'OUT'
          AND status IN ('Подписан', 'Signed')
          AND total_amount IS NOT NULL
          AND total_amount > 0
        """
        filtered_df = pd.read_sql_query(filtered_query, engine, params={'user_id': user_id})

        return {
            'total_invoices': int(overall_df.iloc[0]['total_invoices']),
            'total_customers': int(overall_df.iloc[0]['total_customers']),
            'distinct_months': int(overall_df.iloc[0]['distinct_months']),
            'date_range': (overall_df.iloc[0]['earliest_date'], overall_df.iloc[0]['latest_date']),
            'total_exposure': float(overall_df.iloc[0]['total_exposure']),
            'by_status': status_df.to_dict('records'),
            'filtered_invoices': int(filtered_df.iloc[0]['filtered_invoices']),
            'filtered_months': int(filtered_df.iloc[0]['filtered_months']),
            'filtered_exposure': float(filtered_df.iloc[0]['filtered_exposure'])
        }

    except Exception as e:
        print(f"Error getting data diagnostics: {str(e)}")
        import traceback
        traceback.print_exc()
        return {}


def get_invoice_level_data_for_ecl(user_id, snapshot_date: Optional[datetime] = None) -> pd.DataFrame:
    """
    Get invoice-level data with aging buckets for ECL calculation.

    This provides the most granular view for ECL modeling.

    Args:
        user_id: User identifier (int or str)
        snapshot_date: Optional snapshot date (default: current date)

    Returns:
        DataFrame with columns:
        - invoice_id: Invoice ID
        - document_number: Invoice number
        - document_date: Invoice date
        - customer_inn: Customer tax ID
        - customer_name: Customer name
        - total_amount: Invoice amount
        - days_past_due: Days since invoice date
        - aging_bucket: Bucket name (not_aged, dpd_0_30, dpd_31_60, default)
    """
    try:
        user_id = str(user_id)
        engine = get_db_engine()

        # Use current date if snapshot_date not provided
        if snapshot_date is None:
            snapshot_date = datetime.now()

        query = """
        SELECT
            id as invoice_id,
            document_number,
            document_date,
            buyer_inn as customer_inn,
            buyer_name as customer_name,
            total_amount,
            (%(snapshot_date)s::date - document_date) as days_past_due,
            CASE
                WHEN (%(snapshot_date)s::date - document_date) <= 30 THEN 'not_aged'
                WHEN (%(snapshot_date)s::date - document_date) BETWEEN 31 AND 60 THEN 'dpd_0_30'
                WHEN (%(snapshot_date)s::date - document_date) BETWEEN 61 AND 90 THEN 'dpd_31_60'
                ELSE 'default'
            END as aging_bucket,
            status,
            contract_number,
            seller_name,
            source_filename

        FROM invoices
        WHERE user_id = %(user_id)s
          AND invoice_type = 'OUT'
          AND status IN ('Подписан', 'Signed')
          AND total_amount IS NOT NULL
          AND total_amount > 0
        ORDER BY days_past_due DESC, total_amount DESC
        """

        df = pd.read_sql_query(query, engine, params={
            'user_id': user_id,
            'snapshot_date': snapshot_date
        })

        df['document_date'] = pd.to_datetime(df['document_date'])

        return df

    except Exception as e:
        print(f"Error getting invoice-level data: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def get_monthly_aging_with_payments(user_id) -> pd.DataFrame:
    """
    Get monthly aging buckets incorporating actual payments received (Money IN).

    This provides a more accurate view for PD/ECL model by:
    1. Calculating outstanding invoices (Accounts Receivable, type='OUT')
    2. Matching with actual payments received (Bank transactions, Money IN)
    3. Computing net exposure = invoices - payments
    4. Aging the net exposure into buckets

    This approach better reflects real credit risk by accounting for:
    - Partial payments
    - Full settlements
    - Overdue amounts that remain unpaid

    Args:
        user_id: User identifier (int or str)

    Returns:
        DataFrame with columns: date, not_aged, dpd_0_30, dpd_31_60, default
        Each row represents a month-end snapshot of net outstanding exposure
    """
    try:
        user_id = str(user_id)
        engine = get_db_engine()

        # Get date range from invoice data
        date_query = """
        SELECT
            MIN(document_date) as min_date,
            MAX(document_date) as max_date
        FROM invoices
        WHERE user_id = %(user_id)s
          AND invoice_type = 'OUT'
          AND status IN ('Подписан', 'Signed')
        """
        date_df = pd.read_sql_query(date_query, engine, params={'user_id': user_id})

        if date_df.empty or pd.isna(date_df.iloc[0]['min_date']):
            return pd.DataFrame()

        start_date = pd.to_datetime(date_df.iloc[0]['min_date'])
        end_date = datetime.now()

        # Generate month-end dates for snapshots
        date_range = pd.date_range(start=start_date, end=end_date, freq='ME')

        results = []

        for snapshot_date in date_range:
            # Step 1: Get all invoices (AR) up to snapshot date, grouped by customer
            invoices_query = """
            SELECT
                buyer_inn as customer_inn,
                SUM(total_amount) as total_invoiced
            FROM invoices
            WHERE user_id = %(user_id)s
              AND invoice_type = 'OUT'
              AND status IN ('Подписан', 'Signed')
              AND total_amount IS NOT NULL
              AND total_amount > 0
              AND document_date <= %(snapshot_date)s::date
            GROUP BY buyer_inn
            """

            invoices_df = pd.read_sql_query(invoices_query, engine, params={
                'user_id': user_id,
                'snapshot_date': snapshot_date
            })

            # Step 2: Get all incoming payments (Money IN) up to snapshot date, grouped by customer
            payments_query = """
            SELECT
                counterparty_inn as customer_inn,
                SUM(ABS(amount)) as total_paid
            FROM bank_transactions
            WHERE user_id = %(user_id)s
              AND transaction_type = 'Incoming'
              AND transaction_date <= %(snapshot_date)s::date
              AND amount IS NOT NULL
              AND amount != 0
            GROUP BY counterparty_inn
            """

            payments_df = pd.read_sql_query(payments_query, engine, params={
                'user_id': user_id,
                'snapshot_date': snapshot_date
            })

            # Step 3: Merge invoices and payments by customer INN
            if not invoices_df.empty:
                # Clean INNs for matching
                invoices_df['customer_inn'] = invoices_df['customer_inn'].astype(str).str.strip()
                if not payments_df.empty:
                    payments_df['customer_inn'] = payments_df['customer_inn'].astype(str).str.strip()
                    merged_df = invoices_df.merge(payments_df, on='customer_inn', how='left')
                else:
                    merged_df = invoices_df.copy()
                    merged_df['total_paid'] = 0

                # Fill NaN payments with 0
                merged_df['total_paid'] = merged_df['total_paid'].fillna(0)

                # Calculate net outstanding = invoiced - paid
                merged_df['net_outstanding'] = merged_df['total_invoiced'] - merged_df['total_paid']

                # Only keep positive net outstanding (actual amounts owed)
                merged_df = merged_df[merged_df['net_outstanding'] > 0]

                # Step 4: Get aging info for each customer's oldest unpaid invoice
                # This determines which aging bucket the net outstanding belongs to
                aging_query = """
                SELECT
                    buyer_inn as customer_inn,
                    MIN(document_date) as oldest_invoice_date
                FROM invoices
                WHERE user_id = %(user_id)s
                  AND invoice_type = 'OUT'
                  AND status IN ('Подписан', 'Signed')
                  AND total_amount IS NOT NULL
                  AND total_amount > 0
                  AND document_date <= %(snapshot_date)s::date
                  AND buyer_inn IN %(customer_inns)s
                GROUP BY buyer_inn
                """

                if not merged_df.empty:
                    customer_inns = tuple(merged_df['customer_inn'].tolist())
                    # Handle edge case: if only one customer, ensure tuple format is correct
                    if len(customer_inns) == 1:
                        customer_inns = (customer_inns[0],)

                    aging_df = pd.read_sql_query(aging_query, engine, params={
                        'user_id': user_id,
                        'snapshot_date': snapshot_date,
                        'customer_inns': customer_inns
                    })

                    aging_df['customer_inn'] = aging_df['customer_inn'].astype(str).str.strip()
                    merged_df = merged_df.merge(aging_df, on='customer_inn', how='left')

                    # Calculate days past due from oldest invoice
                    merged_df['oldest_invoice_date'] = pd.to_datetime(merged_df['oldest_invoice_date'])
                    merged_df['days_past_due'] = (snapshot_date - merged_df['oldest_invoice_date']).dt.days

                    # Assign to aging buckets
                    not_aged = merged_df[merged_df['days_past_due'] <= 30]['net_outstanding'].sum()
                    dpd_0_30 = merged_df[(merged_df['days_past_due'] > 30) & (merged_df['days_past_due'] <= 60)]['net_outstanding'].sum()
                    dpd_31_60 = merged_df[(merged_df['days_past_due'] > 60) & (merged_df['days_past_due'] <= 90)]['net_outstanding'].sum()
                    default = merged_df[merged_df['days_past_due'] > 90]['net_outstanding'].sum()

                    results.append({
                        'date': snapshot_date,
                        'not_aged': not_aged,
                        'dpd_0_30': dpd_0_30,
                        'dpd_31_60': dpd_31_60,
                        'default': default
                    })

        if results:
            df = pd.DataFrame(results)
            df['date'] = pd.to_datetime(df['date'])
            return df
        else:
            return pd.DataFrame()

    except Exception as e:
        print(f"Error getting monthly aging with payments: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
