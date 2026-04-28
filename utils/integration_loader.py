"""
Data loader for integration.invoices schema (CBU sync).
Maps the integration table to the same column format used by the rest of the app.
"""
import pandas as pd
from typing import Optional
from .db_helper import get_db_engine, get_db_connection


def get_all_companies(min_invoices: int = 10) -> pd.DataFrame:
    """List all companies in integration.invoices with invoice counts."""
    engine = get_db_engine()
    query = """
        SELECT tin, MAX(name) as name, SUM(cnt) as total_invoices, SUM(volume) as total_volume
        FROM (
            SELECT seller_tin as tin, seller_name as name,
                   COUNT(*) as cnt, SUM(delivery_sum_with_vat) as volume
            FROM integration.invoices
            WHERE seller_tin IS NOT NULL
            GROUP BY seller_tin, seller_name
            UNION ALL
            SELECT buyer_tin as tin, buyer_name as name,
                   COUNT(*) as cnt, SUM(delivery_sum_with_vat) as volume
            FROM integration.invoices
            WHERE buyer_tin IS NOT NULL
            GROUP BY buyer_tin, buyer_name
        ) t
        GROUP BY tin
        HAVING SUM(cnt) >= %(min_inv)s
        ORDER BY total_invoices DESC
    """
    return pd.read_sql_query(query, engine, params={'min_inv': min_invoices})


def load_raw_invoices(tin: str, invoice_type: Optional[str] = None) -> pd.DataFrame:
    """Load invoices with original column names (no renaming)."""
    if not tin:
        return pd.DataFrame()

    engine = get_db_engine()

    if invoice_type == 'OUT':
        where = "seller_tin = %(tin)s"
    elif invoice_type == 'IN':
        where = "buyer_tin = %(tin)s"
    else:
        where = "(seller_tin = %(tin)s OR buyer_tin = %(tin)s)"

    query = f"""
        SELECT
            id, factura_no, factura_date, contract_no, contract_date,
            seller_tin, seller_name, buyer_tin, buyer_name,
            summa, delivery_sum, vat_sum, delivery_sum_with_vat,
            factoring_status, factoring_request_id,
            synced_at, created_at
        FROM integration.invoices
        WHERE {where}
        ORDER BY factura_date DESC
    """

    return pd.read_sql_query(query, engine, params={'tin': tin})


def load_integration_invoices_by_tin(tin: str, invoice_type: Optional[str] = None) -> pd.DataFrame:
    """
    Load invoices for a specific company TIN (not linked to user).

    Args:
        tin: Company tax ID
        invoice_type: 'IN', 'OUT', or None for both
    """
    if not tin:
        return pd.DataFrame()

    engine = get_db_engine()

    if invoice_type == 'OUT':
        where = "seller_tin = %(tin)s"
    elif invoice_type == 'IN':
        where = "buyer_tin = %(tin)s"
    else:
        where = "(seller_tin = %(tin)s OR buyer_tin = %(tin)s)"

    query = f"""
        SELECT
            factura_no   as "Document Number",
            factura_date as "Document Date",
            CASE WHEN seller_tin = %(tin)s THEN 'OUT' ELSE 'IN' END as invoice_type,
            seller_tin   as "Seller (Tax ID or PINFL)",
            seller_name  as "Seller (Name)",
            buyer_tin    as "Buyer (Tax ID or PINFL)",
            buyer_name   as "Buyer (Name)",
            delivery_sum as "Supply Value",
            vat_sum      as "VAT Amount",
            delivery_sum_with_vat as "Supply Value (incl. VAT)",
            'Подписан' as "Status",
            contract_no   as "Contract Number",
            contract_date as "Contract Date",
            NULL          as product_note,
            external_id   as source_filename,
            synced_at     as upload_date,
            id            as integration_id
        FROM integration.invoices
        WHERE {where}
        ORDER BY factura_date DESC
    """

    df = pd.read_sql_query(query, engine, params={'tin': tin})
    if 'product_note' in df.columns:
        df = df.rename(columns={'product_note': 'Примечание к товару (работе, услуге)'})
    return df


def get_user_company_tin(user_id) -> Optional[str]:
    """Get the company TIN/INN linked to a user."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT company_tin FROM users WHERE id = %s", (int(user_id),))
        row = cur.fetchone()
        conn.close()
        return row[0] if row and row[0] else None
    except Exception as e:
        print(f"Error fetching company_tin: {e}")
        return None


def load_integration_invoices(user_id, invoice_type: Optional[str] = None) -> pd.DataFrame:
    """
    Load invoices from integration.invoices, filtered by the user's company TIN.

    Args:
        user_id: User identifier
        invoice_type: 'IN' (purchases - we are buyer), 'OUT' (sales - we are seller), or None for both

    Returns:
        DataFrame with the same column names as load_user_invoices for compatibility
    """
    company_tin = get_user_company_tin(user_id)
    if not company_tin:
        print(f"No company_tin set for user {user_id}")
        return pd.DataFrame()

    engine = get_db_engine()

    # Determine filter
    if invoice_type == 'OUT':
        # We are the seller
        where = "seller_tin = %(tin)s"
    elif invoice_type == 'IN':
        # We are the buyer
        where = "buyer_tin = %(tin)s"
    else:
        where = "(seller_tin = %(tin)s OR buyer_tin = %(tin)s)"

    query = f"""
        SELECT
            factura_no   as "Document Number",
            factura_date as "Document Date",
            CASE WHEN seller_tin = %(tin)s THEN 'OUT' ELSE 'IN' END as invoice_type,
            seller_tin   as "Seller (Tax ID or PINFL)",
            seller_name  as "Seller (Name)",
            buyer_tin    as "Buyer (Tax ID or PINFL)",
            buyer_name   as "Buyer (Name)",
            delivery_sum as "Supply Value",
            vat_sum      as "VAT Amount",
            delivery_sum_with_vat as "Supply Value (incl. VAT)",
            'Подписан' as "Status",
            contract_no   as "Contract Number",
            contract_date as "Contract Date",
            NULL          as product_note,
            external_id   as source_filename,
            synced_at     as upload_date,
            id            as integration_id
        FROM integration.invoices
        WHERE {where}
        ORDER BY factura_date DESC
    """

    df = pd.read_sql_query(query, engine, params={'tin': company_tin})

    # Add Russian alias for note column for compatibility
    if 'product_note' in df.columns:
        df = df.rename(columns={'product_note': 'Примечание к товару (работе, услуге)'})

    return df


def load_integration_items_by_tin(tin: str, invoice_type: str = 'OUT',
                                   start_date=None, end_date=None) -> pd.DataFrame:
    """
    Load invoice items for a specific company TIN, filtered by direction and date.

    Args:
        tin: Company tax ID
        invoice_type: 'OUT' (products sold) or 'IN' (products bought)
        start_date: optional date filter
        end_date: optional date filter
    """
    if not tin:
        return pd.DataFrame()

    engine = get_db_engine()

    where_parts = []
    if invoice_type == 'OUT':
        where_parts.append("i.seller_tin = %(tin)s")
    elif invoice_type == 'IN':
        where_parts.append("i.buyer_tin = %(tin)s")
    else:
        where_parts.append("(i.seller_tin = %(tin)s OR i.buyer_tin = %(tin)s)")

    params = {'tin': tin}
    if start_date is not None:
        where_parts.append("i.factura_date >= %(start_date)s")
        params['start_date'] = start_date
    if end_date is not None:
        where_parts.append("i.factura_date <= %(end_date)s")
        params['end_date'] = end_date

    where = " AND ".join(where_parts)

    query = f"""
        SELECT
            ii.invoice_id,
            i.factura_no,
            i.factura_date,
            i.buyer_tin,
            i.buyer_name,
            i.seller_tin,
            i.seller_name,
            ii.catalog_code,
            ii.catalog_name,
            ii.package_name,
            ii.amount as quantity,
            ii.price,
            ii.total_sum,
            ii.vat_rate,
            ii.vat_sum,
            ii.final_sum
        FROM integration.invoice_items ii
        JOIN integration.invoices i ON ii.invoice_id = i.id
        WHERE {where}
        ORDER BY i.factura_date DESC
    """

    return pd.read_sql_query(query, engine, params=params)


def load_integration_invoice_items(user_id, invoice_type: Optional[str] = None) -> pd.DataFrame:
    """Load invoice items joined to invoices, filtered by user's company TIN."""
    company_tin = get_user_company_tin(user_id)
    if not company_tin:
        return pd.DataFrame()

    engine = get_db_engine()

    if invoice_type == 'OUT':
        where = "i.seller_tin = %(tin)s"
    elif invoice_type == 'IN':
        where = "i.buyer_tin = %(tin)s"
    else:
        where = "(i.seller_tin = %(tin)s OR i.buyer_tin = %(tin)s)"

    query = f"""
        SELECT
            ii.invoice_id,
            i.factura_no as "Document Number",
            i.factura_date as "Document Date",
            CASE WHEN i.seller_tin = %(tin)s THEN 'OUT' ELSE 'IN' END as invoice_type,
            i.seller_tin as "Seller (Tax ID or PINFL)",
            i.seller_name as "Seller (Name)",
            i.buyer_tin as "Buyer (Tax ID or PINFL)",
            i.buyer_name as "Buyer (Name)",
            ii.catalog_code as "Catalog Code",
            ii.catalog_name as "Product Name",
            ii.amount as "Quantity",
            ii.package_name as "Unit of Measure",
            ii.price as "Unit Price",
            ii.total_sum as "Supply Value",
            ii.vat_rate as "VAT Rate",
            ii.vat_sum as "VAT Amount",
            ii.final_sum as "Total Amount"
        FROM integration.invoice_items ii
        JOIN integration.invoices i ON ii.invoice_id = i.id
        WHERE {where}
        ORDER BY i.factura_date DESC
    """

    return pd.read_sql_query(query, engine, params={'tin': company_tin})
