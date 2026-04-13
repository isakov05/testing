import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import datetime
from datetime import datetime as dt, timedelta as td
import numpy as np
from translations import get_text
from auth.db_authenticator import protect_page
from utils.pagination import paginate_dataframe, render_pagination_controls

st.set_page_config(page_title="Warehouse Analytics", page_icon="📦", layout="wide")

protect_page()


def get_warehouse_data():
    """Get invoice item data for warehouse analysis from database"""
    from utils.db_operations import load_user_invoice_items

    # Get user_id from session state
    user_id = st.session_state.get('user_id')
    if not user_id:
        return {
            'invoices_in': None,
            'invoices_out': None
        }

    # Load invoice ITEMS (line items with product details) from database
    invoices_in_items = load_user_invoice_items(user_id, invoice_type='IN')
    invoices_out_items = load_user_invoice_items(user_id, invoice_type='OUT')

    # Convert date columns to datetime for consistency
    if not invoices_in_items.empty and 'Document Date' in invoices_in_items.columns:
        invoices_in_items['Date'] = pd.to_datetime(invoices_in_items['Document Date'], errors='coerce')
    if not invoices_out_items.empty and 'Document Date' in invoices_out_items.columns:
        invoices_out_items['Date'] = pd.to_datetime(invoices_out_items['Document Date'], errors='coerce')

    return {
        'invoices_in': invoices_in_items if not invoices_in_items.empty else None,
        'invoices_out': invoices_out_items if not invoices_out_items.empty else None
    }


def find_product_column(df):
    """Find the product/item description column"""
    product_columns = [
        'Product Name',  # Database column name
        'Наименование товара', 'Наименование товара, работы, услуги',
        'Item Description',
        'Примечание к товару', 'Description', 'Product', 'Item', 'Товар'
    ]
    for col in product_columns:
        if col in df.columns:
            return col

    # Try partial match
    for col in df.columns:
        col_lower = str(col).lower()
        if any(keyword in col_lower for keyword in ['product', 'товар', 'item', 'name']):
            return col
    return None


def find_quantity_column(df):
    """Find the quantity column"""
    quantity_columns = [
        'Quantity',  # Database column name
        'Количество', 'Кол-во', 'Qty', 'Count'
    ]
    for col in quantity_columns:
        if col in df.columns:
            return col

    # Try partial match
    for col in df.columns:
        col_lower = str(col).lower()
        if any(keyword in col_lower for keyword in ['количество', 'quantity', 'кол-во', 'qty']):
            return col
    return None


def find_amount_column(df):
    """Find the amount/price column"""
    amount_columns = [
        'Total Amount',  # Database column name - line item total
        'Supply Value',  # Database column name - net value
        'Стоимость поставки с учётом НДС', 'Цена', 'Сумма с НДС',
        'amount', 'Amount', 'Сумма', 'Стоимость', 'Cost', 'Total', 'Price'
    ]
    for col in amount_columns:
        if col in df.columns:
            return col

    # Try partial match
    for col in df.columns:
        col_lower = str(col).lower()
        if any(keyword in col_lower for keyword in ['стоимость', 'цена', 'сумма', 'amount', 'price', 'cost', 'total']):
            return col
    return None

def find_unit_price_column(df):
    """Find the unit price column"""
    unit_price_columns = [
        'Unit Price',  # Database column name
        'Цена', 'Price'
    ]
    for col in unit_price_columns:
        if col in df.columns:
            return col

    # Try partial match
    for col in df.columns:
        col_lower = str(col).lower()
        if 'unit' in col_lower and ('price' in col_lower or 'цена' in col_lower):
            return col
    return None

def find_date_column(df):
    """Find the date column"""
    date_columns = [
        'Date',  # Standardized column name we create
        'Document Date',  # Database column name
        'Дата документ', 'date'
    ]
    for col in date_columns:
        if col in df.columns:
            return col
    return None


def calculate_inventory_status(invoices_in_df, invoices_out_df):
    """Calculate current inventory status from invoices"""
    if invoices_in_df is None or invoices_in_df.empty:
        st.error(get_text('warehouse_no_incoming_warning', st.session_state.get('language', 'en')))
        return pd.DataFrame()

    # Get column names
    product_col_in = find_product_column(invoices_in_df)
    qty_col_in = find_quantity_column(invoices_in_df)
    amount_col_in = find_amount_column(invoices_in_df)
    unit_price_col_in = find_unit_price_column(invoices_in_df)
    date_col_in = find_date_column(invoices_in_df)

    # Debug: Show which columns were found
    if not all([product_col_in, qty_col_in, amount_col_in, unit_price_col_in]):
        st.error("Missing required columns in invoices_in:")
        st.write(f"- Product column: {product_col_in or 'NOT FOUND'}")
        st.write(f"- Quantity column: {qty_col_in or 'NOT FOUND'}")
        st.write(f"- Amount column: {amount_col_in or 'NOT FOUND'}")
        st.write(f"- Unit price column: {unit_price_col_in or 'NOT FOUND'}")
        st.write("Available columns:", list(invoices_in_df.columns))
        return pd.DataFrame()

    # Process incoming inventory (purchases)
    purchases = invoices_in_df.copy()
    purchases['product'] = purchases[product_col_in].astype(str).str.strip()
    purchases['quantity'] = pd.to_numeric(purchases[qty_col_in], errors='coerce').fillna(0)
    purchases['amount'] = pd.to_numeric(purchases[amount_col_in], errors='coerce').fillna(0)
    purchases['unit_price'] = pd.to_numeric(purchases[unit_price_col_in], errors='coerce').fillna(0)
    if date_col_in:
        purchases['date'] = pd.to_datetime(purchases[date_col_in], errors='coerce')

    # Filter for signed invoices
    if 'СТАТУС' in purchases.columns:
        purchases = purchases[purchases['СТАТУС'] == 'Подписан']

    # Aggregate purchases by product
    purchases_agg = purchases.groupby('product').agg({
        'quantity': 'sum',
        'amount': 'sum',
        'unit_price': 'mean' 
    }).reset_index()
    purchases_agg.columns = ['product', 'purchased_qty', 'purchase_value', 'unit_price']

    # Process outgoing inventory (sales)
    sales_agg = pd.DataFrame(columns=['product', 'sold_qty', 'sales_value', 'unit_price'])
    if invoices_out_df is not None and not invoices_out_df.empty:
        product_col_out = find_product_column(invoices_out_df)
        qty_col_out = find_quantity_column(invoices_out_df)
        amount_col_out = find_amount_column(invoices_out_df)
        unit_price_col_out = find_unit_price_column(invoices_out_df)
        if all([product_col_out, qty_col_out, amount_col_out]):
            sales = invoices_out_df.copy()
            sales['product'] = sales[product_col_out].astype(str).str.strip()
            sales['quantity'] = pd.to_numeric(sales[qty_col_out], errors='coerce').fillna(0)
            sales['amount'] = pd.to_numeric(sales[amount_col_out], errors='coerce').fillna(0)
            sales['unit_price'] = pd.to_numeric(sales[unit_price_col_out], errors='coerce').fillna(0)
            # Filter for signed invoices
            if 'СТАТУС' in sales.columns:
                sales = sales[sales['СТАТУС'] == 'Подписан']

            sales_agg = sales.groupby('product').agg({
                'quantity': 'sum',
                'amount': 'sum',
                'unit_price': 'mean'
            }).reset_index()
            sales_agg.columns = ['product', 'sold_qty', 'sales_value', 'unit_price']
    
    print(sales_agg)

    # Merge purchases and sales
    inventory = purchases_agg.merge(sales_agg, on='product', how='left')
    inventory['sold_qty'] = inventory['sold_qty'].fillna(0)
    inventory['sales_value'] = inventory['sales_value'].fillna(0)
    # Calculate current stock
    inventory['current_stock'] = inventory['purchased_qty'] - inventory['sold_qty']
    inventory['stock_value'] = inventory['purchase_value'] * (inventory['current_stock'] / inventory['purchased_qty'].replace(0, 1))
    inventory['stock_value'] = inventory['stock_value'].fillna(0)

    # Calculate metrics
    inventory['turnover_ratio'] = inventory['sold_qty'] / inventory['purchased_qty'].replace(0, 1)
    inventory['avg_unit_cost'] = inventory['purchase_value'] / inventory['purchased_qty'].replace(0, 1)

    return inventory

def render_inventory_overview(inventory_df):
    """Render inventory status overview"""
    st.subheader(get_text('inv_overview_header', st.session_state.get('language', 'en')))

    if inventory_df.empty:
        st.info(get_text('no_inventory_data', st.session_state.get('language', 'en')))
        return

    # Threshold configuration
    with st.expander(get_text('configure_thresholds', st.session_state.get('language', 'en'))):
        thresh_col1, thresh_col2 = st.columns(2)
        with thresh_col1:
            low_threshold = st.number_input(
                get_text('low_stock_threshold', st.session_state.get('language', 'en')),
                min_value=1,
                max_value=1000,
                value=10,
                help=get_text('low_stock_help', st.session_state.get('language', 'en'))
            )
        with thresh_col2:
            high_threshold = st.number_input(
                get_text('overstock_threshold', st.session_state.get('language', 'en')),
                min_value=low_threshold + 1,
                max_value=10000,
                value=100,
                help=get_text('overstock_help', st.session_state.get('language', 'en'))
            )

    # KPIs
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_products = len(inventory_df)
        st.metric(get_text('metric_total_products', st.session_state.get('language', 'en')), f"{total_products:,}")

    with col2:
        total_stock = inventory_df['current_stock'].sum()
        st.metric(get_text('metric_total_stock_units', st.session_state.get('language', 'en')), f"{total_stock:,.0f}")

    with col3:
        total_value = inventory_df['stock_value'].sum()
        st.metric(get_text('metric_total_stock_value', st.session_state.get('language', 'en')), f"{total_value:,.2f}")

    with col4:
        avg_turnover = inventory_df['turnover_ratio'].mean()
        st.metric(get_text('metric_avg_turnover_ratio', st.session_state.get('language', 'en')), f"{avg_turnover:.2f}")

    st.markdown("---")

    # Stock status breakdown
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### " + get_text('stock_status_header', st.session_state.get('language', 'en')))
        low_stock = inventory_df[inventory_df['current_stock'] < low_threshold]
        overstock = inventory_df[inventory_df['current_stock'] > high_threshold]
        normal_stock = inventory_df[(inventory_df['current_stock'] >= low_threshold) & (inventory_df['current_stock'] <= high_threshold)]

        status_data = pd.DataFrame({
            'Status': [
                get_text('stock_status_low_tpl', st.session_state.get('language', 'en')).format(low_threshold),
                get_text('stock_status_normal', st.session_state.get('language', 'en')),
                get_text('stock_status_over_tpl', st.session_state.get('language', 'en')).format(high_threshold)
            ],
            'Count': [len(low_stock), len(normal_stock), len(overstock)]
        })

        fig = px.pie(
            status_data,
            values='Count',
            names='Status',
            title=get_text('stock_status_distribution_title', st.session_state.get('language', 'en')),
            color='Status',
            color_discrete_map={
                get_text('stock_status_low_tpl', st.session_state.get('language', 'en')).format(low_threshold): '#FF6B6B',
                get_text('stock_status_normal', st.session_state.get('language', 'en')): '#00D4AA',
                get_text('stock_status_over_tpl', st.session_state.get('language', 'en')).format(high_threshold): '#FFA500'
            }
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### " + get_text('top_products_by_stock_value', st.session_state.get('language', 'en')))
        top_products = inventory_df.nlargest(10, 'stock_value')[['product', 'stock_value', 'current_stock']].copy()

        fig = px.bar(
            top_products,
            x='stock_value',
            y='product',
            orientation='h',
            title=get_text('stock_value_by_product_title', st.session_state.get('language', 'en')),
            labels={'stock_value': get_text('label_stock_value_currency', st.session_state.get('language', 'en')), 'product': get_text('label_product', st.session_state.get('language', 'en'))},
            color='stock_value',
            color_continuous_scale='Greens'
        )
        fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)

        # Add copyable list below chart
        with st.expander(get_text('copy_product_names', st.session_state.get('language', 'en'))):
            for idx, row in top_products.iterrows():
                st.text(row['product'])

    # Detailed inventory table
    st.markdown("---")
    st.markdown("### " + get_text('detailed_inventory_report', st.session_state.get('language', 'en')))

    # Add search/filter
    search_term = st.text_input(get_text('search_products', st.session_state.get('language', 'en')), placeholder=get_text('search_products_ph', st.session_state.get('language', 'en')))

    # Filter inventory based on search
    display_df = inventory_df.copy()
    if search_term:
        display_df = display_df[display_df['product'].str.contains(search_term, case=False, na=False)]

    # Select and format columns for display
    table_df = display_df[[
        'product',
        'purchased_qty',
        'sold_qty',
        'current_stock',
        'purchase_value',
        'sales_value',
        'stock_value',
        'turnover_ratio',
        'avg_unit_cost'
    ]].copy()

    # Rename columns for better display
    table_df.columns = [
        get_text('col_product', st.session_state.get('language', 'en')),
        get_text('col_purchased_qty', st.session_state.get('language', 'en')),
        get_text('col_sold_qty', st.session_state.get('language', 'en')),
        get_text('col_current_stock', st.session_state.get('language', 'en')),
        get_text('col_purchase_value', st.session_state.get('language', 'en')),
        get_text('col_sales_value', st.session_state.get('language', 'en')),
        get_text('col_stock_value', st.session_state.get('language', 'en')),
        get_text('col_turnover_ratio', st.session_state.get('language', 'en')),
        get_text('col_avg_unit_cost', st.session_state.get('language', 'en'))
    ]

    # Apply pagination (100 records per page) before formatting
    page_df, pagination_info = paginate_dataframe(table_df, page_size=100, key_prefix="inventory_detail")

    # Format numeric columns
    page_df['Purchased Qty'] = page_df['Purchased Qty'].apply(lambda x: f"{x:,.0f}")
    page_df['Sold Qty'] = page_df['Sold Qty'].apply(lambda x: f"{x:,.0f}")
    page_df['Current Stock'] = page_df['Current Stock'].apply(lambda x: f"{x:,.0f}")
    page_df[get_text('col_purchase_value', st.session_state.get('language', 'en'))] = page_df[get_text('col_purchase_value', st.session_state.get('language', 'en'))].apply(lambda x: f"{x:,.2f}")
    page_df[get_text('col_sales_value', st.session_state.get('language', 'en'))] = page_df[get_text('col_sales_value', st.session_state.get('language', 'en'))].apply(lambda x: f"{x:,.2f}")
    page_df[get_text('col_stock_value', st.session_state.get('language', 'en'))] = page_df[get_text('col_stock_value', st.session_state.get('language', 'en'))].apply(lambda x: f"{x:,.2f}")
    page_df['Turnover Ratio'] = page_df['Turnover Ratio'].apply(lambda x: f"{x:.2f}")
    page_df[get_text('col_avg_unit_cost', st.session_state.get('language', 'en'))] = page_df[get_text('col_avg_unit_cost', st.session_state.get('language', 'en'))].apply(lambda x: f"{x:,.2f}")

    st.dataframe(
        page_df,
        use_container_width=True
    )

    # Render pagination controls at the bottom
    render_pagination_controls(pagination_info, key_prefix="inventory_detail")

    # Download button
    csv = display_df.to_csv(index=False)
    st.download_button(
        label=get_text('download_inventory_csv', st.session_state.get('language', 'en')),
        data=csv,
        file_name=f"inventory_report_{dt.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )

def get_detailed_product_transactions(invoices_in_df, invoices_out_df):
    """Get detailed transaction-level data for all products"""
    transactions = []

    # Process incoming invoices (purchases)
    if invoices_in_df is not None and not invoices_in_df.empty:
        product_col = find_product_column(invoices_in_df)
        qty_col = find_quantity_column(invoices_in_df)
        amount_col = find_amount_column(invoices_in_df)
        unit_price_col = find_unit_price_column(invoices_in_df)
        date_col = find_date_column(invoices_in_df)

        if all([product_col, qty_col, amount_col, unit_price_col]):
            df = invoices_in_df.copy()

            # Filter for signed invoices
            if 'СТАТУС' in df.columns:
                df = df[df['СТАТУС'] == 'Подписан']

            for _, row in df.iterrows():
                product = str(row[product_col]).strip()
                qty = pd.to_numeric(row[qty_col], errors='coerce')
                amount = pd.to_numeric(row[amount_col], errors='coerce')
                unit_price = pd.to_numeric(row[unit_price_col], errors='coerce')
                date = pd.to_datetime(row[date_col], errors='coerce') if date_col else None
                invoice_num = row.get('Номер документ', row.get('Document Number', ''))

                if pd.notna(qty) and pd.notna(amount):
                    transactions.append({
                        'Product': product,
                        'Type': 'Purchase',
                        'Invoice Number': invoice_num,
                        'Date': date,
                        'Quantity': qty,
                        'Amount': amount,
                        'Unit Price': unit_price
                    })

    # Process outgoing invoices (sales)
    if invoices_out_df is not None and not invoices_out_df.empty:
        product_col = find_product_column(invoices_out_df)
        qty_col = find_quantity_column(invoices_out_df)
        amount_col = find_amount_column(invoices_out_df)
        date_col = find_date_column(invoices_out_df)

        if all([product_col, qty_col, amount_col]):
            df = invoices_out_df.copy()

            # Filter for signed invoices
            if 'СТАТУС' in df.columns:
                df = df[df['СТАТУС'] == 'Подписан']

            for _, row in df.iterrows():
                product = str(row[product_col]).strip()
                qty = pd.to_numeric(row[qty_col], errors='coerce')
                amount = pd.to_numeric(row[amount_col], errors='coerce')
                date = pd.to_datetime(row[date_col], errors='coerce') if date_col else None
                invoice_num = row.get('Номер документ', row.get('Document Number', ''))

                if pd.notna(qty) and pd.notna(amount):
                    transactions.append({
                        'Product': product,
                        'Type': 'Sale',
                        'Invoice Number': invoice_num,
                        'Date': date,
                        'Quantity': -qty,  # Negative for sales
                        'Amount': amount,
                        'Unit Price': amount / qty if qty != 0 else 0
                    })

    if not transactions:
        return pd.DataFrame()

    df = pd.DataFrame(transactions)
    df = df.sort_values(['Product', 'Date'], ascending=[True, False])
    return df


def render_detailed_transactions(invoices_in_df, invoices_out_df):
    """Render detailed product transactions table"""
    st.subheader(get_text('detailed_transactions_header', st.session_state.get('language', 'en')))

    transactions_df = get_detailed_product_transactions(invoices_in_df, invoices_out_df)

    if transactions_df.empty:
        st.info(get_text('no_transaction_data', st.session_state.get('language', 'en')))
        return

    # Filters
    col1, col2, col3 = st.columns(3)

    with col1:
        # Product filter
        all_products = [get_text('type_all', st.session_state.get('language', 'en'))] + sorted(transactions_df['Product'].unique().tolist())
        selected_product = st.selectbox(
            get_text('filter_by_product', st.session_state.get('language', 'en')),
            options=all_products,
            key="product_filter"
        )

    with col2:
        # Transaction type filter
        transaction_types = [
            get_text('type_all', st.session_state.get('language', 'en')),
            get_text('type_purchase', st.session_state.get('language', 'en')),
            get_text('type_sale', st.session_state.get('language', 'en')),
        ]
        selected_type = st.selectbox(
            get_text('filter_by_type', st.session_state.get('language', 'en')),
            options=transaction_types,
            key="type_filter"
        )

    with col3:
        # Date range
        if 'Date' in transactions_df.columns and transactions_df['Date'].notna().any():
            # Handle both datetime and date objects
            import datetime
            import pandas as pd
            min_val = transactions_df['Date'].min()
            max_val = transactions_df['Date'].max()

            # Convert to date objects, handling Timestamp specifically
            if isinstance(min_val, pd.Timestamp):
                min_date = min_val.date()
            elif hasattr(min_val, 'date') and not isinstance(min_val, datetime.date):
                min_date = min_val.date()
            else:
                min_date = min_val

            if isinstance(max_val, pd.Timestamp):
                max_date = max_val.date()
            elif hasattr(max_val, 'date') and not isinstance(max_val, datetime.date):
                max_date = max_val.date()
            else:
                max_date = max_val

            date_range = st.date_input(
                get_text('date_range_label', st.session_state.get('language', 'en')),
                value=(min_date, max_date),
                key="transaction_date_range"
            )

    # Apply filters
    filtered_df = transactions_df.copy()

    if selected_product != 'All':
        filtered_df = filtered_df[filtered_df['Product'] == selected_product]

    if selected_type != 'All':
        filtered_df = filtered_df[filtered_df['Type'] == selected_type]

    if 'Date' in filtered_df.columns and len(date_range) == 2:
        start_date, end_date = date_range
        filtered_df = filtered_df[
            (filtered_df['Date'].dt.date >= start_date) &
            (filtered_df['Date'].dt.date <= end_date)
        ]

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_purchases = len(filtered_df[filtered_df['Type'] == 'Purchase'])
        st.metric(get_text('metric_total_purchases', st.session_state.get('language', 'en')), f"{total_purchases:,}")

    with col2:
        total_sales = len(filtered_df[filtered_df['Type'] == 'Sale'])
        st.metric(get_text('metric_total_sales', st.session_state.get('language', 'en')), f"{total_sales:,}")

    with col3:
        total_purchase_value = filtered_df[filtered_df['Type'] == 'Purchase']['Amount'].sum()
        st.metric(get_text('metric_total_purchase_value', st.session_state.get('language', 'en')), f"{total_purchase_value:,.2f}")

    with col4:
        total_sales_value = filtered_df[filtered_df['Type'] == 'Sale']['Amount'].sum()
        st.metric(get_text('metric_total_sales_value', st.session_state.get('language', 'en')), f"{total_sales_value:,.2f}")

    st.markdown("---")

    # Display table
    st.markdown("### " + get_text('transactions_records_tpl', st.session_state.get('language', 'en')).format(len(filtered_df)))

    # Apply pagination (100 records per page) before formatting
    page_df, pagination_info = paginate_dataframe(filtered_df, page_size=100, key_prefix="product_trans")

    # Format the dataframe for display
    if 'Date' in page_df.columns:
        page_df['Date'] = page_df['Date'].dt.strftime('%Y-%m-%d')

    page_df['Amount'] = page_df['Amount'].apply(lambda x: f"{x:,.2f}")
    page_df['Unit Price'] = page_df['Unit Price'].apply(lambda x: f"{x:,.2f}")

    st.dataframe(
        page_df,
        use_container_width=True
    )

    # Render pagination controls at the bottom
    render_pagination_controls(pagination_info, key_prefix="product_trans")

    # Download button
    csv = filtered_df.to_csv(index=False)
    st.download_button(
        label=get_text('download_transactions_csv', st.session_state.get('language', 'en')),
        data=csv,
        file_name=f"product_transactions_{dt.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )


def main():
    lang = st.session_state.get('language', 'en')

    st.title(get_text('warehouse_title', lang))
    st.markdown(get_text('warehouse_subtitle', lang))

    # Get data
    data = get_warehouse_data()
    invoices_in = data['invoices_in']
    invoices_out = data['invoices_out']

    if invoices_in is None or invoices_in.empty:
        st.warning(get_text('warehouse_no_incoming_warning', lang))
        return

    # Calculate metrics
    with st.spinner(get_text('warehouse_analyzing_spinner', lang)):
        inventory_df = calculate_inventory_status(invoices_in, invoices_out)

    # Tabs
    tab1, tab2 = st.tabs([
        get_text('warehouse_tab_inventory_status', lang),
        get_text('warehouse_tab_detailed_txn', lang)
    ])

    with tab1:
        render_inventory_overview(inventory_df)

    with tab2:
        render_detailed_transactions(invoices_in, invoices_out)


if __name__ == "__main__":
    main()
