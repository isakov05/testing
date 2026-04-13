import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
from translations import get_text
from auth.db_authenticator import protect_page

st.set_page_config(page_title="Products Explorer", page_icon="📦", layout="wide")

protect_page()



def get_product_data():
    """Get data needed for product analysis"""
    return {
        'invoices_out': st.session_state.get('invoices_out_processed')
    }


def find_amount_column(df):
    """Find the appropriate amount column"""
    amount_columns = [
        'Стоимость поставки с учётом НДС', 'Supply Value (incl. VAT)',
        'amount', 'Amount', 'Сумма', 'Стоимость', 'Cost', 'Total'
    ]
    for col in amount_columns:
        if col in df.columns:
            return col
    return None


def find_date_column(df):
    """Find the appropriate date column"""
    date_columns = ['Дата документ', 'Document Date', 'date', 'Date']
    for col in date_columns:
        if col in df.columns:
            return col
    return None


def find_customer_column(df):
    """Find the appropriate customer column"""
    customer_columns = [
        'Покупатель (наименование)', 'Buyer (Name)',
        'Покупатель (ИНН или ПИНФЛ)', 'Buyer (Tax ID or PINFL)',
        'customer', 'Customer'
    ]
    for col in customer_columns:
        if col in df.columns:
            return col
    return None


def find_product_column(df):
    """Find the appropriate product column - prioritize national catalog"""
    # First try national catalog columns
    catalog_cols = [
        'Идентификационный код и название по Единому электронному национальному каталогу товаров (услуг)',
        'Product/Service Code and Name from National Catalog',
        'National Catalog Product Code and Name'
    ]

    for col in catalog_cols:
        if col in df.columns:
            return col, 'catalog'

    # Fallback to other product columns
    fallback_cols = ['Product/Service Description', 'Description', 'product', 'service', 'item']
    for col in fallback_cols:
        if col in df.columns:
            return col, 'standard'

    return None, None


def find_quantity_column(df):
    """Find the appropriate quantity column"""
    quantity_columns = ['Количество', 'Quantity', 'qty', 'amount_qty', 'quantity']
    for col in quantity_columns:
        if col in df.columns:
            return col
    return None


def process_product_data(invoices_df):
    """Process product data from outgoing invoices"""
    if invoices_df is None or invoices_df.empty:
        return pd.DataFrame()

    product_df = invoices_df.copy()

    # Standardize date column
    date_col = find_date_column(product_df)
    if date_col:
        product_df['date'] = pd.to_datetime(product_df[date_col], errors='coerce')
    else:
        product_df['date'] = pd.NaT

    # Standardize amount column
    amount_col = find_amount_column(product_df)
    if amount_col:
        product_df['sales_amount'] = pd.to_numeric(product_df[amount_col], errors='coerce').fillna(0)
    else:
        product_df['sales_amount'] = 0

    # Standardize customer information
    customer_col = find_customer_column(product_df)
    if customer_col:
        product_df['customer'] = product_df[customer_col].astype(str).str.strip()
    else:
        product_df['customer'] = 'Unknown'

    # Standardize product information
    product_col, product_type = find_product_column(product_df)
    if product_col:
        product_df['product'] = product_df[product_col].astype(str)
        product_df['product'] = product_df['product'].replace(['nan', 'None', ''], 'Unknown Product')
        product_df.loc[product_df['product'].isna(), 'product'] = 'Unknown Product'

        # If using catalog format, extract product code and name
        if product_type == 'catalog':
            product_df['product_code'] = product_df['product'].str.extract(r'^([^-]+)').iloc[:, 0].str.strip()
            product_df['product_name'] = product_df['product'].str.extract(r'-\s*(.+)').iloc[:, 0].str.strip()

            # If no dash separator, use full string as product name
            mask_no_name = product_df['product_name'].isna()
            product_df.loc[mask_no_name, 'product_name'] = product_df.loc[mask_no_name, 'product']

            product_df['product_code'] = product_df['product_code'].fillna('N/A')
            product_df.loc[product_df['product_name'].str.strip() == '', 'product_name'] = 'Unknown Product'
        else:
            product_df['product_name'] = product_df['product']
            product_df['product_code'] = 'N/A'
    else:
        product_df['product'] = 'General Sales'
        product_df['product_name'] = 'General Sales'
        product_df['product_code'] = 'N/A'

    # Standardize quantity
    quantity_col = find_quantity_column(product_df)
    if quantity_col:
        product_df['quantity'] = pd.to_numeric(product_df[quantity_col], errors='coerce').fillna(1)
    else:
        product_df['quantity'] = 1

    # Calculate unit price
    product_df['unit_price'] = np.where(
        product_df['quantity'] > 0,
        product_df['sales_amount'] / product_df['quantity'],
        0
    )
    product_df['unit_price'] = product_df['unit_price'].replace([np.inf, -np.inf], 0)
    product_df.loc[product_df['unit_price'] < 0, 'unit_price'] = 0

    # Filter for signed/valid invoices only
    if 'СТАТУС' in product_df.columns:
        product_df = product_df[product_df['СТАТУС'] == 'Подписан']
    elif 'Status' in product_df.columns:
        product_df = product_df[product_df['Status'] == 'Подписан']

    return product_df


def calculate_product_metrics(df):
    """Calculate key product metrics"""
    if df.empty:
        return {
            'total_products': 0,
            'total_revenue': 0,
            'total_quantity': 0,
            'avg_unit_price': 0,
            'revenue_growth': 0
        }

    metrics = {
        'total_products': df['product_name'].nunique(),
        'total_revenue': df['sales_amount'].sum(),
        'total_quantity': df['quantity'].sum(),
        'avg_unit_price': df['unit_price'].mean()
    }

    # Calculate growth
    if 'date' in df.columns and df['date'].notna().any():
        current_month = df['date'].max().replace(day=1) if df['date'].max() is not pd.NaT else datetime.now().replace(day=1)
        prev_month = current_month - timedelta(days=32)
        prev_month = prev_month.replace(day=1)

        current_revenue = df[df['date'] >= current_month]['sales_amount'].sum()
        prev_revenue = df[(df['date'] >= prev_month) & (df['date'] < current_month)]['sales_amount'].sum()

        metrics['revenue_growth'] = ((current_revenue - prev_revenue) / prev_revenue * 100) if prev_revenue > 0 else 0
    else:
        metrics['revenue_growth'] = 0

    return metrics


def create_product_performance_chart(df):
    """Create product performance chart"""
    if df.empty:
        return None

    top_products = df.groupby('product_name')['sales_amount'].sum().sort_values(ascending=False).head(15)
    display_names = [name[:50] + "..." if len(name) > 50 else name for name in top_products.index]

    fig = px.bar(
        x=top_products.values,
        y=display_names,
        orientation='h',
        title='Top 15 Products by Revenue',
        labels={'x': 'Revenue', 'y': 'Product/Service'},
        color=top_products.values,
        color_continuous_scale='Viridis'
    )
    fig.update_layout(height=600, showlegend=False)

    return fig


def create_product_trends_chart(df):
    """Create product sales trends over time"""
    if df.empty or 'date' not in df.columns or df['date'].notna().sum() == 0:
        return None

    df_trends = df[df['date'].notna()].copy()
    df_trends['month'] = df_trends['date'].dt.to_period('M')

    # Get top 5 products
    top_5_products = df.groupby('product_name')['sales_amount'].sum().sort_values(ascending=False).head(5)

    monthly_trends = df_trends[df_trends['product_name'].isin(top_5_products.index)].groupby(['month', 'product_name'])['sales_amount'].sum().reset_index()
    monthly_trends['month'] = monthly_trends['month'].astype(str)
    monthly_trends['product_display'] = monthly_trends['product_name'].apply(lambda x: x[:30] + "..." if len(x) > 30 else x)

    fig = px.line(
        monthly_trends,
        x='month',
        y='sales_amount',
        color='product_display',
        title='Top 5 Products Sales Trends Over Time',
        labels={'sales_amount': 'Revenue', 'month': 'Month', 'product_display': 'Product'},
        markers=True
    )
    fig.update_layout(height=400, hovermode='x unified')

    return fig


def create_product_catalog_analysis(df):
    """Create product catalog specific analysis"""
    if df.empty:
        return None, None

    # Product codes analysis
    code_counts = df[df['product_code'] != 'N/A']['product_code'].value_counts().head(10)

    if not code_counts.empty:
        fig_codes = px.bar(
            x=code_counts.values,
            y=code_counts.index,
            orientation='h',
            title='Top 10 Product Codes by Transaction Count',
            labels={'x': 'Transaction Count', 'y': 'Product Code'},
            color=code_counts.values,
            color_continuous_scale='Blues'
        )
        fig_codes.update_layout(height=400, showlegend=False)
    else:
        fig_codes = None

    # Product categories analysis
    df_with_codes = df[df['product_code'] != 'N/A'].copy()
    if not df_with_codes.empty:
        df_with_codes['product_category'] = df_with_codes['product_code'].str.extract(r'^(\w{2,4})')
        category_revenue = df_with_codes.groupby('product_category')['sales_amount'].sum().sort_values(ascending=False).head(10)

        fig_categories = px.pie(
            values=category_revenue.values,
            names=category_revenue.index,
            title='Revenue Distribution by Product Code Prefix'
        )
        fig_categories.update_layout(height=400)
    else:
        fig_categories = None

    return fig_codes, fig_categories


def create_customer_product_analysis(df):
    """Create customer-product analysis charts"""
    if df.empty:
        return None, None

    # Product diversity by customer
    customer_product_diversity = df.groupby('customer')['product_name'].nunique().sort_values(ascending=False).head(10)

    fig_diversity = px.bar(
        x=customer_product_diversity.index,
        y=customer_product_diversity.values,
        title='Top 10 Customers by Product Diversity',
        labels={'x': 'Customer', 'y': 'Number of Different Products'},
        color=customer_product_diversity.values,
        color_continuous_scale='Blues'
    )
    fig_diversity.update_layout(height=400, showlegend=False, xaxis=dict(tickangle=45))

    # Customer spending per product
    top_customers = df.groupby('customer')['sales_amount'].sum().sort_values(ascending=False).head(5)
    customer_product_matrix = df[df['customer'].isin(top_customers.index)].groupby(['customer', 'product_name'])['sales_amount'].sum().reset_index()

    fig_heatmap = px.density_heatmap(
        customer_product_matrix,
        x='product_name',
        y='customer',
        z='sales_amount',
        title='Customer vs Product Purchase Heatmap (Top 5 Customers)',
        labels={'sales_amount': 'Revenue', 'product_name': 'Product'}
    )
    fig_heatmap.update_layout(height=400)

    return fig_diversity, fig_heatmap


def create_quantity_analysis(df):
    """Create quantity-based analysis charts"""
    if df.empty:
        return None, None

    # Product quantity distribution
    product_quantities = df.groupby('product_name')['quantity'].sum().sort_values(ascending=False).head(15)

    fig_quantity = px.bar(
        x=product_quantities.values,
        y=product_quantities.index,
        orientation='h',
        title='Top 15 Products by Total Quantity Sold',
        labels={'x': 'Total Quantity', 'y': 'Product/Service'},
        color=product_quantities.values,
        color_continuous_scale='Oranges'
    )
    fig_quantity.update_layout(height=500, showlegend=False)

    # Unit price vs quantity scatter
    product_summary = df.groupby('product_name').agg({
        'unit_price': 'mean',
        'quantity': 'sum',
        'sales_amount': 'sum'
    }).reset_index()

    product_summary = product_summary[
        (product_summary['unit_price'] > 0) &
        (product_summary['quantity'] > 0) &
        (product_summary['sales_amount'] != 0)
    ].copy()

    if not product_summary.empty:
        product_summary['size_value'] = product_summary['sales_amount'].abs()

        fig_scatter = px.scatter(
            product_summary,
            x='unit_price',
            y='quantity',
            size='size_value',
            hover_name='product_name',
            title='Product Analysis: Unit Price vs Quantity (Size = Total Revenue)',
            labels={'unit_price': 'Average Unit Price', 'quantity': 'Total Quantity Sold'},
            hover_data={'sales_amount': ':,.2f'}
        )
        fig_scatter.update_layout(height=400)
    else:
        fig_scatter = None

    return fig_quantity, fig_scatter


def render_product_summary(metrics):
    """Render product summary metrics"""
    st.subheader("📊 Key Product Metrics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Products",
            f"{metrics['total_products']:,}",
            delta="Product categories"
        )

    with col2:
        st.metric(
            "Total Revenue",
            f"{metrics['total_revenue']:,.2f}",
            delta=f"{metrics['revenue_growth']:+.1f}%" if metrics['revenue_growth'] != 0 else None
        )

    with col3:
        st.metric(
            "Total Quantity",
            f"{metrics['total_quantity']:,.0f}",
            delta="Units sold"
        )

    with col4:
        st.metric(
            "Avg Unit Price",
            f"{metrics['avg_unit_price']:,.2f}"
        )


def render_product_performance(product_df):
    """Render product performance section"""
    st.subheader("Product Revenue Performance")

    performance_chart = create_product_performance_chart(product_df)
    if performance_chart:
        st.plotly_chart(performance_chart, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top Products Summary")

        product_summary = product_df.groupby('product_name').agg({
            'sales_amount': 'sum',
            'quantity': 'sum',
            'customer': 'nunique',
            'unit_price': 'mean'
        }).round(2)

        product_summary.columns = ['Total Revenue', 'Total Quantity', 'Customers', 'Avg Unit Price']
        product_summary = product_summary.sort_values('Total Revenue', ascending=False)

        display_summary = product_summary.head(10).copy()
        display_summary['Total Revenue'] = display_summary['Total Revenue'].apply(lambda x: f"{x:,.2f}")
        display_summary['Avg Unit Price'] = display_summary['Avg Unit Price'].apply(lambda x: f"{x:,.2f}")

        st.dataframe(display_summary, use_container_width=True)

    with col2:
        st.subheader("Revenue Distribution")

        product_revenue = product_df.groupby('product_name')['sales_amount'].sum().sort_values(ascending=False)
        top_8_products = product_revenue.head(8)
        others_revenue = product_revenue.iloc[8:].sum()

        if others_revenue > 0:
            pie_data = list(top_8_products.values) + [others_revenue]
            pie_labels = [name[:30] + "..." if len(name) > 30 else name for name in top_8_products.index] + ['Others']
        else:
            pie_data = list(top_8_products.values)
            pie_labels = [name[:30] + "..." if len(name) > 30 else name for name in top_8_products.index]

        fig_pie = px.pie(values=pie_data, names=pie_labels, title="Product Revenue Distribution")
        st.plotly_chart(fig_pie, use_container_width=True)


def main() -> None:

    lang = st.session_state.get('language', 'en')

    st.title("📦 Products Explorer")
    st.caption("Comprehensive product and service analysis based on sales data")

    # Get data
    product_data = get_product_data()
    invoices_df = product_data['invoices_out']

    if invoices_df is None or invoices_df.empty:
        st.warning("📁 No outgoing invoice data available. Please upload and process invoice files first.")
        st.page_link("pages/file_upload.py", label="→ Go to File Upload", icon="📁")
        return

    # Process product data
    with st.spinner("Processing product data..."):
        product_df = process_product_data(invoices_df)

    if product_df.empty:
        st.warning("No product data could be processed.")
        return

    # Calculate metrics
    metrics = calculate_product_metrics(product_df)

    # Summary metrics
    render_product_summary(metrics)

    st.divider()

    # Tabs for different analyses
    tabs = st.tabs([
        "📊 Product Performance",
        "📈 Product Trends",
        "🏷️ Catalog Analysis",
        "👥 Customer Insights",
        "📦 Quantity Analysis"
    ])

    with tabs[0]:
        render_product_performance(product_df)

    with tabs[1]:
        st.subheader("Product Sales Trends Over Time")

        trends_chart = create_product_trends_chart(product_df)
        if trends_chart:
            st.plotly_chart(trends_chart, use_container_width=True)
        else:
            st.info("No date information available for trend analysis")

        col1, col2 = st.columns(2)

        with col1:
            if 'date' in product_df.columns and product_df['date'].notna().any():
                st.subheader("Monthly Product Performance")

                monthly_products = product_df[product_df['date'].notna()].copy()
                monthly_products['month'] = monthly_products['date'].dt.to_period('M')

                monthly_summary = monthly_products.groupby('month').agg({
                    'sales_amount': 'sum',
                    'quantity': 'sum',
                    'product': 'nunique'
                }).round(2)

                monthly_summary.columns = ['Total Revenue', 'Total Quantity', 'Product Count']
                monthly_summary.index = monthly_summary.index.astype(str)

                monthly_display = monthly_summary.copy()
                monthly_display['Total Revenue'] = monthly_display['Total Revenue'].apply(lambda x: f"{x:,.2f}")

                st.dataframe(monthly_display, use_container_width=True)

        with col2:
            st.subheader("Product Lifecycle")

            if 'date' in product_df.columns and product_df['date'].notna().any():
                product_lifecycle = product_df.groupby('product')['date'].agg(['min', 'max']).reset_index()
                product_lifecycle['days_active'] = (product_lifecycle['max'] - product_lifecycle['min']).dt.days

                def categorize_product(row):
                    if row['days_active'] > 180:
                        return "🌱 Established"
                    elif row['days_active'] > 90:
                        return "🌟 Growing"
                    elif row['days_active'] > 30:
                        return "🔴 New"
                    else:
                        return "⚡ Trial"

                product_lifecycle['Category'] = product_lifecycle.apply(categorize_product, axis=1)
                lifecycle_counts = product_lifecycle['Category'].value_counts()

                fig_lifecycle = px.pie(
                    values=lifecycle_counts.values,
                    names=lifecycle_counts.index,
                    title="Product Lifecycle Distribution"
                )
                st.plotly_chart(fig_lifecycle, use_container_width=True)
            else:
                st.info("No date information available for lifecycle analysis")

    with tabs[2]:
        st.subheader("National Product Catalog Analysis")

        codes_chart, categories_chart = create_product_catalog_analysis(product_df)

        col1, col2 = st.columns(2)

        with col1:
            if codes_chart:
                st.plotly_chart(codes_chart, use_container_width=True)
            else:
                st.info("No product code data available")

        with col2:
            if categories_chart:
                st.plotly_chart(categories_chart, use_container_width=True)
            else:
                st.info("No product category data available")

        # Product code details table
        st.subheader("Product Catalog Details")

        if 'product_code' in product_df.columns and not product_df[product_df['product_code'] != 'N/A'].empty:
            catalog_details = product_df[product_df['product_code'] != 'N/A'].groupby(['product_code', 'product_name']).agg({
                'sales_amount': 'sum',
                'quantity': 'sum',
                'customer': 'nunique'
            }).round(2).reset_index()

            catalog_details.columns = ['Product Code', 'Product Name', 'Total Revenue', 'Total Quantity', 'Customers']
            catalog_details = catalog_details.sort_values('Total Revenue', ascending=False)

            display_catalog = catalog_details.head(20).copy()
            display_catalog['Total Revenue'] = display_catalog['Total Revenue'].apply(lambda x: f"{x:,.2f}")
            display_catalog['Product Name'] = display_catalog['Product Name'].apply(lambda x: x[:50] + "..." if len(x) > 50 else x)

            st.dataframe(display_catalog, use_container_width=True)

            col1, col2, col3 = st.columns(3)

            with col1:
                unique_codes = product_df['product_code'].nunique() - (1 if 'N/A' in product_df['product_code'].values else 0)
                st.metric("Unique Product Codes", f"{unique_codes:,}")

            with col2:
                coded_revenue = product_df[product_df['product_code'] != 'N/A']['sales_amount'].sum()
                total_revenue = product_df['sales_amount'].sum()
                coverage = (coded_revenue / total_revenue * 100) if total_revenue > 0 else 0
                st.metric("Catalog Coverage", f"{coverage:.1f}%", delta="of total revenue")

            with col3:
                avg_code_revenue = coded_revenue / unique_codes if unique_codes > 0 else 0
                st.metric("Avg Revenue per Code", f"{avg_code_revenue:,.2f}")
        else:
            st.info("No product code data available in the dataset")

    with tabs[3]:
        st.subheader("Customer-Product Relationship Analysis")

        diversity_chart, heatmap_chart = create_customer_product_analysis(product_df)

        col1, col2 = st.columns(2)

        with col1:
            if diversity_chart:
                st.plotly_chart(diversity_chart, use_container_width=True)

        with col2:
            if heatmap_chart:
                st.plotly_chart(heatmap_chart, use_container_width=True)

        # Customer product preferences
        st.subheader("Customer Product Preferences")

        customer_segments = product_df.groupby('customer')['sales_amount'].sum()
        high_value_customers = customer_segments.quantile(0.8)
        medium_value_customers = customer_segments.quantile(0.5)

        def segment_customer(customer_name):
            spending = customer_segments.get(customer_name, 0)
            if spending >= high_value_customers:
                return "High Value"
            elif spending >= medium_value_customers:
                return "Medium Value"
            else:
                return "Low Value"

        product_df_segments = product_df.copy()
        product_df_segments['customer_segment'] = product_df_segments['customer'].apply(segment_customer)

        segment_product_prefs = product_df_segments.groupby(['customer_segment', 'product_name'])['sales_amount'].sum().reset_index()

        col1, col2, col3 = st.columns(3)

        for idx, segment in enumerate(['High Value', 'Medium Value', 'Low Value']):
            segment_data = segment_product_prefs[segment_product_prefs['customer_segment'] == segment]
            if not segment_data.empty:
                top_products = segment_data.nlargest(5, 'sales_amount')

                with [col1, col2, col3][idx]:
                    st.write(f"**{segment} Customers**")
                    for _, row in top_products.iterrows():
                        product_name = row['product_name'][:30] + "..." if len(row['product_name']) > 30 else row['product_name']
                        st.write(f"• {product_name}: {row['sales_amount']:,.2f}")

    with tabs[4]:
        st.subheader("Product Quantity and Pricing Analysis")

        quantity_chart, scatter_chart = create_quantity_analysis(product_df)

        col1, col2 = st.columns(2)

        with col1:
            if quantity_chart:
                st.plotly_chart(quantity_chart, use_container_width=True)

        with col2:
            if scatter_chart:
                st.plotly_chart(scatter_chart, use_container_width=True)

        # Product performance metrics
        st.subheader("Product Performance Metrics")

        product_metrics = product_df.groupby('product_name').agg({
            'sales_amount': ['sum', 'mean', 'count'],
            'quantity': ['sum', 'mean'],
            'unit_price': ['mean', 'std'],
            'customer': 'nunique'
        }).round(2)

        product_metrics.columns = ['Total Revenue', 'Avg Revenue', 'Transactions', 'Total Qty', 'Avg Qty', 'Avg Price', 'Price Std', 'Customers']

        def calculate_performance_score(row):
            revenue_score = min(row['Total Revenue'] / product_metrics['Total Revenue'].max() * 40, 40)
            quantity_score = min(row['Total Qty'] / product_metrics['Total Qty'].max() * 30, 30)
            customer_score = min(row['Customers'] / product_metrics['Customers'].max() * 30, 30)
            return revenue_score + quantity_score + customer_score

        product_metrics['Performance Score'] = product_metrics.apply(calculate_performance_score, axis=1)
        product_metrics = product_metrics.sort_values('Performance Score', ascending=False)

        display_metrics = product_metrics.head(15).copy()
        for col in ['Total Revenue', 'Avg Revenue', 'Avg Price']:
            display_metrics[col] = display_metrics[col].apply(lambda x: f"{x:,.2f}")
        display_metrics['Performance Score'] = display_metrics['Performance Score'].apply(lambda x: f"{x:.1f}")

        st.dataframe(display_metrics, use_container_width=True)

        # Export functionality
        st.subheader("Export Data")

        col1, col2 = st.columns(2)

        with col1:
            product_summary = product_df.groupby('product_name').agg({
                'sales_amount': 'sum',
                'quantity': 'sum',
                'customer': 'nunique',
                'unit_price': 'mean'
            }).round(2)
            csv_products = product_summary.to_csv()
            st.download_button(
                "📥 Download Product Analysis",
                data=csv_products,
                file_name=f"product_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

        with col2:
            csv_metrics = product_metrics.to_csv()
            st.download_button(
                "📥 Download Performance Metrics",
                data=csv_metrics,
                file_name=f"product_metrics_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )


if __name__ == "__main__":
    main()
