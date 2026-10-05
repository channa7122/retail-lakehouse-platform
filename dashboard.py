"""Enterprise Retail Lakehouse Analytics Dashboard.

Interactive web application displaying executive KPIs, daily sales trends,
and product category margins directly from the Gold Delta Lake layer.
"""

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(
    page_title="Retail Lakehouse Executive Dashboard",
    page_icon="🛒",
    layout="wide",
)

GOLD_PATH = Path("data/gold")


@st.cache_data
def load_gold_data():
    """Load and prepare Gold Delta tables for analytics visualization."""
    sales_df = pd.read_parquet(GOLD_PATH / "fct_daily_sales")
    products_df = pd.read_parquet(GOLD_PATH / "dim_products")
    stores_df = pd.read_parquet(GOLD_PATH / "dim_stores")

    # Standardize region column name
    if "region" in stores_df.columns:
        stores_df = stores_df.rename(columns={"region": "store_region"})

    # Merge sales with stores for regional analytics
    merged = sales_df.merge(stores_df, on="store_id", how="left")
    merged["sales_date"] = pd.to_datetime(merged["sales_date"])
    return merged, products_df, stores_df


def main():
    st.title("🛒 Enterprise Retail Lakehouse Dashboard")
    st.markdown("Real-time analytics powered by **Apache Spark**, **Delta Lake (Gold Layer)**, and **dbt**.")
    st.divider()

    try:
        sales_df, products_df, stores_df = load_gold_data()
    except Exception as e:
        st.error(f"Error loading Gold Delta tables: {e}")
        st.info("Make sure the pipeline has generated the data inside data/gold/.")
        return

    # --- Sidebar Filters ---
    st.sidebar.header("🔍 Analytics Filters")
    region_column = "store_region" if "store_region" in stores_df.columns else "region"
    regions = ["All"] + sorted(stores_df[region_column].dropna().unique().tolist())
    selected_region = st.sidebar.selectbox("Filter by Store Region", regions)

    filtered_df = sales_df.copy()
    if selected_region != "All":
        filtered_df = filtered_df[filtered_df[region_column] == selected_region]

    # --- Top Executive KPIs ---
    total_revenue = filtered_df["daily_net_revenue"].sum()
    total_orders = filtered_df["total_orders"].sum()
    total_units = filtered_df["total_units_sold"].sum()
    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💰 Total Net Revenue", f"${total_revenue:,.2f}")
    col2.metric("📦 Total Orders Completed", f"{total_orders:,}")
    col3.metric("🏷️ Total Units Sold", f"{total_units:,}")
    col4.metric("📊 Average Order Value (AOV)", f"${avg_order_value:.2f}")

    st.divider()

    # --- Visual Charts ---
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.subheader("📈 Daily Sales Trend")
        daily_trend = filtered_df.groupby("sales_date")["daily_net_revenue"].sum().reset_index()
        fig_trend = px.line(
            daily_trend,
            x="sales_date",
            y="daily_net_revenue",
            labels={"sales_date": "Date", "daily_net_revenue": "Net Revenue ($)"},
            template="plotly_white",
        )
        fig_trend.update_traces(line_color="#1f77b4", line_width=2.5)
        st.plotly_chart(fig_trend, use_container_width=True)

    with chart_col2:
        st.subheader("🏆 Revenue by Product Category")
        category_sales = filtered_df.groupby("category")["daily_net_revenue"].sum().reset_index()
        fig_cat = px.bar(
            category_sales,
            x="category",
            y="daily_net_revenue",
            labels={"category": "Category", "daily_net_revenue": "Revenue ($)"},
            color="category",
            template="plotly_white",
        )
        st.plotly_chart(fig_cat, use_container_width=True)

    # --- Second Row: Margin Analysis & Store Ranking ---
    row2_col1, row2_col2 = st.columns(2)

    with row2_col1:
        st.subheader("💎 Average Profit Margin % by Category")
        margin_by_cat = products_df.groupby("category")["profit_margin_pct"].mean().reset_index()
        fig_margin = px.bar(
            margin_by_cat,
            x="category",
            y="profit_margin_pct",
            labels={"category": "Category", "profit_margin_pct": "Avg Margin (%)"},
            color="profit_margin_pct",
            color_continuous_scale="Viridis",
            template="plotly_white",
        )
        st.plotly_chart(fig_margin, use_container_width=True)

    with row2_col2:
        st.subheader("🏬 Store Sales Ranking (Top 10)")
        top_stores = (
            filtered_df.groupby("store_name")["daily_net_revenue"]
            .sum()
            .reset_index()
            .sort_values(by="daily_net_revenue", ascending=False)
            .head(10)
        )
        fig_stores = px.bar(
            top_stores,
            x="daily_net_revenue",
            y="store_name",
            orientation="h",
            labels={"daily_net_revenue": "Revenue ($)", "store_name": "Store"},
            template="plotly_white",
        )
        st.plotly_chart(fig_stores, use_container_width=True)

    # --- Data Preview ---
    with st.expander("📋 Inspect Raw Gold Records (`fct_daily_sales`)"):
        st.dataframe(filtered_df.head(100), use_container_width=True)


if __name__ == "__main__":
    main()
