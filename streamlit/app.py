"""
Sales Data Engineering Platform - Streamlit dashboard.

Reads directly from the sales_dw Postgres star schema (fact_sales +
dim_customer, dim_product, dim_date) and renders KPI tiles and charts.
"""

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

# Palette (dataviz skill default): categorical slots in fixed order, one
# sequential hue (blue) for single-series magnitude.
COLOR_SEQUENTIAL = "#2a78d6"
CATEGORICAL = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRIDLINE = "#e1e0d9"

st.set_page_config(page_title="Sales Data Engineering Platform", layout="wide")


@st.cache_resource
def get_engine():
    host = os.environ.get("DW_HOST", "localhost")
    port = os.environ.get("DW_PORT", "5433")
    dbname = os.environ.get("DW_NAME", "sales_dw")
    user = os.environ.get("DW_USER", "dw_user")
    password = os.environ.get("DW_PASSWORD", "dw_password")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)


@st.cache_data(ttl=60)
def load_fact():
    engine = get_engine()
    query = """
        SELECT
            f.row_id, f.order_id, f.sales, f.quantity, f.discount, f.profit,
            d.full_date AS order_date, d.year, d.month, d.month_name, d.quarter,
            c.customer_id, c.customer_name, c.segment, c.region, c.state,
            p.product_id, p.product_name, p.category, p.sub_category
        FROM fact_sales f
        JOIN dim_date d ON f.order_date_id = d.date_id
        JOIN dim_customer c ON f.customer_id = c.customer_id
        JOIN dim_product p ON f.product_id = p.product_id
    """
    return pd.read_sql(query, engine, parse_dates=["order_date"])


def chart_layout(fig, title):
    fig.update_layout(
        title=title,
        plot_bgcolor="#fcfcfb",
        paper_bgcolor="#fcfcfb",
        font_color=TEXT_PRIMARY,
        title_font_color=TEXT_PRIMARY,
        margin=dict(t=48, l=10, r=10, b=10),
    )
    fig.update_xaxes(gridcolor=GRIDLINE, linecolor=GRIDLINE, tickfont_color=TEXT_SECONDARY)
    fig.update_yaxes(gridcolor=GRIDLINE, linecolor=GRIDLINE, tickfont_color=TEXT_SECONDARY)
    return fig


st.title("Sales Data Engineering Platform")
st.caption("Superstore sales warehouse — star schema in Postgres, loaded by an Airflow ETL pipeline.")

try:
    df = load_fact()
except Exception as e:
    st.error(f"Could not connect to the warehouse. Has the ETL pipeline been run yet?\n\n{e}")
    st.stop()

if df.empty:
    st.warning("The warehouse is reachable but fact_sales is empty. Run the ETL pipeline first.")
    st.stop()

# ---- KPI row ----
total_revenue = df["sales"].sum()
total_profit = df["profit"].sum()
total_orders = df["order_id"].nunique()
avg_order_value = total_revenue / total_orders if total_orders else 0
total_units = df["quantity"].sum()
profit_margin = (total_profit / total_revenue * 100) if total_revenue else 0

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Total Revenue", f"${total_revenue:,.0f}")
k2.metric("Total Profit", f"${total_profit:,.0f}")
k3.metric("Profit Margin", f"{profit_margin:.1f}%")
k4.metric("Total Orders", f"{total_orders:,}")
k5.metric("Avg Order Value", f"${avg_order_value:,.2f}")
k6.metric("Units Sold", f"{total_units:,.0f}")

st.divider()

# ---- Revenue over time ----
monthly = (
    df.groupby(pd.Grouper(key="order_date", freq="MS"))["sales"]
    .sum()
    .reset_index()
)
fig_time = go.Figure()
fig_time.add_trace(go.Scatter(
    x=monthly["order_date"], y=monthly["sales"],
    mode="lines", line=dict(color=COLOR_SEQUENTIAL, width=2),
    fill="tozeroy", fillcolor="rgba(42,120,214,0.12)",
    hovertemplate="%{x|%b %Y}<br>Revenue: $%{y:,.0f}<extra></extra>",
))
st.plotly_chart(chart_layout(fig_time, "Revenue Over Time (Monthly)"), use_container_width=True)

col1, col2 = st.columns(2)

# ---- Top 10 products by revenue ----
with col1:
    top_products = (
        df.groupby("product_name")["sales"].sum()
        .sort_values(ascending=False).head(10).reset_index()
    )
    fig_prod = px.bar(
        top_products.sort_values("sales"), x="sales", y="product_name",
        orientation="h", color_discrete_sequence=[COLOR_SEQUENTIAL],
    )
    fig_prod.update_traces(hovertemplate="%{y}<br>Revenue: $%{x:,.0f}<extra></extra>")
    fig_prod.update_layout(yaxis_title="", xaxis_title="Revenue ($)")
    st.plotly_chart(chart_layout(fig_prod, "Top 10 Products by Revenue"), use_container_width=True)

# ---- Top 10 customers by revenue ----
with col2:
    top_customers = (
        df.groupby("customer_name")["sales"].sum()
        .sort_values(ascending=False).head(10).reset_index()
    )
    fig_cust = px.bar(
        top_customers.sort_values("sales"), x="sales", y="customer_name",
        orientation="h", color_discrete_sequence=[COLOR_SEQUENTIAL],
    )
    fig_cust.update_traces(hovertemplate="%{y}<br>Revenue: $%{x:,.0f}<extra></extra>")
    fig_cust.update_layout(yaxis_title="", xaxis_title="Revenue ($)")
    st.plotly_chart(chart_layout(fig_cust, "Top 10 Customers by Revenue"), use_container_width=True)

col3, col4 = st.columns(2)

# ---- Revenue by category ----
with col3:
    cat_rev = df.groupby("category")["sales"].sum().sort_values(ascending=False).reset_index()
    fig_cat = px.bar(
        cat_rev, x="category", y="sales",
        color="category", color_discrete_sequence=CATEGORICAL,
    )
    fig_cat.update_traces(hovertemplate="%{x}<br>Revenue: $%{y:,.0f}<extra></extra>")
    fig_cat.update_layout(showlegend=False, xaxis_title="", yaxis_title="Revenue ($)")
    st.plotly_chart(chart_layout(fig_cat, "Revenue by Category"), use_container_width=True)

# ---- Revenue by region ----
with col4:
    region_rev = df.groupby("region")["sales"].sum().sort_values(ascending=False).reset_index()
    fig_region = px.bar(
        region_rev, x="region", y="sales",
        color="region", color_discrete_sequence=CATEGORICAL,
    )
    fig_region.update_traces(hovertemplate="%{x}<br>Revenue: $%{y:,.0f}<extra></extra>")
    fig_region.update_layout(showlegend=False, xaxis_title="", yaxis_title="Revenue ($)")
    st.plotly_chart(chart_layout(fig_region, "Revenue by Region"), use_container_width=True)

with st.expander("View raw fact table sample"):
    st.dataframe(df.head(200))
