import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import os
from dotenv import load_dotenv
from datetime import date

# --- Load environment variables ---
load_dotenv()

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA")
}

st.set_page_config(layout="wide")
st.title("📊 Bilateral Flows Sankey (Smooth Version)")

# --- Create Snowpark session ---
@st.cache_resource
def create_snowpark_session():
    return Session.builder.configs(SNOWFLAKE_CONFIG).create()

session = create_snowpark_session()

# --- Load available variables ---
@st.cache_data
def load_available_vars():
    return (
        session.table("FCT_ECON_COMPARISON")
        .filter((col('"GEO_ID"') == "country/AUT") & col('"COUNTERPART_GEO_ID"').is_not_null())
        .select(col('"VARIABLE_NAME"'))
        .distinct()
        .sort(col('"VARIABLE_NAME"'))
        .to_pandas()["VARIABLE_NAME"]
        .tolist()
    )

# --- Load available years from DATE column ---
@st.cache_data
def load_available_years():
    df = (
        session.table("FCT_ECON_COMPARISON")
        .filter((col('"GEO_ID"') == "country/AUT") & col('"COUNTERPART_GEO_ID"').is_not_null())
        .select(col('"DATE"'))
        .distinct()
        .to_pandas()
    )
    df["DATE"] = pd.to_datetime(df["DATE"])
    return sorted(df["DATE"].dt.year.unique().tolist())

# --- Cached Sankey query by (year, variable) ---
@st.cache_data
def load_sankey_data(year, variable):
    start_date = date(year, 1, 1)
    end_date = date(year, 12, 31)

    df = (
        session.table("FCT_ECON_COMPARISON")
        .filter(
            (col('"GEO_ID"') == "country/AUT") &
            (col('"COUNTERPART_GEO_ID"').is_not_null()) &
            (col('"VARIABLE_NAME"') == variable) &
            (col('"DATE"') >= start_date) &
            (col('"DATE"') <= end_date)
        )
        .select(
            col('"GEO_ID"'),
            col('"COUNTERPART_GEO_ID"'),
            col('"VALUE"')
        )
        .sort(col('"VALUE"').desc())
        .limit(30)  # Slightly lower for performance
        .to_pandas()
    )

    df.rename(columns={
        "GEO_ID": "source",
        "COUNTERPART_GEO_ID": "target",
        "VALUE": "value"
    }, inplace=True)

    df["source"] = df["source"].str.replace("country/", "")
    df["target"] = df["target"].str.replace("country/", "")

    return df

# --- Load filters ---
available_vars = load_available_vars()
available_years = load_available_years()

# --- UI controls ---
sankey_variable = st.sidebar.selectbox("📈 Select Variable", available_vars)
selected_year = st.sidebar.selectbox("📅 Select Year", available_years, index=len(available_years)-1)

# --- Load Sankey data ---
df_sankey = load_sankey_data(selected_year, sankey_variable)

# --- Draw Sankey ---
if df_sankey.empty:
    st.warning("No data found for the selected variable and year.")
else:
    all_nodes = pd.unique(df_sankey[["source", "target"]].values.ravel("K")).tolist()
    node_map = {name: i for i, name in enumerate(all_nodes)}
    df_sankey["source_id"] = df_sankey["source"].map(node_map)
    df_sankey["target_id"] = df_sankey["target"].map(node_map)

    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=all_nodes
        ),
        link=dict(
            source=df_sankey["source_id"],
            target=df_sankey["target_id"],
            value=df_sankey["value"]
        )
    )])

    fig.update_layout(
        title_text=f"💸 {sankey_variable} — {selected_year}",
        font_size=12,
        height=600,
        margin=dict(l=30, r=30, t=50, b=30)
    )

    st.plotly_chart(fig, use_container_width=True)


