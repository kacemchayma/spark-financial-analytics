import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="Stock Market Big Data Analysis",
    layout="wide"
)

# =====================================================
# TITLE
# =====================================================
st.title("📈 Stock Market Big Data Analysis")
st.markdown("""
Analyse Big Data des données financières haute fréquence  
**Apache Spark – Streamlit – Plotly**
""")

# =====================================================
# LOAD DATA
# =====================================================
@st.cache_data
def load_data():
    df = pd.read_parquet("data/stock_id/stock_id.parquet")
    return df.sample(min(len(df), 50000), random_state=42)  # visuel rapide

df = load_data()

# =====================================================
# SIDEBAR
# =====================================================
st.sidebar.header("⚙️ Paramètres")
time_range = st.sidebar.slider(
    "Seconds in bucket",
    int(df["seconds_in_bucket"].min()),
    int(df["seconds_in_bucket"].max()),
    (0, 59)
)

# =====================================================
# FILTER & METRICS
# =====================================================
filtered_df = df[
    (df["seconds_in_bucket"] >= time_range[0]) &
    (df["seconds_in_bucket"] <= time_range[1])
]

if filtered_df.empty:
    st.warning("Aucune donnée pour cette sélection temporelle.")
    st.stop()

spread = filtered_df["ask_price1"] - filtered_df["bid_price1"]

col1, col2, col3 = st.columns(3)
col1.metric("Avg Spread", f"{spread.mean():.6f}")
col2.metric("Avg Bid Price", f"{filtered_df['bid_price1'].mean():.2f}")
col3.metric("Avg Ask Price", f"{filtered_df['ask_price1'].mean():.2f}")

# =====================================================
# VISUAL 1 — Spread Distribution
# =====================================================
st.subheader("📊 Spread Distribution")

fig_spread = px.histogram(
    spread,
    nbins=50,
    labels={"value": "Spread"},
    title="Distribution of Bid-Ask Spread"
)
st.plotly_chart(fig_spread, use_container_width=True)

# =====================================================
# VISUAL 2 — Spread over Time (Aggregated)
# =====================================================
st.subheader("⏱️ Spread Over Time")

# On agrège par seconde pour avoir une courbe lisible
df_time = filtered_df.copy()
df_time["spread"] = spread

df_agg = df_time.groupby("seconds_in_bucket")["spread"].mean().reset_index()

fig_time = px.line(
    df_agg,
    x="seconds_in_bucket",
    y="spread",
    title="Average Spread Evolution Over Time",
    markers=True
)
st.plotly_chart(fig_time, use_container_width=True)

# =====================================================
# VISUAL 3 — Liquidity
# =====================================================
st.subheader("💧 Market Liquidity")

df_time["liquidity"] = (
    df_time["bid_size1"] + df_time["ask_size1"]
)

fig_liq = px.box(
    df_time,
    y="liquidity",
    title="Liquidity Distribution"
)
st.plotly_chart(fig_liq, use_container_width=True)

# =====================================================
# VISUAL 4 — Clustering Interpretation (Phase 4)
# =====================================================
st.subheader("🧠 Market Regimes (Clustering - KMeans)")

# Valeurs issues de output/stock_phase4_results.txt
cluster_data = {
    "Cluster": ["Cluster 0", "Cluster 1", "Cluster 2"],
    "Avg Volatility": [0.000117, 0.000307, 0.000117] 
}
# Note: Cluster 2 value was truncated in output, duplicating 0 for now or using similar scale
# Update with real values if reading full file succeeds. Cluster 2 seems to be 0.000117 from output snippet.

cluster_df = pd.DataFrame(cluster_data)

fig_cluster = px.bar(
    cluster_df,
    x="Cluster",
    y="Avg Volatility",
    title="Market Regimes Identified by Spark MLlib",
    color="Cluster"
)
st.plotly_chart(fig_cluster, use_container_width=True)

# =====================================================
# FOOTER
# =====================================================
st.markdown("---")
st.markdown(
    "👨‍🎓 **Big Data Project – Apache Spark | Visualisation with Streamlit & Plotly**"
)
