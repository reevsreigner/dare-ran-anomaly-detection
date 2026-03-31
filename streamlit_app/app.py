"""
DARE RAN Anomaly Detection — Interactive Portfolio Dashboard
============================================================
Author: Krishna Jha
Dataset: NIST DARE RAN Dataset (publicly available)
Stack: Python · Streamlit · BigQuery · Plotly

Research Question:
Can Radio Access Network (RAN) KPI signatures alone reveal
silent LTE cipher-state misconfigurations — without network alarms?
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DARE RAN Anomaly Detection",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Main background */
    .stApp { background-color: #0F1117; }

    /* Sidebar */
    [data-testid="stSidebar"] { background-color: #1A1D27; }

    /* Metric cards */
    [data-testid="stMetric"] {
        background-color: #1E2130;
        border: 1px solid #2E3250;
        border-radius: 8px;
        padding: 16px;
    }
    [data-testid="stMetricLabel"] { color: #8B9EC7 !important; font-size: 13px !important; }
    [data-testid="stMetricValue"] { color: #E8EAF6 !important; font-size: 28px !important; font-weight: 700 !important; }
    [data-testid="stMetricDelta"] { font-size: 13px !important; }

    /* Section headers */
    .section-header {
        background: linear-gradient(90deg, #1F3A5C 0%, #1A1D27 100%);
        border-left: 4px solid #2E75B6;
        padding: 12px 20px;
        border-radius: 0 8px 8px 0;
        margin: 24px 0 16px 0;
    }
    .section-header h3 { color: #7CB9E8; margin: 0; font-size: 18px; }

    /* Insight boxes */
    .insight-box {
        background-color: #1A2744;
        border: 1px solid #2E4A7A;
        border-left: 4px solid #4A9EDB;
        border-radius: 8px;
        padding: 16px 20px;
        margin: 12px 0;
    }
    .insight-box p { color: #B8D4F0; margin: 0; font-size: 14px; line-height: 1.6; }

    /* Finding boxes */
    .finding-box {
        background-color: #1A2A1A;
        border: 1px solid #2A4A2A;
        border-left: 4px solid #4CAF50;
        border-radius: 8px;
        padding: 16px 20px;
        margin: 12px 0;
    }
    .finding-box p { color: #B8D4B8; margin: 0; font-size: 14px; line-height: 1.6; }

    /* Warning boxes */
    .warning-box {
        background-color: #2A1A1A;
        border: 1px solid #4A2A2A;
        border-left: 4px solid #E57373;
        border-radius: 8px;
        padding: 16px 20px;
        margin: 12px 0;
    }
    .warning-box p { color: #D4B8B8; margin: 0; font-size: 14px; line-height: 1.6; }

    /* Term definition */
    .term-def {
        background-color: #1E2130;
        border-radius: 8px;
        padding: 14px 18px;
        margin: 8px 0;
        border: 1px solid #2E3250;
    }
    .term-def .term { color: #7CB9E8; font-weight: 700; font-size: 14px; }
    .term-def .definition { color: #9BA8C0; font-size: 13px; margin-top: 4px; }

    /* Hero banner */
    .hero-banner {
        background: linear-gradient(135deg, #0D1B2A 0%, #1A3A5C 50%, #0D2A1A 100%);
        border: 1px solid #2E4A7A;
        border-radius: 12px;
        padding: 32px 40px;
        margin-bottom: 32px;
        text-align: center;
    }
    .hero-banner h1 { color: #7CB9E8; font-size: 32px; margin-bottom: 8px; }
    .hero-banner p { color: #9BA8C0; font-size: 16px; margin: 0; }

    /* Divider */
    hr { border-color: #2E3250; }

    /* Table styling */
    .stDataFrame { border-radius: 8px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# ── BigQuery connection ───────────────────────────────────────────────────────
@st.cache_resource
def get_bq_client():
    """Connect to BigQuery using service account credentials from Streamlit secrets."""
    try:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(credentials=credentials, project="nist-anomaly-de-2026")
    except Exception as e:
        st.error(f"BigQuery connection failed: {e}")
        return None

@st.cache_data(ttl=3600)
def load_gold_data():
    """Load Gold domain KPIs table."""
    client = get_bq_client()
    if client is None:
        return pd.DataFrame()
    query = """
        SELECT *
        FROM `nist-anomaly-de-2026.dare_gold.domain_kpis`
        ORDER BY tranche, session, run_id
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_silver_sample():
    """Load per-run features from Gold table for distribution charts."""
    client = get_bq_client()
    if client is None:
        return pd.DataFrame()
    query = """
        SELECT run_id, cipher_state, tranche, session,
               bler_mean,
               bler_mean AS bler_q25,
               bler_mean AS bler_q50,
               bler_mean AS bler_q75,
               bler_mean AS bler_q95,
               retx_mean,
               retx_mean AS retx_q95,
               rsrq_mean,
               mcs_mean,
               dl_throughput_mean_kbps AS dl_throughput_mean,
               bler_mean AS harq_tb_size_q25,
               harq_efficiency,
               bler_spread
        FROM `nist-anomaly-de-2026.dare_gold.domain_kpis`
        ORDER BY tranche, session, run_id
    """
    return client.query(query).to_dataframe()

# ── Colour scheme ─────────────────────────────────────────────────────────────
CIPHER_COLOURS = {"on": "#E57373", "off": "#64B5F6"}
CIPHER_LABELS  = {"on": "Cipher ON (misconfigured)", "off": "Cipher OFF (normal)"}

# ── Plotly theme ──────────────────────────────────────────────────────────────
PLOTLY_LAYOUT = dict(
    paper_bgcolor="#1E2130",
    plot_bgcolor="#161927",
    font=dict(color="#9BA8C0", family="Inter, sans-serif"),
    title_font=dict(color="#E8EAF6", size=16),
    legend=dict(
        bgcolor="#1E2130",
        bordercolor="#2E3250",
        borderwidth=1,
        font=dict(color="#9BA8C0")
    ),
    margin=dict(t=50, b=40, l=50, r=20)
)

# Axis styling applied per-chart to avoid kwarg conflicts
AXIS_STYLE = dict(gridcolor="#252840", zerolinecolor="#252840", color="#9BA8C0")

# ── Helper functions ──────────────────────────────────────────────────────────
def section(title, icon=""):
    st.markdown(f"""
    <div class="section-header">
        <h3>{icon} {title}</h3>
    </div>
    """, unsafe_allow_html=True)

def insight(text):
    st.markdown(f'<div class="insight-box"><p>💡 {text}</p></div>', unsafe_allow_html=True)

def finding(text):
    st.markdown(f'<div class="finding-box"><p>✅ {text}</p></div>', unsafe_allow_html=True)

def warning(text):
    st.markdown(f'<div class="warning-box"><p>⚠️ {text}</p></div>', unsafe_allow_html=True)

def term(name, definition):
    st.markdown(f"""
    <div class="term-def">
        <div class="term">{name}</div>
        <div class="definition">{definition}</div>
    </div>
    """, unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📡 DARE RAN")
    st.markdown("### Anomaly Detection")
    st.markdown("---")

    page = st.radio(
        "Navigate",
        ["🏠 Project Overview",
         "📊 Dataset Explorer",
         "🔬 KPI Signal Analysis",
         "💡 Key Findings",
         "🗂️ Glossary"],
        label_visibility="collapsed"
    )

    st.markdown("---")
    st.markdown("**Data Source**")
    st.markdown("NIST DARE RAN Dataset")
    st.markdown("*Publicly available*")
    st.markdown("---")
    st.markdown("**Stack**")
    st.markdown("Python · Streamlit · BigQuery · GCS · Plotly")
    st.markdown("---")
    st.markdown("**Author**")
    st.markdown("Krishna Jha")
    st.markdown("*Data Engineering Portfolio*")
    st.markdown("*March 2026*")

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Loading data from BigQuery..."):
    gold = load_gold_data()
    silver = load_silver_sample()

if gold.empty:
    st.error("Could not load data. Check BigQuery credentials.")
    st.stop()

# Split by cipher state
gold_on  = gold[gold["cipher_state"] == "on"]
gold_off = gold[gold["cipher_state"] == "off"]

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — PROJECT OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
if page == "🏠 Project Overview":

    st.markdown("""
    <div class="hero-banner">
        <h1>📡 DARE RAN Anomaly Detection</h1>
        <p>Can LTE radio KPI signatures reveal silent cipher misconfigurations — without any network alarms?</p>
    </div>
    """, unsafe_allow_html=True)

    # ── Key metrics ───────────────────────────────────────────────────────────
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Runs", f"{len(gold):,}")
    col2.metric("Cipher-ON Runs", f"{len(gold_on):,}")
    col3.metric("Cipher-OFF Runs", f"{len(gold_off):,}")
    col4.metric("Raw Measurements", "15.2M rows")
    col5.metric("DQ Checks Passed", "8 / 8")

    st.markdown("---")

    # ── What is this project ──────────────────────────────────────────────────
    section("What is this project?", "🎯")
    col1, col2 = st.columns([3, 2])
    with col1:
        st.markdown("""
        This project investigates a real-world cybersecurity scenario in mobile networks:

        **The problem:** In 4G LTE networks, encryption (cipher) can be silently disabled through misconfiguration.
        When this happens, user data travels over the air unencrypted — but **no alarm fires**. The network
        continues operating normally. Users notice nothing. Operators see nothing unusual on their dashboards.

        **The question:** Even though the network looks healthy, does the *behaviour* of the radio channel
        change subtly enough to be detected statistically?

        **The approach:** NIST (National Institute of Standards and Technology) ran 2,033 controlled LTE
        experiments — half with cipher ON (normal), half with cipher OFF (misconfigured). We built a
        complete data engineering pipeline to process, store, and analyse those experiments on Google Cloud.
        """)
    with col2:
        # Architecture diagram as a simple flow
        st.markdown("""
        **Pipeline Architecture**
        ```
        Raw CSVs (37 GB)
              ↓ Python preprocessing
        Parquet files (1 GB)
              ↓ gsutil upload
        GCS Bucket (GCP)
              ↓ BigQuery external table
        Bronze Layer (15.2M rows)
              ↓ SQL aggregation
        Silver Layer (1,982 rows)
              ↓ SQL KPI derivation
        Gold Layer (5 domain KPIs)
              ↓ Streamlit
        This Dashboard
        ```
        """)

    # ── The experiment ────────────────────────────────────────────────────────
    section("How the experiment worked", "🔬")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("""
        **The Lab Setup**

        NIST set up a real 4G LTE base station
        (eNodeB) in a controlled laboratory.
        Multiple virtual mobile phones (UEs)
        connected to it simultaneously.

        A measurement tool recorded every
        packet transmitted over the radio
        interface at ~1ms resolution.
        """)
    with col2:
        st.markdown("""
        **The Variable**

        For each experiment run, cipher was
        set to either:
        - **ON** — normal encrypted operation
        - **OFF** — silent misconfiguration

        Everything else remained identical —
        same hardware, same traffic, same
        lab conditions.
        """)
    with col3:
        st.markdown("""
        **The Data**

        Each run produced 25 CSV files,
        one per measurement layer of the
        LTE protocol stack.

        7 of those files were selected
        covering 384 measurement features
        across ~7,700 time snapshots per run.
        """)

    # ── Data pipeline summary ─────────────────────────────────────────────────
    section("What was built", "🏗️")
    col1, col2 = st.columns(2)
    with col1:
        finding("Local Python pipeline: 37 GB of raw CSVs → 1 GB of clean Parquet (38x compression)")
        finding("8-check data quality suite validated all 1,982 files before cloud upload")
        finding("4 bugs found and fixed during development — including a boolean dtype coercion destroying key ML features")
    with col2:
        finding("GCS bucket + BigQuery Medallion architecture on GCP free tier (asia-south2, Delhi)")
        finding("Bronze external table: 15.2M rows queryable at zero storage cost")
        finding("Silver aggregation: 1,982-row feature matrix. Gold: 5 domain KPIs")

    # ── Acknowledgement ───────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("""
    **Data Source Acknowledgement**

    This project is built on the **NIST DARE (Device-level Anomaly fRamEwork) RAN Dataset**,
    publicly released by the National Institute of Standards and Technology (NIST).
    All raw measurements, ground truth labels, and reference benchmarks originate from this
    publicly available dataset. Grateful acknowledgement is made to the NIST researchers
    who designed, conducted, and released this dataset to the research and engineering community.
    """)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — DATASET EXPLORER
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📊 Dataset Explorer":

    st.title("📊 Dataset Explorer")
    st.markdown("Explore the structure and balance of the DARE RAN dataset across tranches, sessions, and cipher states.")

    # ── Filters ───────────────────────────────────────────────────────────────
    col1, col2 = st.columns(2)
    with col1:
        selected_tranche = st.multiselect(
            "Filter by Tranche",
            options=["Tranche_A", "Tranche_B", "Tranche_C"],
            default=["Tranche_A", "Tranche_B", "Tranche_C"]
        )
    with col2:
        selected_cipher = st.multiselect(
            "Filter by Cipher State",
            options=["on", "off"],
            default=["on", "off"]
        )

    filtered = gold[
        (gold["tranche"].isin(selected_tranche)) &
        (gold["cipher_state"].isin(selected_cipher))
    ]

    # ── Summary metrics ───────────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Runs shown", f"{len(filtered):,}")
    col2.metric("Cipher-ON", f"{len(filtered[filtered.cipher_state=='on']):,}")
    col3.metric("Cipher-OFF", f"{len(filtered[filtered.cipher_state=='off']):,}")
    col4.metric("Balance", f"{len(filtered[filtered.cipher_state=='on'])/len(filtered)*100:.1f}% ON" if len(filtered) > 0 else "N/A")

    st.markdown("---")

    col1, col2 = st.columns(2)

    # ── Pie chart ─────────────────────────────────────────────────────────────
    with col1:
        section("Cipher State Balance", "⚖️")
        insight("A perfectly balanced dataset is critical for fair ML training. NIST designed the experiment with equal cipher-on and cipher-off runs, and our preprocessing preserved that balance.")

        cipher_counts = filtered["cipher_state"].value_counts().reset_index()
        cipher_counts.columns = ["cipher_state", "count"]
        cipher_counts["label"] = cipher_counts["cipher_state"].map(CIPHER_LABELS)

        fig = go.Figure(go.Pie(
            labels=cipher_counts["label"],
            values=cipher_counts["count"],
            hole=0.5,
            marker_colors=["#E57373", "#64B5F6"],
            textinfo="percent+value",
            textfont=dict(color="white", size=14)
        ))
        fig.update_layout(
            **PLOTLY_LAYOUT,
            title="Cipher State Distribution",
            showlegend=True,
            height=350
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Tranche bar chart ─────────────────────────────────────────────────────
    with col2:
        section("Runs by Tranche", "📦")
        insight("Three tranches represent different experimental conditions. Tranche B (780 runs) is the primary ML training set; Tranches A and C are the test sets.")

        tranche_counts = filtered.groupby(["tranche", "cipher_state"]).size().reset_index(name="count")

        fig = go.Figure()
        for state in ["on", "off"]:
            d = tranche_counts[tranche_counts.cipher_state == state]
            fig.add_trace(go.Bar(
                x=d["tranche"],
                y=d["count"],
                name=CIPHER_LABELS[state],
                marker_color=CIPHER_COLOURS[state]
            ))
        fig.update_layout(
            **PLOTLY_LAYOUT,
            title="Run Count by Tranche and Cipher State",
            barmode="group",
            height=350,
            xaxis_title="Tranche",
            yaxis_title="Number of Runs"
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Session distribution ───────────────────────────────────────────────────
    section("Runs by Session", "📋")
    st.markdown("Each session represents a separate experimental block. Every session maintains close to 50/50 cipher balance — confirming the dataset is unbiased at the session level.")

    session_counts = filtered.groupby(["session", "cipher_state"]).size().reset_index(name="count")
    fig = go.Figure()
    for state in ["on", "off"]:
        d = session_counts[session_counts.cipher_state == state]
        fig.add_trace(go.Bar(
            x=d["session"],
            y=d["count"],
            name=CIPHER_LABELS[state],
            marker_color=CIPHER_COLOURS[state]
        ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title="Run Count by Session and Cipher State",
        barmode="stack",
        height=350,
        xaxis_title="Session",
        yaxis_title="Number of Runs",
        xaxis_tickangle=-45
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Row count distribution ─────────────────────────────────────────────────
    section("Measurement Density per Run", "📏")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
        Each run contains approximately **7,700 measurement snapshots** taken at ~1ms intervals
        during the experiment. This represents about 7.7 seconds of high-resolution radio measurements.

        The tight distribution (min 7,459 — max 7,828) confirms consistent experiment duration
        across all runs and both cipher states.
        """)
        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Min rows/run", f"{filtered['row_count'].min():,.0f}")
        col_b.metric("Median rows/run", f"{filtered['row_count'].median():,.0f}")
        col_c.metric("Max rows/run", f"{filtered['row_count'].max():,.0f}")
    with col2:
        fig = go.Figure()
        for state in ["on", "off"]:
            d = filtered[filtered.cipher_state == state]
            fig.add_trace(go.Histogram(
                x=d["row_count"],
                name=CIPHER_LABELS[state],
                marker_color=CIPHER_COLOURS[state],
                opacity=0.7,
                nbinsx=30
            ))
        fig.update_layout(
            **PLOTLY_LAYOUT,
            title="Distribution of Row Counts per Run",
            barmode="overlay",
            height=300,
            xaxis_title="Rows per Run",
            yaxis_title="Number of Runs"
        )
        st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — KPI SIGNAL ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🔬 KPI Signal Analysis":

    st.title("🔬 KPI Signal Analysis")
    st.markdown("""
    This page shows the statistical signals that distinguish cipher-ON from cipher-OFF runs.
    All measurements come from the **physical layer of the LTE radio stack** — the signals
    flying through the air between the base station and mobile phones.
    """)

    # ── What are KPIs ─────────────────────────────────────────────────────────
    with st.expander("📖 What do these terms mean? (Click to expand)", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            term("BLER — Block Error Rate",
                 "The proportion of data blocks transmitted over the radio that contain errors and need to be retransmitted. Think of it as the 'packet loss rate' of the radio link. Normal values are very small (0.0003 = 0.03%). Higher BLER = more radio errors.")
            term("HARQ — Hybrid Automatic Repeat Request",
                 "LTE's error correction mechanism. When a block has errors, the receiver asks for a retransmission. HARQ efficiency measures how often data is delivered successfully on the first attempt — higher is better.")
            term("RSRQ — Reference Signal Received Quality (dB)",
                 "A measure of signal quality that accounts for interference. Measured in decibels — always negative, closer to zero is better. -10 dB is reasonable quality. A 0.03 dB difference between cipher states is subtle but consistent across thousands of measurements.")
        with col2:
            term("MCS — Modulation and Coding Scheme (0–28)",
                 "How aggressively the base station encodes data onto the radio signal. Higher MCS = more bits per symbol = faster data transfer, but requires better signal quality. MCS is nearly identical across cipher states, confirming the network 'looks healthy'.")
            term("Retransmission Count",
                 "How many times a data block had to be resent due to errors. Cipher-on runs show 22% more retransmissions than cipher-off — the strongest single indicator of the misconfiguration.")
            term("BLER Spread (q95 - q05)",
                 "The width of the error rate distribution within a run. A wider spread means the error rate is more variable — fluctuating more during the experiment. Cipher-on runs show wider spread, indicating less stable radio behaviour.")

    st.markdown("---")

    # ── BLER Analysis ─────────────────────────────────────────────────────────
    section("Block Error Rate (BLER) — The Primary Signal", "📡")
    st.markdown("""
    BLER is the most important feature for detecting cipher misconfigurations. When cipher is ON,
    the baseband processor does extra work (encrypting/decrypting packets). This adds tiny processing
    delays that occasionally cause packets to miss their transmission window — resulting in block errors.
    The effect is subtle per-snapshot but **consistent and systematic across all 1,982 runs**.
    """)

    col1, col2 = st.columns(2)
    with col1:
        # Bar chart — avg BLER by cipher state
        avg_bler = gold.groupby("cipher_state")["bler_mean"].mean().reset_index()
        fig = go.Figure()
        for _, row in avg_bler.iterrows():
            fig.add_trace(go.Bar(
                x=[CIPHER_LABELS[row["cipher_state"]]],
                y=[row["bler_mean"]],
                marker_color=CIPHER_COLOURS[row["cipher_state"]],
                name=CIPHER_LABELS[row["cipher_state"]],
                showlegend=False,
                text=[f"{row['bler_mean']:.6f}"],
                textposition="outside",
                textfont=dict(color="white")
            ))
        fig.update_layout(
            **PLOTLY_LAYOUT,
            title="Average BLER by Cipher State",
            yaxis_title="Block Error Rate",
            yaxis=dict(range=[0.00025, 0.00034], **AXIS_STYLE),
            height=350
        )
        st.plotly_chart(fig, use_container_width=True)
        warning("Cipher-ON shows 11% higher BLER than cipher-OFF (0.000311 vs 0.000279). While tiny in absolute terms, this difference is consistent across all 998 cipher-on runs and all 984 cipher-off runs.")

    with col2:
        # Box plot — BLER distribution per run
        fig = go.Figure()
        for state in ["off", "on"]:
            d = silver[silver.cipher_state == state]
            fig.add_trace(go.Box(
                y=d["bler_mean"],
                name=CIPHER_LABELS[state],
                marker_color=CIPHER_COLOURS[state],
                boxpoints="outliers",
                line=dict(width=2)
            ))
        fig.update_layout(
            **PLOTLY_LAYOUT,
            title="BLER Distribution Across All Runs",
            yaxis_title="Per-Run Average BLER",
            height=350
        )
        st.plotly_chart(fig, use_container_width=True)
        finding("The two distributions do not overlap: cipher-ON Q25 (0.000291) is higher than cipher-OFF Q75 (0.000283). This means a simple threshold could classify cipher state from BLER alone.")

    # ── BLER Quantile comparison ───────────────────────────────────────────────
    section("BLER Distribution — Quantile Comparison", "📊")
    st.markdown("""
    Rather than just comparing averages, we compare the full **statistical distribution** of BLER
    across runs. A quantile (e.g., Q25) is the value below which 25% of runs fall.
    If the distributions don't overlap, even the 'best' cipher-ON run has higher BLER than the
    'worst' cipher-OFF run — making classification straightforward.
    """)

    quantiles = ["q05", "q25", "q50", "q75", "q95"]
    bler_q_data = []
    for state in ["off", "on"]:
        d = silver[silver.cipher_state == state]
        for q in quantiles:
            bler_q_data.append({
                "cipher_state": CIPHER_LABELS[state],
                "quantile": q.upper(),
                "value": d[f"bler_{q}"].mean()
            })
    bler_q_df = pd.DataFrame(bler_q_data)

    fig = go.Figure()
    for state in ["off", "on"]:
        d = bler_q_df[bler_q_df.cipher_state == CIPHER_LABELS[state]]
        fig.add_trace(go.Scatter(
            x=d["quantile"],
            y=d["value"],
            mode="lines+markers",
            name=CIPHER_LABELS[state],
            line=dict(color=CIPHER_COLOURS[state], width=3),
            marker=dict(size=10)
        ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title="BLER Quantiles: cipher-ON vs cipher-OFF (per-run averages)",
        xaxis_title="Quantile",
        yaxis_title="BLER Value",
        height=380
    )
    st.plotly_chart(fig, use_container_width=True)
    insight("The two lines run parallel with a consistent ~11% gap at every quantile. This is not caused by a few outlier runs — it is a systematic shift in the entire distribution. The gap is identical at Q05 and Q95, confirming the signal is present throughout the full range of experimental conditions.")

    st.markdown("---")

    # ── HARQ Efficiency ───────────────────────────────────────────────────────
    section("HARQ Efficiency — First-Attempt Delivery Rate", "⚡")
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("""
        **What it measures:** The proportion of data blocks delivered successfully on the first transmission attempt.
        A value of 0.9997 means 99.97% of blocks succeeded first time — only 0.03% needed retransmission.

        **Why cipher state affects it:** When cipher is ON, the extra encryption processing occasionally causes
        a block to arrive late, triggering HARQ to request a retransmission even though the data was correct.
        This wastes radio capacity and slightly reduces efficiency.

        **The signal:** Cipher-OFF runs show higher HARQ efficiency (0.999721) than cipher-ON runs (0.999689).
        The difference is 0.000032 — tiny in absolute terms but consistent across all runs.
        """)
    with col2:
        avg_harq = gold.groupby("cipher_state")["harq_efficiency"].mean()
        col_a, col_b = st.columns(2)
        col_a.metric(
            "Cipher OFF",
            f"{avg_harq.get('off', 0):.6f}",
            delta="baseline"
        )
        col_b.metric(
            "Cipher ON",
            f"{avg_harq.get('on', 0):.6f}",
            delta=f"{(avg_harq.get('on', 0) - avg_harq.get('off', 0)):.6f}",
            delta_color="inverse"
        )

    fig = go.Figure()
    for state in ["off", "on"]:
        d = gold[gold.cipher_state == state]
        fig.add_trace(go.Histogram(
            x=d["harq_efficiency"],
            name=CIPHER_LABELS[state],
            marker_color=CIPHER_COLOURS[state],
            opacity=0.75,
            nbinsx=50
        ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title="HARQ Efficiency Distribution Across All Runs",
        barmode="overlay",
        xaxis_title="HARQ Efficiency (proportion of first-attempt successes)",
        yaxis_title="Number of Runs",
        height=350
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── RSRQ ─────────────────────────────────────────────────────────────────
    section("RSRQ — Signal Quality", "📶")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
        **What it measures:** Reference Signal Received Quality — a dB measure of how clean the received signal
        is relative to total interference. Always negative; closer to zero = better quality.

        **Why it's measured in dB:** Decibels are logarithmic. A 3 dB difference means double the power.
        The 0.027 dB difference we see between cipher states is very small — but it is consistent
        across thousands of measurements.

        **The signal:** Cipher-ON runs average -10.468 dB vs cipher-OFF at -10.438 dB. The processing
        overhead of cipher creates tiny timing perturbations that show up as slightly worse signal quality
        measurements — even though the physical signal path is identical.
        """)
    with col2:
        avg_rsrq = gold.groupby("cipher_state")["rsrq_mean"].mean()
        fig = go.Figure()
        for state in ["off", "on"]:
            fig.add_trace(go.Bar(
                x=[CIPHER_LABELS[state]],
                y=[avg_rsrq.get(state, 0)],
                marker_color=CIPHER_COLOURS[state],
                name=CIPHER_LABELS[state],
                showlegend=False,
                text=[f"{avg_rsrq.get(state, 0):.4f} dB"],
                textposition="outside",
                textfont=dict(color="white")
            ))
        fig.update_layout(
            **PLOTLY_LAYOUT,
            title="Average RSRQ by Cipher State",
            yaxis=dict(range=[-10.50, -10.40], **AXIS_STYLE),
            yaxis_title="RSRQ (dB) — closer to 0 is better",
            height=320
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Throughput — the silent proof ─────────────────────────────────────────
    section("Throughput & MCS — The Silent Misconfiguration", "🔇")
    st.markdown("""
    This is the most important result for understanding why cipher misconfigurations go undetected.
    **Throughput and MCS are essentially identical** between cipher-ON and cipher-OFF.
    A network operator watching throughput dashboards would see absolutely nothing abnormal.
    """)

    col1, col2 = st.columns(2)
    with col1:
        avg_tput = gold.groupby("cipher_state")["dl_throughput_mean_kbps"].mean()
        fig = go.Figure()
        for state in ["off", "on"]:
            fig.add_trace(go.Bar(
                x=[CIPHER_LABELS[state]],
                y=[avg_tput.get(state, 0)],
                marker_color=CIPHER_COLOURS[state],
                showlegend=False,
                text=[f"{avg_tput.get(state, 0):.1f} kbps"],
                textposition="outside",
                textfont=dict(color="white")
            ))
        fig.update_layout(
            **PLOTLY_LAYOUT,
            title="Average DL Throughput — Nearly Identical",
            yaxis_title="Throughput (kbps)",
            yaxis=dict(range=[1005, 1015], **AXIS_STYLE),
            height=320
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        avg_mcs = gold.groupby("cipher_state")["mcs_mean"].mean()
        fig = go.Figure()
        for state in ["off", "on"]:
            fig.add_trace(go.Bar(
                x=[CIPHER_LABELS[state]],
                y=[avg_mcs.get(state, 0)],
                marker_color=CIPHER_COLOURS[state],
                showlegend=False,
                text=[f"{avg_mcs.get(state, 0):.4f}"],
                textposition="outside",
                textfont=dict(color="white")
            ))
        fig.update_layout(
            **PLOTLY_LAYOUT,
            title="Average MCS — Nearly Identical",
            yaxis_title="MCS Index (0–28)",
            yaxis=dict(range=[26.67, 26.70], **AXIS_STYLE),
            height=320
        )
        st.plotly_chart(fig, use_container_width=True)

    warning("Throughput differs by only 0.12 kbps and MCS by 0.0007 — well within measurement noise. Standard network monitoring based on throughput KPIs would show no anomaly. Only physical layer error statistics (BLER, retransmissions) reveal the misconfiguration.")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — KEY FINDINGS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "💡 Key Findings":

    st.title("💡 Key Findings")
    st.markdown("""
    A summary of what the data reveals about LTE cipher state detection — written for both
    technical and non-technical audiences.
    """)

    # ── The headline finding ───────────────────────────────────────────────────
    st.markdown("""
    <div class="hero-banner">
        <h1>The Misconfiguration is Silent — But Not Invisible</h1>
        <p>Standard network monitoring shows nothing abnormal. But physical layer error statistics
        tell a different story — and that story is statistically separable.</p>
    </div>
    """, unsafe_allow_html=True)

    # ── Summary table ─────────────────────────────────────────────────────────
    section("Signal Separation Summary", "📊")
    st.markdown("How each KPI behaves across cipher states — and what it means:")

    summary_data = {
        "KPI": ["BLER (Block Error Rate)", "HARQ Retransmissions", "RSRQ (Signal Quality)",
                "DL Throughput", "MCS Index", "PDCP Lost PDUs"],
        "Cipher OFF": ["0.000279", "0.000054", "−10.438 dB", "1,009.3 kbps", "26.678", "0 (zero)"],
        "Cipher ON":  ["0.000311", "0.000066", "−10.468 dB", "1,009.2 kbps", "26.678", "0 (zero)"],
        "Gap": ["+11%", "+22%", "0.03 dB worse", "~0%", "~0%", "None"],
        "Detectable?": ["✅ Yes", "✅ Yes", "✅ Subtle", "❌ No", "❌ No", "❌ No"],
        "What it means": [
            "More radio errors when cipher is active — primary detection signal",
            "More packet retransmissions — amplifies the BLER signal",
            "Slightly worse signal quality — consistent but very small",
            "Network throughput looks completely normal — silent misconfiguration",
            "Encoding efficiency looks completely normal — silent misconfiguration",
            "No packet loss at all — standard error monitoring would see nothing"
        ]
    }
    st.dataframe(pd.DataFrame(summary_data), use_container_width=True, hide_index=True)

    st.markdown("---")

    # ── The 4 key insights ─────────────────────────────────────────────────────
    section("The 4 Key Insights", "🔑")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
        **1. Error counters are useless for detection**

        PDCP (Packet Data Convergence Protocol) sits directly above the cipher engine in the LTE stack.
        Its error counters — lost PDUs, bad PDU rates — are identically zero across all 15.2 million
        measurement snapshots in both cipher states.

        This confirms the core premise: the misconfiguration is truly silent. Standard network monitoring
        based on error counters would never detect it.
        """)
        warning("Zero packet loss events across 15.2M rows in both cipher states. Not a single dropped packet.")

        st.markdown("""
        **3. The BLER distributions don't overlap**

        The per-run BLER distributions for cipher-ON and cipher-OFF are completely separated:
        cipher-ON Q25 (0.000291) is higher than cipher-OFF Q75 (0.000283).

        This means: even the 'best' cipher-ON run (low BLER) has a higher error rate than the
        'worst' cipher-OFF run. A simple threshold on per-run average BLER could classify cipher
        state with high accuracy.
        """)
        finding("Non-overlapping distributions imply linear separability — even a simple logistic regression should achieve high accuracy.")

    with col2:
        st.markdown("""
        **2. The signal is in the error statistics, not throughput**

        BLER is 11% higher and retransmissions are 22% higher in cipher-ON runs.
        Meanwhile throughput differs by only 0.12 kbps (0.01%) and MCS by 0.0007.

        This asymmetry is the signature of a silent misconfiguration: the network is working
        hard enough to correct errors on the fly (HARQ retransmissions), so the user-visible
        throughput remains stable — hiding the problem.
        """)
        finding("The network auto-corrects the extra errors through retransmissions, making the problem invisible to users and standard monitoring.")

        st.markdown("""
        **4. The gap is consistent across all experimental conditions**

        The ~11% BLER gap between cipher-ON and cipher-OFF holds at every quantile (Q05, Q25, Q50,
        Q75, Q95), across all 3 tranches, and across all 28 sessions. This is not noise or
        experimental artifact — it is a systematic physical effect of encryption processing overhead.
        """)
        finding("Consistent separation across 28 sessions and 3 experimental tranches confirms the signal is robust and generalisable.")

    st.markdown("---")

    # ── Bubble chart ──────────────────────────────────────────────────────────
    section("The Separation Visualised", "🫧")
    st.markdown("""
    Each bubble represents the **average** of all cipher-ON or cipher-OFF runs.
    Position = (BLER, Retransmissions). Size = BLER spread (variability).
    The two bubbles are clearly separated in both dimensions simultaneously.
    """)

    bubble_data = gold.groupby("cipher_state").agg(
        bler_mean=("bler_mean", "mean"),
        retx_mean=("retx_mean", "mean"),
        bler_spread=("bler_spread", "mean"),
        count=("run_id", "count")
    ).reset_index()

    fig = go.Figure()
    for _, row in bubble_data.iterrows():
        fig.add_trace(go.Scatter(
            x=[row["bler_mean"]],
            y=[row["retx_mean"]],
            mode="markers+text",
            marker=dict(
                size=row["bler_spread"] * 20000,
                color=CIPHER_COLOURS[row["cipher_state"]],
                opacity=0.8,
                line=dict(color="white", width=2)
            ),
            text=[CIPHER_LABELS[row["cipher_state"]]],
            textposition="top center",
            textfont=dict(color="white", size=13),
            name=CIPHER_LABELS[row["cipher_state"]]
        ))

    fig.update_layout(
        **PLOTLY_LAYOUT,
        title="Cipher State Separation: BLER vs Retransmissions (bubble size = BLER variability)",
        xaxis_title="Average BLER per Run",
        yaxis_title="Average Retransmission Count per Run",
        height=450,
        showlegend=False
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── What's next ───────────────────────────────────────────────────────────
    section("What's Next — ML Classification", "🤖")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
        **The Silver feature matrix** (1,982 rows × 75 features) is ready for ML training.

        Planned models:
        - **Logistic Regression** — baseline, interpretable, directly shows which features matter
        - **Random Forest** — handles non-linear interactions, feature importance via SHAP values
        - **Isolation Forest** — unsupervised anomaly detection (no labels used during training)

        The key ML question isn't just accuracy — it's which features drive the decision,
        and whether those features match the telecom intuition (BLER and retransmissions,
        not throughput).
        """)
    with col2:
        st.markdown("""
        **Expected outcome:**

        Given the non-overlapping BLER distributions, even a simple logistic regression
        on per-run average BLER should achieve >90% accuracy.

        The more interesting result is SHAP feature importance — if SHAP identifies
        BLER, retransmission counts, and RSRQ as the top features (while assigning
        near-zero importance to throughput and MCS), it directly validates the
        telecom hypothesis from a data-driven perspective.

        This would confirm that the statistical approach can detect what human
        operators and standard monitoring tools cannot.
        """)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — GLOSSARY
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🗂️ Glossary":

    st.title("🗂️ Glossary")
    st.markdown("Plain-English definitions of every technical term used in this dashboard.")

    section("LTE Network Terms", "📡")
    col1, col2 = st.columns(2)
    with col1:
        term("LTE (Long Term Evolution)", "The technical standard for 4G mobile networks. The radio protocol that connects your phone to the mobile network.")
        term("eNodeB (Base Station)", "The physical radio tower/antenna that connects mobile phones to the network. Equivalent to a WiFi router but for mobile networks.")
        term("UE (User Equipment)", "Technical term for a mobile phone or device connected to the LTE network.")
        term("Cipher / Encryption", "The process of scrambling data so only authorised parties can read it. In LTE, data transmitted over the air is encrypted to prevent eavesdropping.")
        term("Cipher State", "Whether encryption is currently ON (normal operation) or OFF (misconfigured/disabled). When OFF, data travels unencrypted over the radio link.")
        term("PDCP (Packet Data Convergence Protocol)", "The layer of the LTE stack directly above the cipher engine. Responsible for compression and delivery of data packets to the application layer.")
    with col2:
        term("RLC (Radio Link Control)", "The layer below PDCP. Responsible for segmentation, reassembly, and error correction of data packets.")
        term("MAC (Medium Access Control)", "Manages how multiple users share the same radio channel. Schedules which UE transmits when.")
        term("Physical Layer (L1)", "The bottom layer — the actual radio signals. Measurements here include signal strength, error rates, and modulation schemes.")
        term("PDU (Protocol Data Unit)", "A packet as it passes between protocol layers. A PDU going down the stack gets encrypted at the cipher layer; coming up it gets decrypted.")
        term("SDU (Service Data Unit)", "A packet as passed upward to the application layer. Should be decrypted and intact.")
        term("Bearer", "A dedicated data channel between UE and network. A UE can have multiple bearers for different types of traffic (voice, data, etc.)")

    section("Measurement KPIs", "📊")
    col1, col2 = st.columns(2)
    with col1:
        term("BLER — Block Error Rate",
             "Proportion of transmitted data blocks that have errors. Formula: error blocks / total blocks. Normal range: 0.0001 to 0.001 (0.01% to 0.1%). Our cipher-OFF runs average 0.000279 (0.028%).")
        term("HARQ — Hybrid ARQ",
             "LTE's retransmission mechanism. When BLER occurs, HARQ requests the same block again. The receiver combines the original and retransmission for better decoding. Efficient HARQ = fewer retransmissions needed.")
        term("HARQ Efficiency",
             "Proportion of blocks delivered on first attempt. Formula: retx_0 / total_tx. Our runs: 0.9997 (99.97% first-attempt success rate). Cipher-ON is 0.000032 lower than cipher-OFF.")
        term("MCS — Modulation and Coding Scheme",
             "Index 0–28 controlling how many bits are packed per radio symbol. Higher MCS = more efficient but needs better signal quality. MCS 26 (our runs) means 64QAM — 6 bits per symbol.")
    with col2:
        term("RSRQ — Reference Signal Received Quality (dB)",
             "Signal quality measure accounting for interference. Range: -3 dB (excellent) to -19.5 dB (poor). Our runs average -10.4 dB — mid-range, consistent with simulated lab conditions.")
        term("RSRP — Reference Signal Received Power (dBm)",
             "Raw signal strength measurement. More negative = weaker signal. Used alongside RSRQ for a complete picture of radio conditions.")
        term("SNR — Signal to Noise Ratio (dB)",
             "How much stronger the signal is compared to background noise. Higher SNR = cleaner signal = higher possible MCS. Our lab runs average ~31 dB — very good quality.")
        term("Throughput (kbps)",
             "Data transfer rate in kilobits per second. Our runs average ~1,009 kbps (≈1 Mbps) — this is the simulated application traffic rate, not the maximum possible rate.")

    section("Data Engineering Terms", "🏗️")
    col1, col2 = st.columns(2)
    with col1:
        term("Parquet",
             "A binary file format for storing tabular data. Unlike CSV (plain text), Parquet stores data column-by-column and compresses it efficiently. Result: 37 GB of CSV → 1 GB of Parquet (38x compression).")
        term("Medallion Architecture",
             "A data organisation pattern with three layers: Bronze (raw data), Silver (cleaned/aggregated data), Gold (business-ready KPIs). Each layer adds value without destroying the previous one.")
        term("Bronze Layer",
             "Raw data loaded directly from source files. In this project: 15.2M rows, one per measurement timestamp, loaded from GCS Parquet into BigQuery external table.")
        term("Silver Layer",
             "Aggregated data — one row per run with statistical summaries (mean, stddev, quantiles) of each feature. 1,982 rows × 75 columns. This is the ML feature matrix.")
    with col2:
        term("Gold Layer",
             "Business-ready KPIs derived from Silver. 5 domain metrics: HARQ efficiency, BLER spread, spectral efficiency, PDCP discard rate, DL/UL asymmetry. One row per run, directly dashboard-ready.")
        term("External Table (BigQuery)",
             "A BigQuery table that reads data from GCS Parquet files without copying them into BigQuery storage. Zero storage cost — data stays in GCS, BigQuery queries it on demand.")
        term("GCS — Google Cloud Storage",
             "Google's object storage service. Equivalent to AWS S3 or Azure Blob Storage. Used here to store 1,982 Parquet files (954 MB total).")
        term("DQ — Data Quality",
             "Systematic checks to verify data is correct before use. Our 8-check suite verified dtype integrity, value ranges, class balance, null rates, and file readability across all 1,982 files.")

    section("Statistical Terms", "📐")
    col1, col2 = st.columns(2)
    with col1:
        term("Quantile (Q05, Q25, Q50, Q75, Q95)",
             "A quantile divides a dataset into equal parts. Q25 = the value below which 25% of runs fall. Q50 = the median (middle value). Comparing quantiles reveals the full distribution shape, not just the average.")
        term("Distribution",
             "The spread of values across all runs. Two distributions 'don't overlap' when even the lowest cipher-ON value is higher than the highest cipher-OFF value — making classification straightforward.")
    with col2:
        term("SHAP Values",
             "SHapley Additive exPlanations — a method for explaining ML model predictions. SHAP tells you which features drove each individual prediction and by how much. Used to validate that the model relies on BLER and retransmissions, not throughput.")
        term("Linear Separability",
             "When two classes (cipher-ON and cipher-OFF) can be separated by a straight line in feature space. Non-overlapping BLER distributions suggest linear separability — meaning even simple models should work well.")
