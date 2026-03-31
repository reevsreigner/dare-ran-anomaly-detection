# DARE RAN Anomaly Detection
### Can LTE radio KPI signatures alone reveal silent cipher misconfigurations — without triggering any network alarms?

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![Google Cloud](https://img.shields.io/badge/Google_Cloud-GCP-4285F4?style=flat&logo=google-cloud&logoColor=white)](https://cloud.google.com)
[![BigQuery](https://img.shields.io/badge/BigQuery-Medallion-4285F4?style=flat&logo=google-cloud&logoColor=white)](https://cloud.google.com/bigquery)
[![Streamlit](https://img.shields.io/badge/Streamlit-Live_Dashboard-FF4B4B?style=flat&logo=streamlit&logoColor=white)](https://streamlit.io)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat)](LICENSE)

---

## Live Dashboard

**[View the interactive dashboard →](https://your-app.streamlit.app)**

---

## Overview

This project applies production-grade data engineering to a real-world cybersecurity problem in 4G LTE networks. When encryption (cipher) is silently disabled through misconfiguration, user data travels unencrypted over the air — but no network alarm fires. Standard monitoring shows nothing abnormal.

The research question: even though the network looks healthy, do the statistical signatures of the radio channel change subtly enough to be detected?

Using the publicly available **NIST DARE (Device-level Anomaly fRamEwork) RAN dataset** — 2,033 real LTE experiment runs with cipher deliberately turned on or off — a complete data engineering pipeline was built on GCP free tier to process, store, and analyse the signal.

**Key finding:** PDCP error counters and throughput are identically zero/identical across both cipher states. But Block Error Rate is **11% higher** and retransmissions are **22% higher** in cipher-on runs, with non-overlapping per-run distributions — confirming strong linear separability for ML classification.

---

## Architecture

```
Raw CSVs (37 GB, local)
        │
        ▼
Python Preprocessing Pipeline
  • Filter to 7 essential UECOMBINED measurement groups
  • Time-trim first/last 5 seconds per run (paper §IV-B)
  • Align row counts across groups (min-rows merge)
  • Attach ground truth labels from SQLite metadata.db
  • 8-check pre-upload DQ validation suite
  • Serialise to Snappy-compressed Parquet
        │
        ▼
Google Cloud Storage (asia-south2)
  dare-raw-nist-anomaly/parquet/
  └── Tranche_A / Session_N / run_id.parquet
  └── Tranche_B / Session_N / run_id.parquet
  └── Tranche_C / Session_N / run_id.parquet
  1,982 files · 954 MB · 38x compression
        │
        ▼
BigQuery Medallion Architecture
  ┌─────────────────────────────────────────────┐
  │  Bronze  dare_bronze.raw_measurements        │
  │          External table over GCS Parquet     │
  │          15.2M rows · $0 storage cost        │
  ├─────────────────────────────────────────────┤
  │  Silver  dare_silver.run_features            │
  │          Per-run aggregations (quantiles,    │
  │          means, stddev) · 1,982 × 75 cols   │
  ├─────────────────────────────────────────────┤
  │  Gold    dare_gold.domain_kpis               │
  │          5 domain KPIs · 1,982 rows          │
  │          Dashboard-ready                     │
  └─────────────────────────────────────────────┘
        │
        ▼
Streamlit Dashboard (Streamlit Cloud — free)
  Connected live to BigQuery Gold + Silver
  8 pages · Interactive · Publicly shareable
```

---

## Key Findings

| Metric | Cipher OFF | Cipher ON | Gap | Detectable |
|--------|-----------|----------|-----|------------|
| BLER (Block Error Rate) | 0.000279 | 0.000311 | +11% | Yes |
| HARQ Retransmissions | 0.000054 | 0.000066 | +22% | Yes |
| RSRQ (Signal Quality) | −10.438 dB | −10.468 dB | 0.03 dB | Subtle |
| DL Throughput | 1,009.3 kbps | 1,009.2 kbps | ~0% | No |
| MCS Index | 26.678 | 26.678 | ~0% | No |
| PDCP Lost PDUs | 0 | 0 | None | No |

The cipher-on BLER Q25 (0.000291) is higher than cipher-off Q75 (0.000283) — the distributions do not overlap, confirming linear separability.

---

## Repository Structure

```
dare-ran-anomaly-detection/
│
├── scripts/
│   ├── preprocess.py           # Main preprocessing pipeline
│   ├── validate_parquet.py     # Post-processing validation against NIST benchmarks
│   ├── dq_check.py             # 8-check pre-upload DQ suite
│   ├── find_bad_files.py       # Identifies INT32/FLOAT64 type conflicts
│   ├── fix_retx_dtype.py       # Fixes retransmission column dtype issues
│   ├── explore_dataset.py      # Initial dataset exploration
│   └── create_silver_table.sql # Silver aggregation SQL
│
├── streamlit_app/
│   ├── app.py                  # Main Streamlit dashboard
│   ├── requirements.txt        # Python dependencies
│   └── .streamlit/
│       └── secrets.toml.template
│
├── terraform/
│   ├── main.tf                 # GCP infrastructure as code
│   └── variables.tf
│
├── docker/
│   └── ingestion/
│       ├── Dockerfile
│       └── ingest_data.py
│
├── dbt/
│   └── dbt_project.yml
│
├── notebooks/
│   └── nist_eda.ipynb
│
└── README.md
```

---

## Pipeline Details

### Preprocessing (`scripts/preprocess.py`)

Transforms raw LTE CSVs into clean, labelled Parquet files.

**Input:** 25 CSV files per run × 2,033 runs = 37 GB  
**Output:** 1 Parquet file per run × 1,982 runs = 1 GB

**7 essential measurement groups selected:**

| Group | Protocol Layer | Key Signals | Features |
|-------|---------------|-------------|----------|
| UECOMBINED_L1DLCARRIERSTATS | Physical (L1) | BLER, MCS, SNR, retransmissions | 130 |
| UECOMBINED_L1CARRIERPOWERS | Physical (L1) | RSRP, RSRQ | 8 |
| UECOMBINED_MACRXSTATS_ALL | MAC | PDU sizes | 60 |
| UECOMBINED_MACTXSTATS_ALL | MAC | Transmit stats | 13 |
| UECOMBINED_RLCRXSTATS_ALL | RLC | RLC receive | 76 |
| UECOMBINED_RLCTXSTATS_ALL | RLC | Retransmissions | 27 |
| UECOMBINED_PDCPRXSTATS_ALL | PDCP | Lost/bad PDUs | 70 |
| | | **Total** | **384** |

**4 bugs found and fixed during development:**

1. **Wrong file groups** — script was reading all 25 groups; fixed to 7 essential UECOMBINED groups only (60% I/O reduction)
2. **Low-frequency bottleneck** — 3 summary groups (15–470 rows/run) included in essential set, causing min-rows merge to truncate 99% of data; removed
3. **DataFrame fragmentation** — repeated `merged.insert()` calls replaced with single `pd.concat([meta, merged], axis=1)`
4. **Boolean dtype coercion** — `optimise_dtypes()` was converting float columns with only 0/1 values to boolean, destroying `lost_pdus` and `bad_pdu_rate`; fixed by always downcasting to float32

### Data Quality Suite (`scripts/dq_check.py`)

8 checks run across all 1,982 Parquet files before upload:

| Check | Description | Result |
|-------|-------------|--------|
| 1 — Boolean dtype | No measurement columns stored as boolean | PASS |
| 2 — Value ranges | BLER 0–1, MCS 0–28, SNR −30 to 60 dB | PASS |
| 3 — Row counts | Median ~7,700 rows/run, no outliers | PASS |
| 4 — Column counts | Schema drift 118–180 cols (expected) | PASS |
| 5 — Cipher balance | Every session 49–53% cipher-on | PASS |
| 6 — Null rates | No key column >1% null | PASS |
| 7 — Timestamps | No suspicious duplicate timestamps | PASS |
| 8 — Readability | All files openable by PyArrow | PASS |

### Pipeline Validation

Two NIST benchmark values confirmed pipeline correctness:

- **x182** (MAC PDU size q0.25): paper target 268.2–268.7 bytes → pipeline produced **268.45** ✓
- **x15** (RSRQ cipher separation): cipher-on median **0.387**, cipher-off **0.664** ✓

---

## BigQuery SQL

### Bronze External Table

The Bronze layer is a BigQuery external table pointing directly at GCS Parquet files — no data copied, zero storage cost.

```sql
CREATE OR REPLACE EXTERNAL TABLE dare_bronze.raw_measurements
OPTIONS (
  format = "PARQUET",
  uris = [
    "gs://dare-raw-nist-anomaly/parquet/Tranche_A/Session_1/*.parquet",
    -- ... 28 session URIs total
  ]
);
```

### Silver Aggregation

Collapses 15.2M Bronze rows into a 1,982-row per-run feature matrix using quantiles, means, and standard deviations. See `scripts/create_silver_table.sql`.

### Gold Domain KPIs

5 domain KPIs derived from Silver:

| KPI | Formula | Signal Direction |
|-----|---------|-----------------|
| `harq_efficiency` | retx_0 / retx_total | Lower in cipher-on |
| `bler_spread` | bler_q95 − bler_q05 | Wider in cipher-on |
| `spectral_efficiency` | dl_throughput / mcs | No difference |
| `pdcp_discard_rate` | lost_pdus / sdu_throughput | Zero in both |
| `dl_ul_asymmetry` | dl_throughput / mac_tx_throughput | No difference |

---

## ML Framework

Three models planned using the Silver feature matrix (1,982 rows × 75 features):

**Training set:** Tranche_B (759 runs)  
**Test set:** Tranche_A + Tranche_C (1,223 runs) — cross-tranche generalisation

| Model | Type | Expected Accuracy | Key Output |
|-------|------|------------------|------------|
| Logistic Regression | Supervised | >90% | Feature coefficients |
| Random Forest | Supervised | >88% cross-tranche | SHAP feature importance |
| Isolation Forest | Unsupervised | >75% AUC-ROC | Anomaly scores without labels |

---

## GCP Infrastructure

| Resource | Configuration |
|----------|--------------|
| GCP Project | nist-anomaly-de-2026 |
| Region | asia-south2 (Delhi) |
| GCS Bucket | dare-raw-nist-anomaly |
| Storage Class | Standard |
| BigQuery Datasets | dare_bronze, dare_silver, dare_gold, dare_reference |
| External Table | dare_bronze.raw_measurements (15.2M rows) |

---

## Setup & Reproduction

### Prerequisites

```bash
pip install pandas pyarrow google-cloud-bigquery streamlit plotly
```

### Run preprocessing

```bash
python scripts/preprocess.py \
  --dare_root "/path/to/DARE_Data_Public/Full_dataset" \
  --output_dir "/path/to/output" \
  --tranche Tranche_B
```

### Run DQ checks

```bash
python scripts/dq_check.py --output_dir "/path/to/output"
```

### Upload to GCS

```bash
gsutil -m cp -r /path/to/output/parquet gs://your-bucket/parquet/
```

### Run dashboard locally

```bash
cd streamlit_app
streamlit run app.py
```

Configure `.streamlit/secrets.toml` with your GCP service account credentials (see `secrets.toml.template`).

---

## Data Source Acknowledgement

This project is built on the **NIST DARE (Device-level Anomaly fRamEwork) RAN Dataset**, publicly released by the National Institute of Standards and Technology (NIST). The dataset comprises 2,033 real LTE experiment runs collected at the NIST laboratory covering controlled cipher-on and cipher-off conditions across multiple UE configurations and sessions.

Grateful acknowledgement is made to the NIST researchers who designed, conducted, and released this dataset to the research and engineering community.

---

## Author

**Krishna Kumar Jha**  
Data Engineering · Network Analysis  
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Krishna_Kumar_Jha-0A66C2?style=flat&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/krishna-kumar-jha-018aa8173/)
[![GitHub](https://img.shields.io/badge/GitHub-reevsreigner-181717?style=flat&logo=github&logoColor=white)](https://github.com/reevsreigner)
