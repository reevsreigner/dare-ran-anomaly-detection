-- =============================================================================
-- DARE RAN — Silver Layer Aggregation
-- Table: dare_silver.run_features
-- Purpose: Collapse 15.2M Bronze rows into 1,982-row per-run feature matrix.
--          One row per run. Statistical summaries of key KPI columns.
--          This is the feature matrix that feeds ML and Gold KPI computation.
-- Run in: BigQuery SQL editor (dare_bronze must exist and be populated)
-- Author: Krishna Jha | March 2026
-- =============================================================================

CREATE OR REPLACE TABLE dare_silver.run_features AS

WITH
-- ── Step 1: Compute per-run quantiles for every key feature column ─────────
-- APPROX_QUANTILES(x, 20) returns 21 values: [0%, 5%, 10%, ..., 95%, 100%]
-- OFFSET(1)  = 5th  percentile
-- OFFSET(5)  = 25th percentile
-- OFFSET(10) = 50th percentile (median)
-- OFFSET(15) = 75th percentile
-- OFFSET(19) = 95th percentile

run_stats AS (
  SELECT
    -- ── Identity columns ──────────────────────────────────────────────────
    run_id,
    cipher_state,
    tranche,
    session,
    ANY_VALUE(measurement_ts)   AS measurement_ts,
    COUNT(*)                    AS row_count,

    -- ── BLER (Block Error Rate) ───────────────────────────────────────────
    -- Key feature: cipher-on shows ~11% higher BLER than cipher-off
    -- x42 benchmark from NIST paper
    ROUND(AVG(l1_dl_carrier__dl_sch_bler), 8)                                                       AS bler_mean,
    ROUND(STDDEV(l1_dl_carrier__dl_sch_bler), 8)                                                    AS bler_stddev,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__dl_sch_bler, 20)[OFFSET(1)],  8)                         AS bler_q05,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__dl_sch_bler, 20)[OFFSET(5)],  8)                         AS bler_q25,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__dl_sch_bler, 20)[OFFSET(10)], 8)                         AS bler_q50,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__dl_sch_bler, 20)[OFFSET(15)], 8)                         AS bler_q75,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__dl_sch_bler, 20)[OFFSET(19)], 8)                         AS bler_q95,
    ROUND(COUNTIF(l1_dl_carrier__dl_sch_bler > 0) / COUNT(*), 6)                                   AS bler_nonzero_rate,

    -- ── MCS (Modulation and Coding Scheme, 0–28) ──────────────────────────
    -- Higher MCS = more bits per symbol = faster but needs better signal
    ROUND(AVG(l1_dl_carrier__mean_mcs), 6)                                                          AS mcs_mean,
    ROUND(STDDEV(l1_dl_carrier__mean_mcs), 6)                                                       AS mcs_stddev,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__mean_mcs, 20)[OFFSET(5)],  6)                            AS mcs_q25,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__mean_mcs, 20)[OFFSET(10)], 6)                            AS mcs_q50,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__mean_mcs, 20)[OFFSET(15)], 6)                            AS mcs_q75,

    -- ── SNR (Signal to Noise Ratio, dB) ───────────────────────────────────
    ROUND(AVG(l1_dl_carrier__snr__cw__0__db_), 6)                                                  AS snr_mean,
    ROUND(STDDEV(l1_dl_carrier__snr__cw__0__db_), 6)                                               AS snr_stddev,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__snr__cw__0__db_, 20)[OFFSET(5)],  6)                    AS snr_q25,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__snr__cw__0__db_, 20)[OFFSET(10)], 6)                    AS snr_q50,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__snr__cw__0__db_, 20)[OFFSET(15)], 6)                    AS snr_q75,

    -- ── HARQ Retransmissions ──────────────────────────────────────────────
    -- cipher-on shows ~22% more retransmissions than cipher-off
    -- x148 benchmark from NIST paper
    ROUND(AVG(l1_dl_carrier__average_retransmission_count), 8)                                      AS retx_mean,
    ROUND(STDDEV(l1_dl_carrier__average_retransmission_count), 8)                                   AS retx_stddev,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__average_retransmission_count, 20)[OFFSET(10)], 8)        AS retx_q50,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__average_retransmission_count, 20)[OFFSET(15)], 8)        AS retx_q75,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__average_retransmission_count, 20)[OFFSET(19)], 8)        AS retx_q95,
    ROUND(COUNTIF(l1_dl_carrier__average_retransmission_count > 0) / COUNT(*), 6)                  AS retx_nonzero_rate,

    -- ── HARQ Transport Block Size ─────────────────────────────────────────
    -- x182 benchmark: NIST paper target 268.2–268.7 bytes
    ROUND(AVG(l1_dl_carrier__mean_harq_tb_size__cw__0__bytes_), 6)                                 AS harq_tb_size_mean,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__mean_harq_tb_size__cw__0__bytes_, 20)[OFFSET(5)],  6)   AS harq_tb_size_q25,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__mean_harq_tb_size__cw__0__bytes_, 20)[OFFSET(10)], 6)   AS harq_tb_size_q50,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__mean_harq_tb_size__cw__0__bytes_, 20)[OFFSET(15)], 6)   AS harq_tb_size_q75,

    -- ── DL Throughput (kbps) ──────────────────────────────────────────────
    -- Nearly identical across cipher states — confirms silent misconfiguration
    ROUND(AVG(l1_dl_carrier__dl_sch_throughput_kbps_), 4)                                          AS dl_throughput_mean,
    ROUND(STDDEV(l1_dl_carrier__dl_sch_throughput_kbps_), 4)                                       AS dl_throughput_stddev,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__dl_sch_throughput_kbps_, 20)[OFFSET(5)],  4)            AS dl_throughput_q25,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__dl_sch_throughput_kbps_, 20)[OFFSET(10)], 4)            AS dl_throughput_q50,
    ROUND(APPROX_QUANTILES(l1_dl_carrier__dl_sch_throughput_kbps_, 20)[OFFSET(15)], 4)            AS dl_throughput_q75,

    -- ── RSRQ (Reference Signal Received Quality, dB) ─────────────────────
    -- x15 benchmark: cipher-on ~0.027 dB lower than cipher-off
    ROUND(AVG(`l1_powers__real_rsrq__pcc__db_`), 6)                                                AS rsrq_mean,
    ROUND(STDDEV(`l1_powers__real_rsrq__pcc__db_`), 6)                                             AS rsrq_stddev,
    ROUND(APPROX_QUANTILES(`l1_powers__real_rsrq__pcc__db_`, 20)[OFFSET(1)],  6)                  AS rsrq_q05,
    ROUND(APPROX_QUANTILES(`l1_powers__real_rsrq__pcc__db_`, 20)[OFFSET(5)],  6)                  AS rsrq_q25,
    ROUND(APPROX_QUANTILES(`l1_powers__real_rsrq__pcc__db_`, 20)[OFFSET(10)], 6)                  AS rsrq_q50,
    ROUND(APPROX_QUANTILES(`l1_powers__real_rsrq__pcc__db_`, 20)[OFFSET(15)], 6)                  AS rsrq_q75,
    ROUND(APPROX_QUANTILES(`l1_powers__real_rsrq__pcc__db_`, 20)[OFFSET(19)], 6)                  AS rsrq_q95,

    -- ── RSRP (Reference Signal Received Power, dBm) ───────────────────────
    ROUND(AVG(`l1_powers__real_rsrp__pcc__dbm_`), 6)                                               AS rsrp_mean,
    ROUND(STDDEV(`l1_powers__real_rsrp__pcc__dbm_`), 6)                                            AS rsrp_stddev,
    ROUND(APPROX_QUANTILES(`l1_powers__real_rsrp__pcc__dbm_`, 20)[OFFSET(5)],  6)                 AS rsrp_q25,
    ROUND(APPROX_QUANTILES(`l1_powers__real_rsrp__pcc__dbm_`, 20)[OFFSET(10)], 6)                 AS rsrp_q50,
    ROUND(APPROX_QUANTILES(`l1_powers__real_rsrp__pcc__dbm_`, 20)[OFFSET(15)], 6)                 AS rsrp_q75,

    -- ── MAC RX Throughput ─────────────────────────────────────────────────
    ROUND(AVG(mac_rx__aggregate_sdu_throughput_kbps_), 4)                                          AS mac_rx_agg_tput_mean,
    ROUND(APPROX_QUANTILES(mac_rx__aggregate_sdu_throughput_kbps_, 20)[OFFSET(10)], 4)            AS mac_rx_agg_tput_q50,
    ROUND(AVG(mac_rx__pdu_throughput_kbps_), 4)                                                    AS mac_rx_pdu_tput_mean,
    ROUND(APPROX_QUANTILES(mac_rx__pdu_throughput_kbps_, 20)[OFFSET(10)], 4)                      AS mac_rx_pdu_tput_q50,

    -- ── MAC TX Throughput ─────────────────────────────────────────────────
    ROUND(AVG(mac_tx__aggregate_sdu_throughput_kbps_), 4)                                          AS mac_tx_agg_tput_mean,
    ROUND(APPROX_QUANTILES(mac_tx__aggregate_sdu_throughput_kbps_, 20)[OFFSET(10)], 4)            AS mac_tx_agg_tput_q50,

    -- ── PDCP RX — cipher-proximate layer ─────────────────────────────────
    -- Lost PDUs confirmed zero across all runs — kept for completeness
    ROUND(AVG(pdcp_rx__lost_pdus), 8)                                                              AS pdcp_lost_pdus_mean,
    ROUND(AVG(pdcp_rx__lost_pdu_rate_packet_sec_), 8)                                              AS pdcp_lost_pdu_rate_mean,
    ROUND(AVG(pdcp_rx__aggregate_lost_pdu_rate_packet_sec_), 8)                                    AS pdcp_agg_lost_pdu_rate_mean,
    ROUND(AVG(pdcp_rx__sdu_throughput_kbps_), 4)                                                   AS pdcp_sdu_tput_mean,
    ROUND(APPROX_QUANTILES(pdcp_rx__sdu_throughput_kbps_, 20)[OFFSET(10)], 4)                     AS pdcp_sdu_tput_q50,
    ROUND(AVG(pdcp_rx__aggregate_sdu_throughput_kbps_), 4)                                         AS pdcp_agg_sdu_tput_mean,

    -- ── RLC RX ────────────────────────────────────────────────────────────
    ROUND(AVG(rlc_rx__lost_pdus), 8)                                                               AS rlc_lost_pdus_mean,
    ROUND(AVG(rlc_rx__lost_pdu_rate_packet_sec_), 8)                                               AS rlc_lost_pdu_rate_mean,
    ROUND(AVG(rlc_rx__sdu_throughput_kbps_), 4)                                                    AS rlc_rx_sdu_tput_mean,
    ROUND(APPROX_QUANTILES(rlc_rx__sdu_throughput_kbps_, 20)[OFFSET(10)], 4)                      AS rlc_rx_sdu_tput_q50,
    ROUND(AVG(rlc_rx__total_throughput_kbps_), 4)                                                  AS rlc_rx_total_tput_mean,

    -- ── Retransmission breakdown (Retx_0 = first attempt success) ─────────
    -- Retx_0 = packets delivered on first attempt (no HARQ needed)
    -- Retx_1+ = packets needing 1, 2, 3... retransmissions
    -- Ratio of Retx_1+ to total = HARQ efficiency proxy
    ROUND(AVG(l1_dl_carrier__retransmission_0), 6)                                                 AS retx_0_mean,
    ROUND(AVG(l1_dl_carrier__retransmission_1), 6)                                                 AS retx_1_mean,
    ROUND(AVG(l1_dl_carrier__retransmission_2), 6)                                                 AS retx_2_mean,
    ROUND(AVG(l1_dl_carrier__retransmission_3), 6)                                                 AS retx_3_mean,

    -- ── Derived: total retransmissions needed (Retx_1 + Retx_2 + ...) ─────
    -- Used in Gold to compute harq_efficiency domain KPI
    ROUND(AVG(
      l1_dl_carrier__retransmission_1 +
      l1_dl_carrier__retransmission_2 +
      l1_dl_carrier__retransmission_3 +
      l1_dl_carrier__retransmission_4 +
      l1_dl_carrier__retransmission_5
    ), 6)                                                                                           AS retx_1plus_mean,

    ROUND(AVG(
      l1_dl_carrier__retransmission_0 +
      l1_dl_carrier__retransmission_1 +
      l1_dl_carrier__retransmission_2 +
      l1_dl_carrier__retransmission_3 +
      l1_dl_carrier__retransmission_4 +
      l1_dl_carrier__retransmission_5
    ), 6)                                                                                           AS retx_total_mean

  FROM dare_bronze.raw_measurements
  GROUP BY run_id, cipher_state, tranche, session
)

-- ── Step 2: Select final Silver columns in logical order ──────────────────
SELECT
  -- Identity
  run_id,
  cipher_state,
  tranche,
  session,
  measurement_ts,
  row_count,

  -- BLER features
  bler_mean,
  bler_stddev,
  bler_q05,
  bler_q25,
  bler_q50,
  bler_q75,
  bler_q95,
  bler_nonzero_rate,

  -- MCS features
  mcs_mean,
  mcs_stddev,
  mcs_q25,
  mcs_q50,
  mcs_q75,

  -- SNR features
  snr_mean,
  snr_stddev,
  snr_q25,
  snr_q50,
  snr_q75,

  -- Retransmission features
  retx_mean,
  retx_stddev,
  retx_q50,
  retx_q75,
  retx_q95,
  retx_nonzero_rate,
  retx_0_mean,
  retx_1_mean,
  retx_2_mean,
  retx_3_mean,
  retx_1plus_mean,
  retx_total_mean,

  -- HARQ TB size (x182 benchmark)
  harq_tb_size_mean,
  harq_tb_size_q25,
  harq_tb_size_q50,
  harq_tb_size_q75,

  -- DL throughput
  dl_throughput_mean,
  dl_throughput_stddev,
  dl_throughput_q25,
  dl_throughput_q50,
  dl_throughput_q75,

  -- RSRQ (x15 benchmark)
  rsrq_mean,
  rsrq_stddev,
  rsrq_q05,
  rsrq_q25,
  rsrq_q50,
  rsrq_q75,
  rsrq_q95,

  -- RSRP
  rsrp_mean,
  rsrp_stddev,
  rsrp_q25,
  rsrp_q50,
  rsrp_q75,

  -- MAC throughput
  mac_rx_agg_tput_mean,
  mac_rx_agg_tput_q50,
  mac_rx_pdu_tput_mean,
  mac_rx_pdu_tput_q50,
  mac_tx_agg_tput_mean,
  mac_tx_agg_tput_q50,

  -- PDCP (cipher-proximate)
  pdcp_lost_pdus_mean,
  pdcp_lost_pdu_rate_mean,
  pdcp_agg_lost_pdu_rate_mean,
  pdcp_sdu_tput_mean,
  pdcp_sdu_tput_q50,
  pdcp_agg_sdu_tput_mean,

  -- RLC
  rlc_lost_pdus_mean,
  rlc_lost_pdu_rate_mean,
  rlc_rx_sdu_tput_mean,
  rlc_rx_sdu_tput_q50,
  rlc_rx_total_tput_mean

FROM run_stats
ORDER BY tranche, session, run_id;
