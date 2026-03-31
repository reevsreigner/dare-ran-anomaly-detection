"""
DARE RAN Dataset — Local Preprocessing Pipeline
================================================
Converts raw CSV files to Snappy-compressed Parquet before GCP upload.

CONFIRMED FACTS (from Folder_structure.txt analysis):
  - 2,033 run folders  |  53,699 files  |  37.87 GB total
  - Files are CSV (NOT xlsx)
  - Each run has 25-32 CSV files in traffic_generator/
  - All CSVs in one run share the same timestamp: YYYYMMDD_HHMMSS_GROUPNAME.csv
  - metadata.db lives at Session level (one per session, not per run)
  - Reference files at: Full_dataset/variable_name_key.csv
  - NIST processed output at: Full_dataset/Tranche_X/processed_data_X.csv

FOLDER STRUCTURE:
  E:/DARE_Data_Public/Full_dataset/
  ├── variable_name_key.csv
  ├── Measurands_and_Measurement_Terms.xlsx
  ├── Tranche_A/
  │   ├── processed_data_A.csv   <- NIST's own output, use for validation
  │   ├── MeasurementNotes.xlsx
  │   ├── Session_1/
  │   │   ├── metadata.db        <- cipher labels for ALL runs in this session
  │   │   ├── cipheron_0001/
  │   │   │   └── traffic_generator/
  │   │   │       ├── 220909_125427_L1CELLDLOVERVIEW.csv
  │   │   │       ├── 220909_125427_UECOMBINED_L1DLCARRIERSTATS.csv
  │   │   │       └── ... (25 files, all same timestamp prefix)
  │   │   └── cipheroff_0003/
  │   │       └── traffic_generator/
  ├── Tranche_B/   (13 sessions x 60 runs each)
  └── Tranche_C/   (7  sessions x 60 runs each)

OUTPUT:
  E:/dare_preprocessed/
  ├── manifest.csv               <- upload this to GCS alongside parquet/
  ├── quarantine/                <- runs that failed checks (review these)
  └── parquet/
      ├── Tranche_A/
      │   └── Session_1/
      │       ├── cipheron_0001.parquet
      │       └── cipheroff_0003.parquet
      ├── Tranche_B/
      └── Tranche_C/

USAGE:
  # Process one tranche (recommended — start with Tranche_B)
  python scripts/preprocess.py --dare_root "E:/DARE_Data_Public/Full_dataset" --output_dir "E:/dare_preprocessed" --tranche Tranche_B

  # Process all tranches
  python scripts/preprocess.py --dare_root "E:/DARE_Data_Public/Full_dataset" --output_dir "E:/dare_preprocessed"

  # Faster test run on priority groups only
  python scripts/preprocess.py --dare_root "E:/DARE_Data_Public/Full_dataset" --output_dir "E:/dare_preprocessed" --tranche Tranche_B --priority_only

REQUIREMENTS:
  pip install pandas pyarrow tqdm
"""

import argparse
import csv as csv_module
import logging
import re
import sqlite3
import sys
import textwrap
import warnings
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
TRIM_SECONDS   = 5         # Remove first/last N seconds per run (paper §IV-B)
COMPRESSION    = "snappy"  # Best ratio/speed for BigQuery
ROW_GROUP_SIZE = 50_000    # Optimal for BigQuery external table scans

# ESSENTIAL groups: the ONLY 9 CSV files that produce x1-x424 features.
# Cross-referenced against variable_name_key.csv — every other group
# (L1DLSTATS_NA, L1CELLWATCH_NA, SCCSTATUS, throughput grids, etc.)
# contains raw subframe data with NO mapping in the feature set.
# Skipping them cuts read volume by ~60% with zero loss of signal.
ESSENTIAL_GROUPS = {
    "UECOMBINED_L1DLCARRIERSTATS",   # 130 x-features: BLER, MCS, throughput (x42)
    "UECOMBINED_L1CARRIERPOWERS",    #   8 x-features: RSRP, RSRQ (x15)
    "UECOMBINED_MACRXSTATS_ALL",     #  60 x-features: MAC PDU sizes (x182)
    "UECOMBINED_MACTXSTATS_ALL",     #  13 x-features: MAC TX stats
    "UECOMBINED_RLCRXSTATS_ALL",     #  76 x-features: RLC RX stats
    "UECOMBINED_RLCTXSTATS_ALL",     #  27 x-features: retransmissions (x148)
    "UECOMBINED_PDCPRXSTATS_ALL",    #  70 x-features: PDCP discard (cipher-proximate)
    # Removed low-frequency summary groups that bottleneck min_rows merge:
    #   L1CELLDLOVERVIEW        (~470 rows/run) — was capping all runs to 470 rows
    #   SYSOVERVIEW_NA          (~15-30 rows/run) — was causing 154 quarantines
    #   L1CELLDLCARRIEROVERVIEW (~15-30 rows/run) — same issue
    # Only the 7 UECOMBINED groups remain — all high-frequency, thousands of rows/run
}
# Total: 384 x-features from 7 groups (all paper benchmarks + domain KPIs intact)
# All groups are high-frequency measurement files — row counts will be consistent

# Clean column prefix mapping — avoids 50-char prefixes on every column
GROUP_PREFIX_MAP = {
    "UECOMBINED_L1DLCARRIERSTATS":  "l1_dl_carrier",
    "UECOMBINED_L1ULCARRIERSTATS":  "l1_ul_carrier",
    "UECOMBINED_L1CARRIERPOWERS":   "l1_powers",
    "UECOMBINED_MACRXSTATS_ALL":    "mac_rx",
    "UECOMBINED_MACTXSTATS_ALL":    "mac_tx",
    "UECOMBINED_PDCPRXSTATS_ALL":   "pdcp_rx",
    "UECOMBINED_PDCPTXSTATS_ALL":   "pdcp_tx",
    "UECOMBINED_RLCRXSTATS_ALL":    "rlc_rx",
    "UECOMBINED_RLCTXSTATS_ALL":    "rlc_tx",
    "UECOMBINED_L1DLSTATS_NA":      "l1_dl_stats",
    "UECOMBINED_L1ULSTATS_NA":      "l1_ul_stats",
    "UECOMBINED_SCCSTATUS_ALL":     "scc_status",
    "UECOMBINED_CARRIERTHROUGHPUT3D":       "tput_3d",
    "UECOMBINED_CARRIERTHROUGHPUTGRID":     "tput_grid",
    "UECOMBINED_CARRIERTHROUGHPUTOVERVIEW": "tput_overview",
    "UECOMBINED_CARRIERTHROUGHPUTTEXT_NA":  "tput_text",
    "UECOMBINED_THROUGHPUT3D_NA":    "tput_3d_na",
    "UECOMBINED_THROUGHPUTGRID_NA":  "tput_grid_na",
    "L1CELLDLOVERVIEW":              "l1_cell_dl",
    "L1CELLULOVERVIEW":              "l1_cell_ul",
    "L1CELLDLCARRIEROVERVIEW":       "l1_cell_dl_carrier",
    "L1CELLULCARRIEROVERVIEW":       "l1_cell_ul_carrier",
    "L1CELLWATCH_NA":                "l1_watch",
    "L1CELLSSBPOWERS":               "l1_ssb",
    "SYSOVERVIEW_NA":                "sys_overview",
}


# ── Cipher label functions ─────────────────────────────────────────────────────
def cipher_from_folder(name: str) -> str | None:
    """Extract cipher state from run folder name: cipheron_XXXX -> 'on'"""
    n = name.lower()
    if n.startswith("cipheron"):
        return "on"
    if n.startswith("cipheroff"):
        return "off"
    return None


def cipher_from_db(db_path: Path, run_folder_name: str) -> str | None:
    """
    Cross-validate cipher state from session metadata.db (SQLite).
    Table: configurations  |  Columns: id, name, eNB
    'name' matches run folder name. 'eNB' holds cipher state value.
    """
    if not db_path.exists():
        return None
    try:
        con = sqlite3.connect(str(db_path))
        cur = con.cursor()
        cur.execute("SELECT eNB FROM configurations WHERE name = ?",
                    (run_folder_name,))
        row = cur.fetchone()
        con.close()
        if row:
            val = str(row[0]).strip().lower()
            if val in ("on", "1", "true", "enabled", "cipheron"):
                return "on"
            if val in ("off", "0", "false", "disabled", "cipheroff"):
                return "off"
    except Exception as e:
        log.debug(f"metadata.db read failed [{run_folder_name}]: {e}")
    return None


# ── Timestamp extraction ───────────────────────────────────────────────────────
def extract_measurement_ts(tg_dir: Path) -> str | None:
    """
    Extract measurement timestamp from the CSV filename prefix.
    Pattern: YYMMDD_HHMMSS_GROUPNAME.csv
    All CSVs in one run share the same timestamp — just read the first one.
    Returns ISO string like "2022-09-09T12:54:27"
    """
    for f in tg_dir.iterdir():
        if f.suffix.lower() != ".csv":
            continue
        parts = f.stem.split("_", 2)
        if len(parts) >= 2 and len(parts[0]) == 6 and len(parts[1]) == 6:
            try:
                d, t = parts[0], parts[1]
                return (f"20{d[:2]}-{d[2:4]}-{d[4:6]}"
                        f"T{t[:2]}:{t[2:4]}:{t[4:6]}")
            except (ValueError, IndexError):
                continue
    return None


# ── CSV group reader with time-trimming ────────────────────────────────────────
def read_csv_group(csv_path: Path, trim_seconds: int) -> pd.DataFrame | None:
    """
    Read one measurement group CSV and apply time-trimming (paper §IV-B).
    Removes the first and last trim_seconds of the measurement window
    to exclude UE attachment/detachment transients.
    """
    try:
        df = pd.read_csv(csv_path, low_memory=False)
    except Exception as e:
        log.debug(f"read_csv failed [{csv_path.name}]: {e}")
        return None

    if df.empty:
        return None

    # Normalise column names
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    # Time-trim: find a numeric time/timestamp column
    ts_col = next(
        (c for c in df.columns
         if ("time" in c or "timestamp" in c)
         and pd.api.types.is_numeric_dtype(df[c])),
        None
    )

    if ts_col:
        t_min = df[ts_col].min()
        t_max = df[ts_col].max()
        df = df[
            (df[ts_col] >= t_min + trim_seconds) &
            (df[ts_col] <= t_max - trim_seconds)
        ].reset_index(drop=True)
    else:
        # Fallback: row-count trim
        # At 200ms sampling, 5s = 25 rows each end
        trim_rows = min(25, max(1, len(df) // 20))
        if len(df) > trim_rows * 2:
            df = df.iloc[trim_rows:-trim_rows].reset_index(drop=True)

    return df if not df.empty else None


# ── Dtype optimisation ─────────────────────────────────────────────────────────
def optimise_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Downcast columns to smallest safe types.
    Adds 20-40% extra compression on top of Snappy.
    """
    for col in df.columns:
        try:
            if pd.api.types.is_integer_dtype(df[col]):
                df[col] = pd.to_numeric(df[col], downcast="integer")
            elif pd.api.types.is_float_dtype(df[col]):
                # Always downcast to float32 — never convert to boolean.
                # A column like lost_pdus contains only 0.0 in zero-loss runs
                # but could be 5, 10, 50 in other runs. Converting based on
                # one run's value range destroys numeric information.
                df[col] = df[col].astype("float32")
            elif df[col].dtype == object:
                if df[col].nunique() <= 50:
                    df[col] = df[col].astype("category")
        except Exception:
            pass
    return df


# ── Quarantine helper ──────────────────────────────────────────────────────────
def quarantine(run_dir: Path, output_dir: Path, reason: str):
    """Write a small flag file to quarantine/ so bad runs are visible."""
    q_dir = output_dir / "quarantine"
    q_dir.mkdir(parents=True, exist_ok=True)
    tranche  = run_dir.parent.parent.name
    session  = run_dir.parent.name
    run      = run_dir.name
    flag     = q_dir / f"{tranche}__{session}__{run}.txt"
    flag.write_text(f"Run  : {run_dir}\nReason: {reason}\n",
                    encoding="utf-8")


# ── Single run processor ───────────────────────────────────────────────────────
def process_run(run_dir: Path, tranche: str, session: str,
                db_path: Path, output_dir: Path,
                groups_filter: set | None,
                trim_seconds: int) -> dict:
    """
    Process one run folder → one Parquet file.
    Returns a manifest row dict with outcome, sizes, and quality flags.
    """
    run_name = run_dir.name
    tg_dir   = run_dir / "traffic_generator"

    row = {
        "run_id":             run_name,
        "run_number":         _run_number(run_name),
        "tranche":            tranche,
        "session":            session,
        "cipher_state":       "unknown",
        "cipher_source":      "none",
        "label_mismatch":     False,
        "measurement_ts":     None,
        "row_count":          0,
        "col_count":          0,
        "csv_files_read":     0,
        "raw_size_bytes":     0,
        "parquet_size_bytes": 0,
        "parquet_path":       "",
        "outcome":            "skip",
        "skip_reason":        "",
        "processed_at":       datetime.now(timezone.utc).isoformat(),
    }

    # ── traffic_generator must exist ──────────────────────────────────────────
    if not tg_dir.exists():
        row["skip_reason"] = "no traffic_generator folder"
        return row

    # ── Dual cipher label validation ──────────────────────────────────────────
    c_folder = cipher_from_folder(run_name)
    c_db     = cipher_from_db(db_path, run_name)

    if c_folder and c_db and c_folder != c_db:
        row["label_mismatch"] = True
        log.warning(f"  LABEL MISMATCH: {run_name} — "
                    f"folder='{c_folder}'  db='{c_db}'")

    cipher = c_folder or c_db or "unknown"
    row["cipher_state"]  = cipher
    row["cipher_source"] = "folder" if c_folder else ("db" if c_db else "none")

    if cipher == "unknown":
        row["outcome"]     = "quarantine"
        row["skip_reason"] = "cipher state unknown"
        quarantine(run_dir, output_dir, row["skip_reason"])
        return row

    # ── Discover and read CSV groups ──────────────────────────────────────────
    csv_files = sorted(tg_dir.glob("*.csv"))
    if not csv_files:
        row["skip_reason"] = "no CSV files in traffic_generator"
        return row

    row["measurement_ts"] = extract_measurement_ts(tg_dir)
    row["raw_size_bytes"] = sum(f.stat().st_size for f in csv_files)

    group_dfs  = []
    files_read = 0

    for csv_path in csv_files:
        # Extract group name: YYMMDD_HHMMSS_GROUPNAME.csv -> GROUPNAME
        parts      = csv_path.stem.split("_", 2)
        group_name = parts[2] if len(parts) >= 3 else csv_path.stem

        # Apply group filter if specified
        if groups_filter and group_name not in groups_filter:
            continue

        df = read_csv_group(csv_path, trim_seconds)
        if df is None or df.empty:
            continue

        # Prefix columns with clean group tag to avoid collisions
        tag = GROUP_PREFIX_MAP.get(group_name,
                                   re.sub(r"[^a-z0-9_]", "_",
                                          group_name.lower())[:25])
        df = df.add_prefix(f"{tag}__")
        group_dfs.append(df)
        files_read += 1

    row["csv_files_read"] = files_read

    if not group_dfs:
        row["outcome"]     = "quarantine"
        row["skip_reason"] = "all CSV groups empty after trimming"
        quarantine(run_dir, output_dir, row["skip_reason"])
        return row

    # ── Merge groups by row index ─────────────────────────────────────────────
    # All groups in one run are time-aligned within the same 5-minute window.
    # Align to the shortest group to prevent shape mismatches.
    min_rows = min(len(d) for d in group_dfs)
    merged   = pd.concat(
        [d.iloc[:min_rows].reset_index(drop=True) for d in group_dfs],
        axis=1
    )

    # ── Row count sanity check ─────────────────────────────────────────────────
    if len(merged) < 200:
        row["outcome"]     = "quarantine"
        row["skip_reason"] = f"too few rows after trimming: {len(merged)}"
        quarantine(run_dir, output_dir, row["skip_reason"])
        return row

    # ── Attach metadata columns ────────────────────────────────────────────────
    # Single pd.concat avoids repeated .insert() which fragments the DataFrame
    n = len(merged)
    meta = pd.DataFrame({
        "run_id":         [run_name]              * n,
        "run_number":     [row["run_number"]]     * n,
        "cipher_state":   [cipher]                * n,
        "tranche":        [tranche]               * n,
        "session":        [session]               * n,
        "measurement_ts": [row["measurement_ts"]] * n,
        "label_mismatch": [row["label_mismatch"]] * n,
    })
    merged = pd.concat([meta, merged], axis=1).reset_index(drop=True)

    # ── Dtype optimisation ─────────────────────────────────────────────────────
    merged = optimise_dtypes(merged)

    row["row_count"] = len(merged)
    row["col_count"] = len(merged.columns)

    # ── Write Parquet ──────────────────────────────────────────────────────────
    out_path = (output_dir / "parquet" / tranche / session
                / f"{run_name}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        table = pa.Table.from_pandas(merged, preserve_index=False)
        pq.write_table(table, str(out_path),
                       compression=COMPRESSION,
                       row_group_size=ROW_GROUP_SIZE)
        row["parquet_size_bytes"] = out_path.stat().st_size
        row["parquet_path"]       = str(out_path.relative_to(output_dir))
        row["outcome"]            = "ok"
    except Exception as e:
        row["outcome"]     = "quarantine"
        row["skip_reason"] = f"Parquet write failed: {e}"
        quarantine(run_dir, output_dir, row["skip_reason"])

    return row


def _run_number(name: str) -> int:
    m = re.search(r"(\d+)$", name)
    return int(m.group(1)) if m else -1


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="DARE Dataset — Preprocess CSV runs to Parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
        Examples:
          # Start here — process Tranche_B first (training set, most uniform)
          python scripts/preprocess.py \\
              --dare_root  "E:\\DARE_Data_Public\\Full_dataset" \\
              --output_dir "E:\\dare_preprocessed" \\
              --tranche    Tranche_B

          # Then process the other two
          python scripts/preprocess.py --dare_root "E:\\DARE_Data_Public\\Full_dataset" --output_dir "E:\\dare_preprocessed" --tranche Tranche_A
          python scripts/preprocess.py --dare_root "E:\\DARE_Data_Public\\Full_dataset" --output_dir "E:\\dare_preprocessed" --tranche Tranche_C

          # Process all at once (takes 2-4 hours)
          python scripts/preprocess.py --dare_root "E:\\DARE_Data_Public\\Full_dataset" --output_dir "E:\\dare_preprocessed"

          # Fast test: priority groups only, one tranche
          python scripts/preprocess.py --dare_root "E:\\DARE_Data_Public\\Full_dataset" --output_dir "E:\\dare_preprocessed" --tranche Tranche_B --priority_only
        """)
    )
    parser.add_argument(
        "--dare_root", required=True,
        help="Path to Full_dataset folder  e.g. E:\\DARE_Data_Public\\Full_dataset"
    )
    parser.add_argument(
        "--output_dir", required=True,
        help="Where to write Parquet files and manifest  e.g. E:\\dare_preprocessed"
    )
    parser.add_argument(
        "--tranche", default="all",
        choices=["Tranche_A", "Tranche_B", "Tranche_C", "all"],
        help="Which tranche to process (default: all)"
    )
    parser.add_argument(
        "--read_all", action="store_true",
        help="Read ALL 25 CSV groups including raw subframe data (slower, not needed for x1-x424)"
    )
    parser.add_argument(
        "--trim_seconds", type=int, default=TRIM_SECONDS,
        help=f"Seconds to trim from each end of a run (default: {TRIM_SECONDS})"
    )
    args = parser.parse_args()

    dare_root  = Path(args.dare_root)
    output_dir = Path(args.output_dir)

    if not dare_root.exists():
        log.error(f"dare_root not found: {dare_root}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    groups_filter = ESSENTIAL_GROUPS if not args.read_all else None

    # ── Discover tranches ──────────────────────────────────────────────────────
    if args.tranche == "all":
        tranche_dirs = sorted(
            d for d in dare_root.iterdir()
            if d.is_dir() and d.name.startswith("Tranche_")
        )
    else:
        td = dare_root / args.tranche
        if not td.exists():
            log.error(f"Tranche folder not found: {td}")
            sys.exit(1)
        tranche_dirs = [td]

    log.info(f"Tranches to process : {[d.name for d in tranche_dirs]}")
    log.info(f"Output directory    : {output_dir}")
    log.info(f"Read all groups     : {args.read_all} (default: essential groups only)")
    log.info(f"Trim seconds        : {args.trim_seconds}")

    manifest_rows = []
    stats = {"ok": 0, "quarantine": 0, "skip": 0,
             "bytes_in": 0, "bytes_out": 0, "mismatches": 0}

    # ── Walk tranches → sessions → runs ───────────────────────────────────────
    for t_dir in tranche_dirs:
        tranche      = t_dir.name
        session_dirs = sorted(
            d for d in t_dir.iterdir()
            if d.is_dir() and d.name.startswith("Session_")
        )
        log.info(f"\nTranche: {tranche}  ({len(session_dirs)} sessions)")

        for s_dir in session_dirs:
            session  = s_dir.name
            db_path  = s_dir / "metadata.db"

            if not db_path.exists():
                log.warning(f"  No metadata.db in {s_dir.name} — "
                             "cipher labels from folder names only")

            run_dirs = sorted(
                d for d in s_dir.iterdir()
                if d.is_dir() and (d.name.lower().startswith("cipher"))
            )
            log.info(f"  Session: {session}  ({len(run_dirs)} runs)")

            for run_dir in tqdm(run_dirs,
                                desc=f"    {session}",
                                leave=False,
                                unit="run"):
                result = process_run(
                    run_dir=run_dir,
                    tranche=tranche,
                    session=session,
                    db_path=db_path,
                    output_dir=output_dir,
                    groups_filter=groups_filter,  # None = essential only, set = specific groups
                    trim_seconds=args.trim_seconds,
                )

                manifest_rows.append(result)
                stats[result["outcome"]] += 1
                stats["bytes_in"]  += result["raw_size_bytes"]
                stats["bytes_out"] += result["parquet_size_bytes"]
                if result["label_mismatch"]:
                    stats["mismatches"] += 1

    # ── Write manifest ─────────────────────────────────────────────────────────
    manifest_path = output_dir / "manifest.csv"
    if manifest_rows:
        fieldnames = list(manifest_rows[0].keys())
        with open(manifest_path, "w", newline="", encoding="utf-8") as f:
            writer = csv_module.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(manifest_rows)
        log.info(f"\nManifest: {manifest_path}  ({len(manifest_rows)} rows)")

    # ── Quarantine summary ─────────────────────────────────────────────────────
    q_rows = [r for r in manifest_rows if r["outcome"] == "quarantine"]
    if q_rows:
        q_path = output_dir / "quarantine.csv"
        with open(q_path, "w", newline="", encoding="utf-8") as f:
            writer = csv_module.DictWriter(f, fieldnames=list(q_rows[0].keys()))
            writer.writeheader()
            writer.writerows(q_rows)

    # ── Final summary ──────────────────────────────────────────────────────────
    ratio = (stats["bytes_in"] / stats["bytes_out"]
             if stats["bytes_out"] > 0 else 0)

    log.info("\n" + "=" * 65)
    log.info("PREPROCESSING COMPLETE")
    log.info("=" * 65)
    log.info(f"  Runs OK            : {stats['ok']}")
    log.info(f"  Runs quarantined   : {stats['quarantine']}  "
             f"(see {output_dir / 'quarantine.csv'})")
    log.info(f"  Runs skipped       : {stats['skip']}")
    log.info(f"  Label mismatches   : {stats['mismatches']}  <- review before ML")
    log.info(f"  Raw CSV input      : {stats['bytes_in'] / 1e9:.2f} GB")
    log.info(f"  Parquet output     : {stats['bytes_out'] / 1e9:.2f} GB")
    log.info(f"  Compression ratio  : {ratio:.1f}x")
    log.info("=" * 65)
    log.info("\nNext step:")
    log.info("  python scripts/validate_parquet.py "
             f"--output_dir \"{output_dir}\" "
             f"--dare_root \"{dare_root}\"")


if __name__ == "__main__":
    main()
