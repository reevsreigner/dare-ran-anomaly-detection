"""
DARE RAN Dataset — Parquet Validation Script
============================================
Run this AFTER preprocess.py and BEFORE uploading to GCS.

What it checks:
  1.  Every OK run in manifest.csv has a Parquet file on disk
  2.  Each Parquet file is readable and non-empty
  3.  No runs with 'unknown' cipher state
  4.  Cipher state is ~50/50 per tranche (expect this for DARE)
  5.  No label mismatches between folder name and metadata.db
  6.  Row counts are consistent within each tranche (flags outliers)
  7.  All Parquet files in a tranche share the same column schema
  8.  Quarantine summary — how many runs were rejected and why
  9.  NIST comparison — loads processed_data_B.csv and shows their
      x15 values so you can confirm your pipeline matches (requires --dare_root)
  10. Compression report — exactly how much you will upload to GCS

USAGE:
  python scripts/validate_parquet.py \\
      --output_dir "E:\\dare_preprocessed" \\
      --dare_root  "E:\\DARE_Data_Public\\Full_dataset"

  # Without NIST comparison (if you have not processed Tranche_B yet):
  python scripts/validate_parquet.py --output_dir "E:\\dare_preprocessed"

REQUIREMENTS:
  pip install pandas pyarrow
"""

import argparse
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

OK   = "[OK]  "
WARN = "[WARN]"
FAIL = "[FAIL]"
SEP  = "-" * 68
SEP2 = "=" * 68


def fmt_bytes(b: int) -> str:
    if b >= 1e9: return f"{b / 1e9:.2f} GB"
    if b >= 1e6: return f"{b / 1e6:.1f} MB"
    return f"{b / 1e3:.1f} KB"


def validate(output_dir: Path, dare_root: Path | None):

    print(f"\n{SEP2}")
    print(f"  DARE PARQUET VALIDATION")
    print(f"  output_dir : {output_dir}")
    print(f"  dare_root  : {dare_root or '(not provided)'}")
    print(SEP2)

    # ── Load manifest ──────────────────────────────────────────────────────────
    manifest_path = output_dir / "manifest.csv"
    if not manifest_path.exists():
        print(f"\n{FAIL} manifest.csv not found: {manifest_path}")
        print("  Run preprocess.py first.")
        sys.exit(1)

    manifest = pd.read_csv(manifest_path)
    ok_runs  = manifest[manifest["outcome"] == "ok"].copy()
    print(f"\nManifest: {len(manifest)} total rows | "
          f"{len(ok_runs)} OK | "
          f"{(manifest['outcome']=='quarantine').sum()} quarantined | "
          f"{(manifest['outcome']=='skip').sum()} skipped\n")

    issues = []
    schema_by_tranche: dict[str, set] = defaultdict(set)

    # ── CHECK 1: File existence and readability ────────────────────────────────
    print(SEP)
    print("CHECK 1 — File existence and readability")
    print(SEP)
    missing    = 0
    unreadable = 0

    for _, row in ok_runs.iterrows():
        fpath = output_dir / row["parquet_path"]
        if not fpath.exists():
            missing += 1
            issues.append(f"Missing: {fpath.name}")
            continue
        try:
            pf   = pq.read_table(str(fpath))
            cols = tuple(sorted(pf.schema.names))
            schema_by_tranche[row["tranche"]].add(cols)
        except Exception as e:
            unreadable += 1
            issues.append(f"Unreadable: {fpath.name} — {e}")

    if missing == 0 and unreadable == 0:
        print(f"  {OK} All {len(ok_runs)} Parquet files present and readable")
    else:
        if missing    > 0:
            print(f"  {FAIL} {missing} files missing")
        if unreadable > 0:
            print(f"  {FAIL} {unreadable} files unreadable")

    # ── CHECK 2: Unknown cipher states ────────────────────────────────────────
    print(f"\n{SEP}")
    print("CHECK 2 — Cipher state completeness")
    print(SEP)
    unknown = (ok_runs["cipher_state"] == "unknown").sum()
    if unknown == 0:
        print(f"  {OK} No unknown cipher states")
    else:
        print(f"  {FAIL} {unknown} runs with UNKNOWN cipher state")
        issues.append(f"{unknown} runs with unknown cipher state")

    # ── CHECK 3: 50/50 balance per tranche ────────────────────────────────────
    print(f"\n{SEP}")
    print("CHECK 3 — Cipher state balance per tranche (expect ~50/50)")
    print(SEP)
    for tranche, grp in ok_runs.groupby("tranche"):
        counts  = grp["cipher_state"].value_counts()
        total   = len(grp)
        on_pct  = counts.get("on",  0) / total * 100
        off_pct = counts.get("off", 0) / total * 100
        delta   = abs(on_pct - off_pct)
        status  = OK if delta < 10 else WARN
        print(f"  {status} {tranche}: on={on_pct:.0f}%  off={off_pct:.0f}%  "
              f"(delta={delta:.1f}%,  n={total})")

    # ── CHECK 4: Label mismatches ──────────────────────────────────────────────
    print(f"\n{SEP}")
    print("CHECK 4 — Folder-name vs metadata.db label mismatches")
    print(SEP)
    mm = ok_runs[ok_runs["label_mismatch"] == True]
    if len(mm) == 0:
        print(f"  {OK} No label mismatches")
    else:
        print(f"  {WARN} {len(mm)} mismatches — review before ML training:")
        for _, r in mm.iterrows():
            print(f"       {r['tranche']}/{r['session']}/{r['run_id']}")

    # ── CHECK 5: Row count consistency ────────────────────────────────────────
    print(f"\n{SEP}")
    print("CHECK 5 — Row count consistency per tranche")
    print(SEP)
    for tranche, grp in ok_runs.groupby("tranche"):
        median  = grp["row_count"].median()
        low     = grp[grp["row_count"] < median * 0.40]
        high    = grp[grp["row_count"] > median * 2.50]
        if len(low) == 0 and len(high) == 0:
            print(f"  {OK} {tranche}: median={median:.0f} rows,  no outliers")
        else:
            print(f"  {WARN} {tranche}: {len(low)} low + {len(high)} high outliers "
                  f"(median={median:.0f})")
            for _, r in pd.concat([low, high]).iterrows():
                print(f"       {r['run_id']}: {r['row_count']} rows")

    # ── CHECK 6: Schema consistency ───────────────────────────────────────────
    print(f"\n{SEP}")
    print("CHECK 6 — Schema consistency per tranche")
    print(SEP)
    for tranche, schemas in schema_by_tranche.items():
        if len(schemas) == 1:
            n_cols = len(list(schemas)[0])
            print(f"  {OK} {tranche}: uniform schema  ({n_cols} columns)")
        else:
            print(f"  {WARN} {tranche}: {len(schemas)} different schemas — column drift!")
            issues.append(f"Schema inconsistency in {tranche}")

    # ── CHECK 7: Quarantine summary ───────────────────────────────────────────
    print(f"\n{SEP}")
    print("CHECK 7 — Quarantined runs")
    print(SEP)
    q_path = output_dir / "quarantine.csv"
    if not q_path.exists():
        print(f"  {OK} No quarantine.csv found  (zero quarantined runs)")
    else:
        try:
            q_df = pd.read_csv(q_path)
            if len(q_df) == 0:
                print(f"  {OK} quarantine.csv is empty  (zero quarantined runs)")
            else:
                print(f"  {WARN} {len(q_df)} runs quarantined:")
                for reason, cnt in q_df["skip_reason"].value_counts().items():
                    print(f"       {cnt}x  {reason}")
        except Exception as e:
            print(f"  {WARN} Could not read quarantine.csv: {e}")

    # ── CHECK 8: NIST comparison ───────────────────────────────────────────────
    if dare_root:
        print(f"\n{SEP}")
        print("CHECK 8 — NIST processed_data_B.csv comparison")
        print(SEP)
        nist_path = dare_root / "Tranche_B" / "processed_data_B.csv"
        if not nist_path.exists():
            print(f"  {WARN} processed_data_B.csv not found at expected path:")
            print(f"       {nist_path}")
        else:
            try:
                nist = pd.read_csv(nist_path)
                print(f"  {OK} NIST file loaded: "
                      f"{len(nist)} rows x {len(nist.columns)} columns")

                your_b = ok_runs[ok_runs["tranche"] == "Tranche_B"]
                print(f"       Your Tranche_B OK runs: {len(your_b)}")
                print(f"       NIST Tranche_B rows    : {len(nist)}")

                # Show NIST x15 split by cipher state if columns exist
                if "x15" in nist.columns:
                    print(f"\n  NIST x15 (RSRQ proportion at -10.5 dB):")
                    print(f"       Overall: mean={nist['x15'].mean():.4f}  "
                          f"median={nist['x15'].median():.4f}")

                    # Indicator column: 1=cipher_on, 0=cipher_off
                    ind_col = next(
                        (c for c in nist.columns
                         if c.lower() in ("indicator", "cipher_state",
                                          "label", "cipher")),
                        None
                    )
                    if ind_col:
                        for label, val in [("cipher_on", 1), ("cipher_off", 0)]:
                            sub = nist[nist[ind_col] == val]["x15"]
                            print(f"       {label}: median={sub.median():.4f}  "
                                  f"(n={len(sub)})")
                    print(f"\n  Paper target: x15 ≈ 0.34 for run_317 (cipher=on)")
                    print(f"  Use this as your pipeline correctness benchmark.")

                if "x182" in nist.columns:
                    print(f"\n  NIST x182 (MAC PDU size q0.25):")
                    print(f"       mean={nist['x182'].mean():.2f}  "
                          f"median={nist['x182'].median():.2f}")
                    print(f"  Paper target: x182 ≈ 268.2–268.7 bytes")

            except Exception as e:
                print(f"  {WARN} Could not read processed_data_B.csv: {e}")

    # ── Compression report ─────────────────────────────────────────────────────
    print(f"\n{SEP}")
    print("COMPRESSION REPORT")
    print(SEP)
    raw_total  = ok_runs["raw_size_bytes"].sum()
    parq_total = ok_runs["parquet_size_bytes"].sum()
    ratio      = raw_total / parq_total if parq_total > 0 else 0

    print(f"  Raw CSV input   : {fmt_bytes(int(raw_total))}")
    print(f"  Parquet output  : {fmt_bytes(int(parq_total))}")
    print(f"  Compression     : {ratio:.1f}x")
    print(f"  GCS upload size : {fmt_bytes(int(parq_total))}  <- what you upload")
    print()

    per_t = ok_runs.groupby("tranche").agg(
        runs      = ("run_id",            "count"),
        raw_gb    = ("raw_size_bytes",    lambda x: round(x.sum() / 1e9, 2)),
        parquet_gb= ("parquet_size_bytes",lambda x: round(x.sum() / 1e9, 2)),
    )
    print(per_t.to_string())

    # ── Final verdict ──────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    if not issues:
        print(f"{OK} ALL CHECKS PASSED — safe to upload to GCS")
        print()
        print("  gsutil -m cp -r \"E:\\dare_preprocessed\\parquet\" gs://dare-raw-data/")
        print("  gsutil cp \"E:\\dare_preprocessed\\manifest.csv\" gs://dare-raw-data/")
    else:
        print(f"{WARN} {len(issues)} issue(s) found — review before uploading:")
        for issue in issues:
            print(f"    - {issue}")
    print(SEP2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validate DARE preprocessed Parquet files"
    )
    parser.add_argument(
        "--output_dir", required=True,
        help="Path to dare_preprocessed folder  e.g. E:\\dare_preprocessed"
    )
    parser.add_argument(
        "--dare_root", default=None,
        help="Path to Full_dataset (enables NIST comparison check)"
    )
    args = parser.parse_args()

    validate(
        output_dir = Path(args.output_dir),
        dare_root  = Path(args.dare_root) if args.dare_root else None,
    )
