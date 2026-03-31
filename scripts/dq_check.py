"""
DARE Parquet — Pre-upload Data Quality Check
Checks all 1,982 parquet files across all three tranches.
Run from the project root:
    python scripts/dq_check.py --output_dir "E:/dare_preprocessed"
"""

import argparse
import pathlib
import pandas as pd
import numpy as np
from collections import defaultdict

def run_dq(output_dir: str):
    base = pathlib.Path(output_dir)
    parquet_base = base / "parquet"

    all_files = sorted(parquet_base.rglob("*.parquet"))
    print(f"\nFound {len(all_files)} parquet files\n")
    print("=" * 65)

    # ── Accumulators ──────────────────────────────────────────────
    issues = []
    bool_violations      = []
    range_violations     = []
    null_summary         = defaultdict(list)   # col -> list of null rates
    row_counts           = defaultdict(list)   # tranche/session -> [row_counts]
    col_counts           = defaultdict(list)
    cipher_per_session   = defaultdict(lambda: {"on": 0, "off": 0})
    ts_gap_violations    = []
    checked              = 0

    # ── Known physical ranges for key columns ─────────────────────
    # Format: column_fragment -> (min_allowed, max_allowed, description)
    RANGE_CHECKS = {
        "dl-sch_bler":           (0.0,   1.0,   "BLER must be 0–1"),
        "dl-sch_throughput":     (0.0,   1e6,   "Throughput must be positive"),
        "snr":                   (-30.0, 60.0,  "SNR plausible range dB"),
        "path_loss":             (0.0,   200.0, "Path loss plausible range dB"),
        "mean_mcs":              (0.0,   28.0,  "MCS index 0–28"),
        "lost_pdu":              (0.0,   1e5,   "Lost PDUs must be non-negative"),
        "bad_pdu":               (0.0,   1e5,   "Bad PDUs must be non-negative"),
        "sdu_throughput":        (0.0,   1e6,   "SDU throughput must be positive"),
    }

    print("Checking all files (this takes 3–5 minutes)...\n")

    for f in all_files:
        parts  = f.parts
        tranche  = [p for p in parts if p.startswith("Tranche_")][0]
        session  = [p for p in parts if p.startswith("Session_")][0]
        run_id   = f.stem
        key      = f"{tranche}/{session}"

        try:
            df = pd.read_parquet(f)
        except Exception as e:
            issues.append(f"UNREADABLE: {f.name} — {e}")
            continue

        checked += 1

        # ── Check 1: No unexpected boolean columns ─────────────────
        bad_bool = [c for c in df.columns
                    if str(df[c].dtype) == "boolean" and c != "label_mismatch"]
        if bad_bool:
            bool_violations.append((run_id, bad_bool))

        # ── Check 2: Cipher state is valid ─────────────────────────
        cipher = df["cipher_state"].iloc[0]
        if cipher not in ("on", "off"):
            issues.append(f"INVALID CIPHER STATE: {run_id} has '{cipher}'")
        else:
            cipher_per_session[key][cipher] += 1

        # ── Check 3: Row and column counts ─────────────────────────
        row_counts[tranche].append(len(df))
        col_counts[tranche].append(len(df.columns))

        # ── Check 4: Value range checks ────────────────────────────
        for col_fragment, (lo, hi, desc) in RANGE_CHECKS.items():
            matching = [c for c in df.columns if col_fragment in c.lower()
                        and df[c].dtype in (np.float32, np.float64, float)]
            for col in matching:
                col_min = float(df[col].min())
                col_max = float(df[col].max())
                if col_min < lo or col_max > hi:
                    range_violations.append({
                        "run":  run_id,
                        "col":  col,
                        "min":  round(col_min, 4),
                        "max":  round(col_max, 4),
                        "allowed": f"{lo}–{hi}",
                        "desc": desc,
                    })

        # ── Check 5: Null rates for key columns ────────────────────
        key_cols = [c for c in df.columns
                    if any(x in c for x in
                           ["bler", "lost_pdu", "throughput", "snr", "mcs"])]
        for col in key_cols[:10]:   # sample up to 10 key cols per file
            null_rate = float(df[col].isna().mean())
            null_summary[col].append(null_rate)

        # ── Check 6: Timestamp continuity (sample every 10th file) ─
        if checked % 10 == 0 and "pdcp_rx__time" in df.columns:
            times = df["pdcp_rx__time"].dropna().head(200)
            # Check for duplicate timestamps (would indicate merge error)
            dup_rate = times.duplicated().mean()
            if dup_rate > 0.95:
                ts_gap_violations.append(
                    f"{run_id}: {dup_rate:.0%} duplicate timestamps")

    # ── Report ─────────────────────────────────────────────────────
    print("=" * 65)
    print("DATA QUALITY REPORT")
    print("=" * 65)
    print(f"Files checked: {checked} / {len(all_files)}")
    print()

    # Boolean check
    print("── CHECK 1: Boolean dtype violations ──────────────────────")
    if bool_violations:
        print(f"  [FAIL] {len(bool_violations)} files with boolean columns:")
        for run, cols in bool_violations[:5]:
            print(f"    {run}: {cols}")
    else:
        print("  [PASS] No boolean violations")
    print()

    # Range checks
    print("── CHECK 2: Value range violations ────────────────────────")
    # Group by column fragment
    range_by_col = defaultdict(list)
    for v in range_violations:
        frag = next((k for k in RANGE_CHECKS if k in v["col"]), v["col"])
        range_by_col[frag].append(v)

    if range_violations:
        print(f"  [WARN] {len(range_violations)} range violations across "
              f"{len(range_by_col)} column types:")
        for frag, violations in list(range_by_col.items())[:8]:
            runs  = list({v['run'] for v in violations})
            mins  = [v['min'] for v in violations]
            maxes = [v['max'] for v in violations]
            allowed = violations[0]['allowed']
            print(f"    {frag}")
            print(f"      Allowed: {allowed} | "
                  f"Observed: {min(mins):.4f}–{max(maxes):.4f} | "
                  f"Affected runs: {len(runs)}")
    else:
        print("  [PASS] All values within physical ranges")
    print()

    # Row counts
    print("── CHECK 3: Row count distribution ────────────────────────")
    for tranche, counts in sorted(row_counts.items()):
        arr = np.array(counts)
        median = np.median(arr)
        outliers = np.sum(np.abs(arr - median) > 3 * np.std(arr))
        print(f"  {tranche}: median={median:.0f}  "
              f"min={arr.min()}  max={arr.max()}  "
              f"outliers(3σ)={outliers}")
    print()

    # Column counts
    print("── CHECK 4: Column count distribution ─────────────────────")
    for tranche, counts in sorted(col_counts.items()):
        arr   = np.array(counts)
        unique = len(set(counts))
        print(f"  {tranche}: min={arr.min()}  max={arr.max()}  "
              f"distinct schemas={unique}")
    print()

    # Cipher balance per session
    print("── CHECK 5: Cipher balance per session ─────────────────────")
    imbalanced = []
    for key, counts in sorted(cipher_per_session.items()):
        total = counts["on"] + counts["off"]
        if total == 0:
            continue
        pct_on = counts["on"] / total * 100
        flag = "  [WARN]" if abs(pct_on - 50) > 15 else "  [OK]  "
        print(f"{flag} {key}: on={counts['on']}  off={counts['off']}  "
              f"({pct_on:.0f}% on)")
        if abs(pct_on - 50) > 15:
            imbalanced.append(key)
    print()

    # Null rates
    print("── CHECK 6: Null rates in key columns ──────────────────────")
    high_null = []
    for col, rates in sorted(null_summary.items()):
        mean_null = np.mean(rates)
        if mean_null > 0.01:
            high_null.append((col, mean_null))
    if high_null:
        print(f"  [WARN] {len(high_null)} columns with >1% nulls:")
        for col, rate in sorted(high_null, key=lambda x: -x[1])[:10]:
            print(f"    {col}: {rate:.1%} null")
    else:
        print("  [PASS] No key columns with >1% null rate")
    print()

    # Timestamp check
    print("── CHECK 7: Timestamp continuity ───────────────────────────")
    if ts_gap_violations:
        print(f"  [WARN] {len(ts_gap_violations)} files with suspicious timestamps:")
        for v in ts_gap_violations[:5]:
            print(f"    {v}")
    else:
        print("  [PASS] Timestamp continuity looks normal")
    print()

    # Unreadable files
    print("── CHECK 8: Unreadable files ───────────────────────────────")
    if issues:
        print(f"  [FAIL] {len(issues)} issues:")
        for i in issues[:5]:
            print(f"    {i}")
    else:
        print("  [PASS] All files readable")
    print()

    # Final verdict
    print("=" * 65)
    total_issues = (len(bool_violations) + len(issues) +
                    len(imbalanced) + len(ts_gap_violations))
    if total_issues == 0:
        print("VERDICT: CLEAN — safe to upload to GCS")
    else:
        print(f"VERDICT: {total_issues} issue(s) need review before upload")
    print("=" * 65)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_dir", required=True)
    args = parser.parse_args()
    run_dq(args.output_dir)
