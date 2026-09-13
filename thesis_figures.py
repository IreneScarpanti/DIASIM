#!/usr/bin/env python3
"""
thesis_figures.py — regenerate the coordinates behind the thesis evaluation
figures directly from the benchmark CSVs, so that every figure in the thesis is
reproducible from this repository.

Usage:
    python3 thesis_figures.py [directory-with-csvs]

If no directory is given, the CSVs are looked up next to this script and in the
usual cmd/ subfolders. The script prints, for each figure panel, the exact
(x, y) coordinates used in the corresponding TikZ/pgfplots source.

Metrics (see the thesis, Chapter 7, and the README):

  Scalability (Ch. 6) -> median wall time over the repetitions, and
                         speedup = median(sequential) / median(parallel).
                         Wall-time distributions are right-skewed, so the
                         median (not the mean) is the appropriate summary.

  Broadcast panels  -> mean over the 10 seeds of the named CSV column
                       (delivery_rate, agreement_rate).

  Election panels   -> "successful runs out of ten", where a run is a SUCCESS
                       iff  terminated == true  AND  values_correct == 1.0
                       (the run terminates within its step budget and every
                       correct node commits exactly the correct values).

                       This is deliberately scored on the committed outcome,
                       not on the leader role: under byz_leader a correct node
                       can hold the leader role while the correct values are
                       never committed, so a role-based score would overstate
                       success. See Chapter 7.
"""

import csv
import os
import sys
from collections import defaultdict


def find_csv(name, base):
    """Locate a CSV by name: next to the script, in base, or in cmd/ subdirs."""
    candidates = [
        os.path.join(base, name),
        os.path.join(base, "cmd", "benchmark", name),
        os.path.join(base, "cmd", "robustness", name),
        os.path.join(base, "cmd", "comparison", name),
        os.path.join(base, "cmd", "election_robustness", name),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    raise FileNotFoundError(f"could not find {name} (looked in {candidates})")


def load(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def fmt(points):
    """Format a dict {x: y} as a pgfplots coordinate string."""
    return " ".join(f"({x},{points[x]})" for x in sorted(points))


# ── Election metric ──────────────────────────────────────────────────────────

def election_success(row):
    return (row["terminated"].strip().lower() == "true"
            and abs(float(row["values_correct"]) - 1.0) < 1e-9)


def election_curve(rows, algorithm, failure_type):
    d = defaultdict(lambda: [0, 0])  # f -> [successes, total]
    for r in rows:
        if r["algorithm"] == algorithm and r["failure_type"] == failure_type:
            f = int(r["f"])
            d[f][1] += 1
            if election_success(r):
                d[f][0] += 1
    return {f: d[f][0] for f in sorted(d)}


# ── Broadcast metric ─────────────────────────────────────────────────────────

def broadcast_curve(rows, algorithm, failure_type, column):
    d = defaultdict(list)
    for r in rows:
        if r["algorithm"] == algorithm and r["failure_type"] == failure_type:
            d[int(r["f"])].append(float(r[column]))
    return {f: round(sum(v) / len(v), 4) for f, v in sorted(d.items())}


# ── Scalability metric (Chapter 6) ───────────────────────────────────────────
# Wall-time distributions are right-skewed, so the thesis summarizes each
# configuration by the MEDIAN of the repetitions (not the mean) and its spread
# by the interquartile range. Speedup = median(sequential) / median(parallel).

def _median(xs):
    xs = sorted(xs)
    n = len(xs)
    if n == 0:
        return None
    mid = n // 2
    if n % 2:
        return float(xs[mid])
    return (xs[mid - 1] + xs[mid]) / 2.0


def scalability_curve(rows, experiment, x_field):
    """Return {x: (median_seq_us, median_par_us, speedup)} for one experiment."""
    seq = defaultdict(list)
    par = defaultdict(list)
    for r in rows:
        if r["experiment"] != experiment:
            continue
        x = int(r[x_field])
        us = int(r["wall_time_us"])
        (seq if r["mode"] == "sequential" else par)[x].append(us)
    out = {}
    for x in sorted(set(seq) | set(par)):
        ms = _median(seq.get(x, []))
        mp = _median(par.get(x, []))
        speedup = (ms / mp) if (ms and mp) else None
        out[x] = (ms, mp, speedup)
    return out


def main():
    base = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(os.path.abspath(__file__))

    comparison = load(find_csv("comparison_results.csv", base))
    robustness = load(find_csv("robustness_results.csv", base))
    benchmark = load(find_csv("benchmark_results.csv", base))

    print("=" * 70)
    print("Chapter 6 — Scalability   (source: benchmark_results.csv)")
    print("  metric: median wall time over the repetitions; speedup =")
    print("          median(sequential) / median(parallel)")
    print("=" * 70)
    exps = [
        ("flooding_nodes", "nodes", "Flooding, varying nodes"),
        ("raft_nodes", "nodes", "Raft (100 values), varying nodes"),
        ("raft_values", "values", "Raft (100 nodes), varying values"),
    ]
    for exp, xf, label in exps:
        print(f"  {label}:")
        curve = scalability_curve(benchmark, exp, xf)
        for x, (ms, mp, sp) in curve.items():
            sp_s = f"{sp:.3f}x" if sp is not None else "n/a"
            print(f"    {xf}={x:<5} seq={ms/1000:9.2f}ms  par={mp/1000:9.2f}ms  speedup={sp_s}")

    print()
    print("=" * 70)
    print("exp-broadcast-robustness.tex   (source: robustness_results.csv)")
    print("=" * 70)
    print("  flooding-ACK  crash_permanent  delivery_rate:")
    print("   ", fmt(broadcast_curve(robustness, "flooding_ack", "crash_permanent", "delivery_rate")))
    print("  naive         crash_temporary  delivery_rate:")
    print("   ", fmt(broadcast_curve(robustness, "flooding_naive", "crash_temporary", "delivery_rate")))
    print("  bracha        byz_active       delivery_rate:")
    print("   ", fmt(broadcast_curve(robustness, "bracha", "byz_active", "delivery_rate")))
    print("  bracha        byz_active       agreement_rate:")
    print("   ", fmt(broadcast_curve(robustness, "bracha", "byz_active", "agreement_rate")))

    print()
    print("=" * 70)
    print("exp-election-baseline.tex      (source: comparison_results.csv)")
    print("  metric: successful runs /10  (terminated AND values_correct==1)")
    print("=" * 70)
    for ft, label in [("crash_permanent", "crash"), ("byz_active", "byz_active")]:
        for algo in ["bully", "raft"]:
            print(f"  {algo:12s} {label:11s}:", fmt(election_curve(comparison, algo, ft)))

    print()
    print("=" * 70)
    print("exp-raft-robust.tex            (source: comparison_results.csv)")
    print("  metric: successful runs /10  (terminated AND values_correct==1)")
    print("=" * 70)
    print("  byz_active:")
    for algo in ["raft_robust", "raft"]:
        print(f"    {algo:12s}:", fmt(election_curve(comparison, algo, "byz_active")))
    print("  byz_leader (raft and raft_robust are identical):")
    for algo in ["raft", "raft_robust"]:
        print(f"    {algo:12s}:", fmt(election_curve(comparison, algo, "byz_leader")))


if __name__ == "__main__":
    main()
