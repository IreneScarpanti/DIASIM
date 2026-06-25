#!/usr/bin/env python3
"""Election benchmark plots: Bully vs Raft under crash and Byzantine."""
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import sys, os

def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "election_results.csv"
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}"); sys.exit(1)
    df = pd.read_csv(csv_path)

    algo_styles = {
        "bully": ("#f59e0b", "o", "-",  "Bully"),
        "raft":  ("#2563eb", "D", "--", "Raft"),
    }
    scenarios = [s for s in ["crash_permanent","byz_active","byz_leader"] if s in df["failure_type"].unique()]
    titles = {
        "crash_permanent": "Crash Permanente",
        "byz_active": "Byzantine Attivi\n(forge & flood)",
        "byz_leader": "Byzantine Leader\n(nodo con ID più alto)",
    }
    metrics = [
        ("leader_agreement", "Leader Agreement", "Leader Agreement"),
        ("values_correct", "Values Correct Rate", "Values Correct"),
    ]
    n_sc = len(scenarios)
    fig, axes = plt.subplots(len(metrics), n_sc, figsize=(7*n_sc, 5*len(metrics)))
    if n_sc == 1: axes = axes.reshape(len(metrics), 1)

    n_val = int(df["n"].iloc[0])
    fig.suptitle(f"DIASIM — Bully vs Raft\nn={n_val}, Full Mesh, SeededDelay [1,10], 10 reps",
                 fontsize=14, fontweight="bold")

    for col, sc in enumerate(scenarios):
        sd = df[df["failure_type"] == sc]
        for row, (metric, ylabel, mtitle) in enumerate(metrics):
            ax = axes[row, col]
            for algo, (color, marker, ls, label) in algo_styles.items():
                ad = sd[sd["algorithm"] == algo]
                if ad.empty: continue
                stats = ad.groupby("f")[metric].agg(["mean","std"]).sort_index()
                ax.errorbar(stats.index, stats["mean"], yerr=stats["std"],
                            marker=marker, linestyle=ls, color=color,
                            capsize=3, label=label, linewidth=2, markersize=5, alpha=0.9)
            ax.set_xlabel("Numero di nodi guasti (f)")
            ax.set_ylabel(ylabel)
            ax.set_title(f"{titles.get(sc,sc)}\n{mtitle}", fontsize=10, fontweight="bold")
            ax.set_ylim(-0.05, 1.15)
            ax.axhline(y=1.0, color="gray", linestyle=":", alpha=0.3)
            ax.axvline(x=(n_val-1)/3, color="red", linestyle="--", alpha=0.4)
            ax.axvline(x=n_val/2, color="blue", linestyle="--", alpha=0.4)
            ax.annotate("n/3", xy=((n_val-1)/3+0.3, 0.05), fontsize=7, color="red", alpha=0.7)
            ax.annotate("n/2", xy=(n_val/2+0.3, 0.10), fontsize=7, color="blue", alpha=0.7)
            ax.legend(fontsize=9)
            ax.grid(True, alpha=0.3)

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    plt.savefig("election_plots.png", dpi=150, bbox_inches="tight")
    print("Saved election_plots.png")

if __name__ == "__main__": main()