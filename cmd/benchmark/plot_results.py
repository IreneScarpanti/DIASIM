
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import sys
import os

def plot_wall_time(ax, grp_df, x_col, title, xlabel):
    c_seq, c_par = "#2563eb", "#dc2626"
    for mode, color, marker, ls in [
        ("sequential", c_seq, "o", "-"),
        ("parallel", c_par, "s", "--"),
    ]:
        md = grp_df[grp_df["mode"] == mode]
        stats = md.groupby(x_col)["wall_time_ms"].agg(["mean", "std"]).sort_index()
        ax.errorbar(stats.index, stats["mean"], yerr=stats["std"],
                    marker=marker, linestyle=ls, color=color,
                    capsize=3, label=mode, linewidth=1.8, markersize=6)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Wall time (ms)")
    ax.set_title(title)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(ticker.ScalarFormatter())


def plot_speedup(ax, grp_df, x_col, title, xlabel):
    seq_mean = grp_df[grp_df["mode"] == "sequential"].groupby(x_col)["wall_time_ms"].mean()
    par_mean = grp_df[grp_df["mode"] == "parallel"].groupby(x_col)["wall_time_ms"].mean()
    common = seq_mean.index.intersection(par_mean.index)
    speedup = seq_mean[common] / par_mean[common]

    color = "#10b981" if speedup.mean() > 1.0 else "#f59e0b"
    ax.plot(common, speedup, marker="D", linewidth=2.2, color=color, markersize=7)
    ax.axhline(y=1.0, color="gray", linestyle=":", alpha=0.7, linewidth=1.5)
    ax.fill_between(common, 1.0, speedup, where=speedup >= 1.0,
                    alpha=0.12, color="green", interpolate=True)
    ax.fill_between(common, 1.0, speedup, where=speedup < 1.0,
                    alpha=0.12, color="red", interpolate=True)

    # Annotate each point with the speedup value
    for x, y in zip(common, speedup):
        ax.annotate(f"{y:.2f}\u00d7", (x, y), textcoords="offset points",
                    xytext=(0, 10), ha="center", fontsize=7.5, fontweight="bold")

    ax.set_xlabel(xlabel)
    ax.set_ylabel("Speedup (seq / par)")
    ax.set_title(title)
    ax.set_xscale("log")
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(ticker.ScalarFormatter())
    ax.annotate("parallel faster \u2191", xy=(0.02, 0.93), xycoords="axes fraction",
                fontsize=8, color="green", alpha=0.8)
    ax.annotate("sequential faster \u2193", xy=(0.02, 0.03), xycoords="axes fraction",
                fontsize=8, color="red", alpha=0.8)


def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "cmd/benchmark/benchmark_results.csv"
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        sys.exit(1)

    df = pd.read_csv(csv_path)
    df["wall_time_ms"] = df["wall_time_us"] / 1000.0

    fig, axes = plt.subplots(2, 3, figsize=(19, 10))
    fig.suptitle(
        "DIASIM Benchmark — When Does Parallel (CMB) Help?\n"
        "8 CPUs, Full Mesh, FixedDelay=1, 10 reps/config",
        fontsize=14, fontweight="bold"
    )

    # ── Exp 1: Flooding varying nodes ─────────────────────────────────────
    exp1 = df[df["experiment"] == "flooding_nodes"]
    plot_wall_time(axes[0, 0], exp1, "nodes",
                   "Exp 1: Flooding — Wall Time", "Number of nodes")
    plot_speedup(axes[1, 0], exp1, "nodes",
                 "Exp 1: Flooding — Speedup", "Number of nodes")

    # ── Exp 2: Raft varying nodes ─────────────────────────────────────────
    exp2 = df[df["experiment"] == "raft_nodes"]
    plot_wall_time(axes[0, 1], exp2, "nodes",
                   "Exp 2: Raft (100 values) — Wall Time", "Number of nodes")
    plot_speedup(axes[1, 1], exp2, "nodes",
                 "Exp 2: Raft — Speedup vs Nodes", "Number of nodes")

    # ── Exp 3: Raft varying values ────────────────────────────────────────
    exp3 = df[df["experiment"] == "raft_values"]
    plot_wall_time(axes[0, 2], exp3, "values",
                   "Exp 3: Raft (100 nodes) — Wall Time", "Number of values")
    plot_speedup(axes[1, 2], exp3, "values",
                 "Exp 3: Raft — Speedup vs Values", "Number of values")

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    out_path = "cmd/benchmark/benchmark_plots.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Plots saved to {out_path}")
    plt.close()


if __name__ == "__main__":
    main()