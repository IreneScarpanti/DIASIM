import pandas as pd
import matplotlib.pyplot as plt
import sys, os

def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "cmd/robustness/robustness_results.csv"
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}"); sys.exit(1)
    df = pd.read_csv(csv_path)

    algo_styles = {
        "flooding_naive": ("#f59e0b", "o", "-",  "Flooding Naive"),
        "flooding_ack":   ("#2563eb", "s", "--", "Flooding ACK"),
        "bracha":         ("#10b981", "D", "-",  "Bracha"),
    }
    scenario_titles = {
        "crash_temporary":  "Crash Temporaneo\n(crash t=1, recovery t=15)",
        "crash_permanent":  "Crash Permanente",
        "byz_active":       "Byzantine Attivi\n(forge & flood)",
        "byz_broadcaster":  "Byzantine Broadcaster\n(equivocazione)",
    }
    scenarios = [s for s in scenario_titles if s in df["failure_type"].unique()]
    n_sc = len(scenarios)
    n_val = int(df["n"].iloc[0])
    threshold = (n_val - 1) / 3

    fig, axes = plt.subplots(2, n_sc, figsize=(6.5 * n_sc, 10))
    if n_sc == 1: axes = axes.reshape(2, 1)

    fig.suptitle(
        f"DIASIM — Robustness Benchmark\n"
        f"n={n_val}, Full Mesh, SeededDelay [1,10], f = 0..{int(df['f'].max())}, 10 reps",
        fontsize=14, fontweight="bold")

    for col, scenario in enumerate(scenarios):
        sd = df[df["failure_type"] == scenario]
        title = scenario_titles.get(scenario, scenario)
        for row, (metric, ylabel, mtitle) in enumerate([
            ("delivery_rate", "Delivery Rate (valore corretto)", "Delivery Rate"),
            ("agreement_rate", "Agreement Rate", "Agreement Rate"),
        ]):
            ax = axes[row, col]
            for algo, (color, marker, ls, label) in algo_styles.items():
                ad = sd[sd["algorithm"] == algo]
                if ad.empty: continue
                stats = ad.groupby("f")[metric].agg(["mean", "std"]).sort_index()
                ax.errorbar(stats.index, stats["mean"], yerr=stats["std"],
                            marker=marker, linestyle=ls, color=color,
                            capsize=3, label=label, linewidth=2, markersize=5, alpha=0.9)
            ax.set_xlabel("Numero di nodi guasti (f)")
            ax.set_ylabel(ylabel)
            ax.set_title(f"{title}\n{mtitle}", fontsize=10, fontweight="bold")
            ax.set_ylim(-0.05, 1.15)
            ax.axhline(y=1.0, color="gray", linestyle=":", alpha=0.3)
            ax.axvline(x=threshold, color="red", linestyle="--", alpha=0.5, linewidth=1.5)
            ax.annotate(f"f=n/3={threshold:.0f}", xy=(threshold+0.3, 0.05),
                        fontsize=7, color="red", alpha=0.8)
            ax.legend(fontsize=7, loc="lower left")
            ax.grid(True, alpha=0.3)

    plt.tight_layout(rect=[0, 0, 1, 0.91])
    plt.savefig("cmd/robustness/robustness_plots.png", dpi=150, bbox_inches="tight")
    print("Plots saved to robustness_plots.png")

if __name__ == "__main__": main()