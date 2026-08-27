import matplotlib.pyplot as plt
from matplotlib.ticker import LogLocator, NullFormatter
import numpy as np
import pandas as pd

COLOR_FIX = "#3D6FA6"
COLOR_INTRO = "#5FA8A0"

def plot_cwe_median(df, dataset_name, commit_role):
    role_label = "Fixing" if commit_role == "VFC" else "Introducing"
    df_role = df[df["commit_role"] == commit_role]
    df_grouped = df_role.groupby("cwe")[["lines_added", "lines_removed", "lines_modified"]].median().reset_index()
    cwe_counts = df_role["cwe"].value_counts()

    metrics = {
        "lines_added": "Lines Added",
        "lines_removed": "Lines Removed",
        "lines_modified": "Lines Modified"
    }

    for col, label in metrics.items():
        fig, ax = plt.subplots(figsize=(14, 6))
        bars = ax.bar(df_grouped["cwe"], df_grouped[col], color="steelblue", edgecolor="black")
        
        for bar, cwe in zip(bars, df_grouped["cwe"]):
            n = cwe_counts[cwe]
            median_val = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                    f"n={n}\nMedian: {median_val:.1f}", ha="center", va="bottom", fontsize=8)
        
        ax.set_title(f"Median {label} per CWE — {dataset_name} ({role_label} Commits)")
        ax.set_xlabel("CWE")
        ax.set_ylabel(f"Median {label}")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        if dataset_name == "TOSEM" and label == "Lines Added":
            plt.yscale('log')
        plt.show()
        
def style_axis(ax):
    # Full bounding box
    for spine in ax.spines.values():
        spine.set_visible(True)
        spine.set_color("#333333")

    # Horizontal gridlines only, behind bars
    ax.grid(axis="y", color="#e5e5e5", linestyle="-", linewidth=0.8, zorder=0)
    ax.grid(axis="x", visible=False)

    # Ensure y-axis ticks and labels are always visible
    ax.tick_params(
        axis="both", which="major", labelsize=8, colors="#333333", left=True
    )
    ax.tick_params(axis="y", which="minor", left=False)  # hide minor ticks

def plot_cwe_single(df, dataset_name, ax, n_min=5):
    sub = df[df["dataset"] == dataset_name]
    stats = sub.groupby("cwe")["total_churn"].agg(median="median", n="count")
    stats = stats[stats["n"] >= n_min]
    med = stats["median"].sort_values(ascending=False)

    cmap = plt.get_cmap("viridis")
    colors = [cmap(i / max(len(med) - 1, 1)) for i in range(len(med))]

    ax.bar(range(len(med)), med.values, color=colors, edgecolor="none")
    clean_labels = [str(x).replace("CWE-", "") for x in med.index]
    ax.set_xticks(range(len(med)))
    ax.set_xticklabels(
    clean_labels,
    rotation=45,
    ha="right",
    rotation_mode="anchor",
    fontsize=9,
    )
    # ax.set_xlabel("Vulnerability Type (CWE ID)", fontsize=9, fontweight="bold")
    ax.set_yscale("log")
    ax.set_ylim(10, 2000)
    ax.yaxis.set_major_locator(LogLocator(base=10.0, subs=(1.0,)))
    ax.yaxis.set_minor_formatter(NullFormatter())
    # ax.set_ylabel("Median Churn per CWE", fontsize=9, fontweight="bold")
    ax.set_title(dataset_name, fontsize=9)
    style_axis(ax)

def plot_cwe_paired(df, dataset_name, ax, n_min=5):
    sub = df[df["dataset"] == dataset_name]
    stats = sub.groupby(["cwe", "commit_role"])["total_churn"].agg(median="median", n="count").reset_index()

    suppressed = stats["n"] < n_min
    stats.loc[suppressed, "median"] = np.nan

    piv_med = stats.pivot(index="cwe", columns="commit_role", values="median")
    piv_med = piv_med.dropna(how="all")  # drop CWEs where both roles are suppressed

    if "VIC" in piv_med.columns:
        piv_med = piv_med.sort_values("VIC", ascending=False)

    x = np.arange(len(piv_med))
    width = 0.38

    fix_vals = piv_med.get("VFC", pd.Series(index=piv_med.index))
    intro_vals = piv_med.get("VIC", pd.Series(index=piv_med.index))

    bars_fix = ax.bar(x - width/2, fix_vals, width=width, color=COLOR_FIX, label="Fixing", edgecolor="none")
    bars_intro = ax.bar(x + width/2, intro_vals, width=width, color=COLOR_INTRO, label="Introducing", edgecolor="none")

    clean_labels = [str(x).replace("CWE-", "") for x in piv_med.index]

    ax.set_xticks(x)
    ax.set_xticklabels(
    clean_labels,
    rotation=45,
    ha="right",
    rotation_mode="anchor",
    fontsize=9,
    )

    # ax.set_xlabel("Vulnerability Type (CWE ID)", fontsize=9, fontweight="bold")
    ax.set_yscale("log")
    ax.set_ylim(10, 2000)
    ax.yaxis.set_major_locator(LogLocator(base=10.0, subs=(1.0,)))
    ax.yaxis.set_minor_formatter(NullFormatter())
    # ax.set_ylabel("Median Churn per CWE", fontsize=9, fontweight="bold")
    ax.set_title(dataset_name, fontsize=9)
    style_axis(ax)

    return bars_fix, bars_intro