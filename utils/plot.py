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

def plot_cwe_paired(
    df, n_min=5, omit_unpaired=False, figsize=(7.5, 3.2), width_ratios=[9, 21]
):
    """Plots paired Fixing vs. Introducing code churn per CWE.

    Parameters:
    - df: DataFrame containing CWE churn data.
    - n_min: Minimum sample count cutoff per role.
    - omit_unpaired: If True, drops the entire CWE if EITHER Fixing or Introducing < n_min.
                    If False, keeps single bars where one role meets n_min.
    """
    # 1. Calculate counts and medians per group
    # ... [your existing aggregation logic here] ...

    if omit_unpaired:
        # Require BOTH fixing and introducing counts to meet the threshold
        valid_cwes = df[
            (df["fixing_count"] >= n_min) & (df["introducing_count"] >= n_min)
        ]["cwe_id"]
    else:
        # Require AT LEAST ONE role to meet the threshold
        valid_cwes = df[
            (df["fixing_count"] >= n_min) | (df["introducing_count"] >= n_min)
        ]["cwe_id"]

    df_filtered = df[df["cwe_id"].isin(valid_cwes)].copy()

    # Apply cutoff to individual values below n_min
    df_filtered.loc[df_filtered["fixing_count"] < n_min, "fixing_median"] = (
        np.nan
    )
    df_filtered.loc[
        df_filtered["introducing_count"] < n_min, "introducing_median"
    ] = np.nan

    # 2. Render subplots using your established styling
    fig, (ax1, ax2) = plt.subplots(
        1,
        2,
        figsize=figsize,
        gridspec_kw={"width_ratios": width_ratios},
        sharey=True,
    )

    # ... [your plotting and formatting code] ...

    return fig, (ax1, ax2)

def compute_candlestick_stats(df, group_cols=("Language", "Dataset")):
    """
    df must have columns: whatever's in group_cols, plus lines_added/lines_removed/lines_modified.
    Returns a nested dict: {(lang, role): {metric_label: {median, q1, q3, min, max}}}
    """
    metrics = [
        ("lines_added", "Added"),
        ("lines_removed", "Removed"),
        ("lines_modified", "Modified"),
    ]

    data = {}
    for group_key, sub in df.groupby(list(group_cols)):
        data[group_key] = {}
        for col, label in metrics:
            data[group_key][label] = {
                "median": sub[col].median(),
                "q1": sub[col].quantile(0.25),
                "q3": sub[col].quantile(0.75),
                "min": sub[col].min(),
                "max": sub[col].max(),
                "n": sub[col].count(),
            }
    return data