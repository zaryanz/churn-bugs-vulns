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

def plot_cwe_paired(df, dataset_name, ax, n_min=5, require_both=False):
    sub = df[df["dataset"] == dataset_name]
    stats = sub.groupby(["cwe", "commit_role"])["total_churn"].agg(median="median", n="count").reset_index()

    suppressed = stats["n"] < n_min
    stats.loc[suppressed, "median"] = np.nan

    piv_med = stats.pivot(index="cwe", columns="commit_role", values="median")

    # ensure both role columns exist even if one role is entirely missing for this dataset
    for role in ["VFC", "VIC"]:
        if role not in piv_med.columns:
            piv_med[role] = np.nan

    if require_both:
        piv_med = piv_med.dropna(how="any")   # keep only CWEs where BOTH roles meet n_min
    else:
        piv_med = piv_med.dropna(how="all")   # keep CWEs where AT LEAST ONE role meets n_min

    if "VIC" in piv_med.columns:
        piv_med = piv_med.sort_values("VIC", ascending=False)

    x = np.arange(len(piv_med))
    width = 0.38

    fix_vals = piv_med["VFC"]
    intro_vals = piv_med["VIC"]

    bars_fix = ax.bar(x - width/2, fix_vals, width=width, color=COLOR_FIX, label="Fixing", edgecolor="none")
    bars_intro = ax.bar(x + width/2, intro_vals, width=width, color=COLOR_INTRO, label="Introducing", edgecolor="none")

    clean_labels = [str(c).replace("CWE-", "") for c in piv_med.index]
    ax.set_xticks(x)
    ax.set_xticklabels(clean_labels, rotation=45, ha="right", rotation_mode="anchor", fontsize=9)

    ax.set_yscale("log")
    # ax.set_ylim(10, 2000)
    ax.yaxis.set_major_locator(LogLocator(base=10.0, subs=(1.0,)))
    ax.yaxis.set_minor_formatter(NullFormatter())
    ax.set_title(dataset_name, fontsize=9)
    style_axis(ax)

    return bars_fix, bars_intro

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

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import LogLocator, NullFormatter


def plot_cwe_dumbbell(
    df,
    dataset_name,
    ax,
    n_min=5,
    require_both=True,
    show_xlabel=False
):
    """
    Plot median total churn for Fixing and Introducing commits by CWE.

    Parameters
    ----------
    df : pandas.DataFrame
        Must contain:
        - dataset
        - cwe
        - commit_role
        - total_churn

    dataset_name : str
        Dataset to plot, e.g. "Hinrichs" or "ICVul".

    ax : matplotlib.axes.Axes
        Axis on which to draw the plot.

    n_min : int, default=5
        Minimum number of commits required for a CWE-role group.

    require_both : bool, default=True
        If True, retain only CWEs with both Fixing and Introducing medians.

    show_xlabel : bool, default=False
        Whether to show the x-axis label on this panel.
    """

    # Filter to the selected dataset
    sub = df[df["dataset"] == dataset_name].copy()

    # Compute median churn and sample size for every CWE and role
    stats = (
        sub.groupby(["cwe", "commit_role"])["total_churn"]
        .agg(median="median", n="count")
        .reset_index()
    )

    # Suppress groups below the minimum sample-size threshold
    stats.loc[stats["n"] < n_min, "median"] = np.nan

    # Convert role values into columns
    piv_med = stats.pivot(
        index="cwe",
        columns="commit_role",
        values="median"
    )

    # Ensure both expected roles exist
    for role in ["VFC", "VIC"]:
        if role not in piv_med.columns:
            piv_med[role] = np.nan

    # Retain only complete Fixing–Introducing pairs by default
    if require_both:
        piv_med = piv_med.dropna(subset=["VFC", "VIC"])
    else:
        piv_med = piv_med.dropna(subset=["VFC", "VIC"], how="all")

    # Sort by Introducing median, highest at the top
    piv_med = piv_med.sort_values(
        by="VIC",
        ascending=False,
        na_position="first"
    )

    # Check that all plotted values are valid for a log axis
    plotted_values = piv_med[["VFC", "VIC"]].to_numpy(dtype=float)

    if np.any(plotted_values <= 0):
        invalid_cwes = piv_med.index[
            (piv_med[["VFC", "VIC"]] <= 0).any(axis=1)
        ].tolist()

        raise ValueError(
            "A logarithmic axis requires strictly positive medians. "
            f"Non-positive values found for: {invalid_cwes}"
        )

    y = np.arange(len(piv_med))

    fixing = piv_med["VFC"].to_numpy(dtype=float)
    introducing = piv_med["VIC"].to_numpy(dtype=float)

    # Draw connectors first so that markers appear on top
    for yi, fix_value, intro_value in zip(y, fixing, introducing):
        ax.plot(
            [fix_value, intro_value],
            [yi, yi],
            color="0.55",
            linewidth=1.2,
            solid_capstyle="round",
            zorder=1
        )

    # Fixing medians: blue circles
    fixing_points = ax.scatter(
        fixing,
        y,
        s=48,
        marker="o",
        color=COLOR_FIX,
        edgecolor="white",
        linewidth=0.8,
        zorder=3,
        label="Fixing"
    )

    # Introducing medians: teal squares
    introducing_points = ax.scatter(
        introducing,
        y,
        s=48,
        marker="s",
        color=COLOR_INTRO,
        edgecolor="white",
        linewidth=0.8,
        zorder=3,
        label="Introducing"
    )

    # CWE labels
    clean_labels = [
        str(cwe).replace("CWE-", "")
        for cwe in piv_med.index
    ]

    ax.set_yticks(y)
    ax.set_yticklabels(clean_labels, fontsize=8)

    # Put the largest Introducing median at the top
    ax.invert_yaxis()

    # Logarithmic x-axis
    ax.set_xscale("log")

    ax.xaxis.set_major_locator(
        LogLocator(base=10.0, subs=(1.0,))
    )
    ax.xaxis.set_minor_locator(
        LogLocator(base=10.0, subs=np.arange(2, 10) * 0.1)
    )
    ax.xaxis.set_minor_formatter(NullFormatter())

    # Panel title
    ax.set_title(dataset_name, fontsize=9)

    # Gridlines: useful only along the quantitative axis
    ax.set_axisbelow(True)

    ax.grid(
        axis="x",
        which="major",
        color="0.86",
        linewidth=0.65
    )
    print(ax.get_xlim())

    ax.grid(axis="y", visible=False)
    style_axis(ax)

    return fixing_points, introducing_points

FIG_LAYOUT = {
    "left": 0.10,
    "right": 0.98,
    "bottom": 0.20,
    "top": 0.86,
    "wspace": 0.08,
    "hspace": 0.08,
}


def format_figure(
    fig,
    *,
    xlabel=None,
    ylabel=None,
    legend_handles=None,
    legend_labels=None,
    legend_y=0.975,
    xlabel_y=0.055,
    ylabel_x=0.025,
    layout=None,
):
    """
    Apply consistent spacing and typography to a Matplotlib figure.
    """

    layout = layout or FIG_LAYOUT

    # Apply identical subplot margins
    fig.subplots_adjust(**layout)

    # Consistent axis-label spacing and tick-label spacing
    for ax in fig.axes:
        ax.xaxis.labelpad = 6
        ax.yaxis.labelpad = 6
        ax.tick_params(axis="both", which="major", pad=3)

    # Figure-level labels
    if ylabel is not None:
        fig.supylabel(
            ylabel,
            x=ylabel_x,
            fontsize=9,
            fontweight="bold",
            va="center",
        )

    if xlabel is not None:
        fig.supxlabel(
            xlabel,
            y=xlabel_y,
            fontsize=9,
            fontweight="bold",
        )

    # Figure-level legend
    if legend_handles is not None:
        fig.legend(
            handles=legend_handles,
            labels=legend_labels,
            loc="upper center",
            bbox_to_anchor=(0.5, legend_y),
            ncol=2,
            frameon=False,
            fontsize=9,
            handletextpad=0.5,
            columnspacing=1.4,
        )

    return fig


def save_figure(fig, filename):
    """
    Export figures with identical physical dimensions and margins.
    """
    fig.savefig(
        filename,
        format="pdf",
        bbox_inches=None,
        pad_inches=0,
    )