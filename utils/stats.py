from scipy.stats import mannwhitneyu
from cliffs_delta import cliffs_delta

def run_stats(df):
    """
    Performs Mann-Whitney U and Cliff's Delta on Vulnerability vs Bug.
    Includes descriptive labels and formatted statistical output.
    """
    metrics = ['lines_added', 'lines_removed', 'lines_modified']
    alpha = 0.05
    bonf_alpha = alpha / len(metrics)
    
    group_v = df[df['Dataset'] == 'Vulnerability']
    group_b = df[df['Dataset'] == 'Bug']
    print(f"\nMANN-WHITNEY U TEST & CLIFF'S DELTA (Vulnerability vs. Bug)\n")
    print(f"Bonferroni-corrected alpha: {bonf_alpha:.4f}\n")
    print(f"{'Metric':<18} | {'p-value':<12} | {'Cliff’s Delta':<20} | {'Sig'}")
    print("-" * 70)
    
    for metric in metrics:
        v_vals = group_v[metric]
        b_vals = group_b[metric]
        
        _, p = mannwhitneyu(v_vals, b_vals, alternative='two-sided')
        d, res = cliffs_delta(v_vals, b_vals)
        
        p_str = f"{p:.2e}" if p < 0.001 else f"{p:.5f}"
        sig = "*" if p < bonf_alpha else ""
        
        display_name = metric.replace('_', ' ').title()
        print(f"{display_name:<18} | {p_str:<12} | {d:>7.3f} ({res:<10}) | {sig}")

def run_pct_stats(df):
    metrics = ['pct_added', 'pct_removed', 'pct_modified']
    
    group_v = df[df['Dataset'] == 'Vulnerability']
    group_b = df[df['Dataset'] == 'Bug']
    
    alpha = 0.05
    bonf_alpha = alpha / len(metrics)

    print("\nMANN-WHITNEY U TEST (% Metrics)\n")
    print(f"Bonferroni-corrected alpha: {bonf_alpha:.4f}\n")

    print(f"{'Metric':<18} | {'p-value':<12} | {'Cliff’s Delta':<20} | {'Sig'}")
    print("-" * 70)

    for metric in metrics:
        v_vals = group_v[metric]
        b_vals = group_b[metric]

        _, p = mannwhitneyu(v_vals, b_vals, alternative='two-sided')
        d, res = cliffs_delta(v_vals, b_vals)

        p_str = f"{p:.2e}" if p < 0.001 else f"{p:.5f}"
        sig = "*" if p < bonf_alpha else ""

        display_name = metric.replace('_', ' ').title()
        
        print(f"{display_name:<18} | {p_str:<12} | {d:>7.3f} ({res:<10}) | {sig}")

import pandas as pd

def descriptive_stats_table(df):

    metrics = [
        ("lines_added", "Lines Added"),
        ("lines_removed", "Lines Removed"),
        ("lines_modified", "Lines Modified"),
    ]

    result = pd.DataFrame()

    for metric, title in metrics:

        grouped = df.groupby("Dataset")[metric]

        stats = pd.DataFrame({
            "n": grouped.count(),
            "Median": grouped.median(),
            "Q1": grouped.quantile(0.25),
            "Q3": grouped.quantile(0.75),
            "Minimum": grouped.min(),
            "Maximum": grouped.max(),
        }).T

        stats = stats[["Vulnerability", "Bug"]]

        stats.columns = pd.MultiIndex.from_product(
            [[title], stats.columns]
        )

        result = pd.concat([result, stats], axis=1)

    return result.round(1)