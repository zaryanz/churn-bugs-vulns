from scipy.stats import mannwhitneyu
from cliffs_delta import cliffs_delta

def run_stats(df):
    """
    Performs Mann-Whitney U and Cliff's Delta on Vulnerability vs Bug.
    Includes descriptive labels and formatted statistical output.
    """
    metrics = ['lines_added', 'lines_removed', 'lines_modified']
    
    group_v = df[df['Dataset'] == 'Vulnerability']
    group_b = df[df['Dataset'] == 'Bug']
    print(f"\nMANN-WHITNEY U TEST & CLIFF'S DELTA (Vulnerability vs. Bug)\n")
    print(f"{'Metric':<18} | {'p-value':<12} | {'Cliff’s Delta':<20}")
    print("-" * 55)
    
    for metric in metrics:
        v_vals = group_v[metric]
        b_vals = group_b[metric]
        
        _, p = mannwhitneyu(v_vals, b_vals, alternative='two-sided')
        d, res = cliffs_delta(v_vals, b_vals)
        
        p_str = f"{p:.2e}" if p < 0.001 else f"{p:.5f}"
        
        display_name = metric.replace('_', ' ').title()
        print(f"{display_name:<18} | {p_str:<12} | {d:>7.3f} ({res:<10})")