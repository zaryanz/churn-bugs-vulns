import matplotlib.pyplot as plt

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