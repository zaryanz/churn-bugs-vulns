import pandas as pd
from pathlib import Path

COMMITS = Path("data/intermediate/commits_icvul.csv")
DIFFS = Path("data/raw/icvul_diffs.csv")
OUT = Path("data/intermediate/churn_icvul.csv")

commits = pd.read_csv(COMMITS)
diffs = pd.read_csv(DIFFS)

# Join diffs with commit roles
merged = diffs.merge(
    commits,
    left_on="hash",
    right_on="commit_id",
    how="inner"
)

# Aggregate churn per commit
churn = (
    merged
    .groupby(
        ["commit_id", "commit_role", "repo_url", "project", "dataset_source"]
    )
    .agg({
        "num_lines_added": "sum",
        "num_lines_deleted": "sum"
    })
    .reset_index()
)

churn["churn"] = (
    churn.num_lines_added +
    churn.num_lines_deleted
)

OUT.parent.mkdir(parents=True, exist_ok=True)
churn.to_csv(OUT, index=False)

print(f"Saved churn for {len(churn)} ICVul commits → {OUT}")
