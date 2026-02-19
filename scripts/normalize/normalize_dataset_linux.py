import json
import pandas as pd
from pathlib import Path

IN = Path("data/raw/dataset_linux.json")
OUT = Path("data/intermediate/commits_dataset_linux.csv")

rows = []

with open(IN) as f:
    data = json.load(f)

for entry in data:
    project = "linux"
    repo_url = "https://github.com/torvalds/linux"

    # Fixing commit (VFC)
    rows.append({
        "commit_id": entry["fix_commit_hash"],
        "repo_url": repo_url,
        "project": project,
        "label_type": "bug",
        "commit_role": "BFC",
        "language": "C",
        "dataset_source": "KFC"
    })

    # Bug-inducing commits (VIC)
    for bic in entry["bug_commit_hash"]:
        rows.append({
            "commit_id": bic,
            "repo_url": repo_url,
            "project": project,
            "label_type": "bug",
            "commit_role": "BIC",
            "language": "C",
            "dataset_source": "KFC"
        })

df = pd.DataFrame(rows)
df.drop_duplicates(subset=["commit_id"], inplace=True)
df.to_csv(OUT, index=False)

print(f"Saved {len(df)} Linux commits")
