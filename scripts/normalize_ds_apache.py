import json
import pandas as pd
from pathlib import Path

IN = Path("data/raw/DS_APACHE.json")
OUT = Path("data/intermediate/commits_ds_apache.csv")

rows = []

with open(IN) as f:
    data = json.load(f)

for entry in data:
    project = entry["repo_name"]
    repo_url = f"https://github.com/apache/{project}"

    # fixing commit (BFC)
    rows.append({
        "commit_id": entry["bug_fixing_commit"],
        "repo_url": repo_url,
        "project": project,
        "label_type": "bug",
        "commit_role": "BFC",
        "language": "Java",
        "dataset_source": "DS_APACHE"
    })

    # inducing commits (BICs)
    for bic in entry["bug_inducing_commit"]:
        rows.append({
            "commit_id": bic,
            "repo_url": repo_url,
            "project": project,
            "label_type": "bug",
            "commit_role": "BIC",
            "language": "Java",
            "dataset_source": "DS_APACHE"
        })

df = pd.DataFrame(rows)
df.drop_duplicates(subset=["commit_id"], inplace=True)

OUT.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT, index=False)

print(f"Saved {len(df)} commits → {OUT}")
