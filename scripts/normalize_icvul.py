import pandas as pd
import ast
from pathlib import Path
from urllib.parse import urlparse

IN = Path("data/raw/icvul.csv")
OUT = Path("data/intermediate/commits_icvul.csv")

df = pd.read_csv(IN)

rows = []

for _, r in df.iterrows():
    repo_url = r.repo_url
    project = urlparse(repo_url).path.strip("/").split("/")[-1]

    # VFC
    rows.append({
        "commit_id": r.fc_hash,
        "repo_url": repo_url,
        "project": project,
        "label_type": "vulnerability",
        "commit_role": "VFC",
        "language": "C",
        "dataset_source": "ICVul",
        "cve_id": r.cve_id,
        "cwe_id": r.cwe_id
    })

    # VIC
    try:
        vccs = ast.literal_eval(r.vcc_hash)
    except:
        vccs = []

    for vcc in vccs:
        rows.append({
            "commit_id": vcc,
            "repo_url": repo_url,
            "project": project,
            "label_type": "vulnerability",
            "commit_role": "VIC",
            "language": "C",
            "dataset_source": "ICVul",
            "cve_id": r.cve_id,
            "cwe_id": r.cwe_id
        })

out = pd.DataFrame(rows)

MIN_COMMITS_PER_REPO = 5

repo_counts = out.groupby("repo_url").size()
valid_repos = repo_counts[repo_counts >= MIN_COMMITS_PER_REPO].index

out = out[out.repo_url.isin(valid_repos)]

out.drop_duplicates(subset=["commit_id"], inplace=True)

OUT.parent.mkdir(parents=True, exist_ok=True)
out.to_csv(OUT, index=False)

print(f"Saved {len(out)} ICVul commits → {OUT}")
