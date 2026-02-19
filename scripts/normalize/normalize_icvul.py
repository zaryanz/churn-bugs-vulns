import pandas as pd
import ast
from pathlib import Path
from urllib.parse import urlparse

LINUX_DATA = Path("data/intermediate/commits_dataset_linux.csv") # to remove duplicates, since ICVul contains linux commits too

IN = Path("data/raw/icvul.csv")
OUT = Path("data/intermediate/commits_icvul_restricted.csv")

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

out.drop_duplicates(subset=["commit_id"], inplace=True)

if LINUX_DATA.exists():
    linux_ids = set(pd.read_csv(LINUX_DATA)['commit_id'])
    out = out[~out['commit_id'].isin(linux_ids)]
    print(f"Filtered out Linux duplicates. Remaining candidates: {len(out)}")

MAX_UNIQUE_REPOS = 20 
top_repos = out['repo_url'].value_counts().nlargest(MAX_UNIQUE_REPOS).index
out = out[out['repo_url'].isin(top_repos)]

if len(out) > 1000:
    out = out.sample(n=1000, random_state=42)

OUT.parent.mkdir(parents=True, exist_ok=True)
out.to_csv(OUT, index=False)

print(f"Saved {len(out)} ICVul commits from {out['repo_url'].nunique()} unique repos → {OUT}")
