import subprocess
import pandas as pd
from pathlib import Path
import glob

REPOS = Path("repos")
REPOS.mkdir(exist_ok=True)

# Load all commit CSVs
csv_files = glob.glob("data/intermediate/commits_*.csv")

seen = set()

for csv_path in csv_files:
    df = pd.read_csv(csv_path)

    for _, r in df.iterrows():
        key = (r.project, r.repo_url)
        if key in seen:
            continue
        seen.add(key)

        target = REPOS / r.project
        if target.exists():
            continue

        print(f"Cloning {r.project} from {r.repo_url}")
        subprocess.run(
            ["git", "clone", "--filter=blob:none", r.repo_url, str(target)],
            check=False
        )
