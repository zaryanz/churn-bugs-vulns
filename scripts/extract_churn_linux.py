import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm

from utils.git_utils import get_diffstat_metrics

df = pd.read_csv("data/intermediate/commits_dataset_linux.csv")

rows = []

for _, r in tqdm(df.iterrows(), total=len(df)):
    repo_path = Path("repos") / r.project
    if not repo_path.exists():
        continue
    else:
        repo_path = str(repo_path)

    added = deleted = modified = files = 0

    try:
        for c in Repository(repo_path, single=r.commit_id).traverse_commits():
            for m in c.modified_files:
                if r.language == "C++" and not m.filename.endswith((".c",".cpp",".h",".hpp")):
                    continue
                mod, add, rem = get_diffstat_metrics(m.added_lines, m.deleted_lines)
                modified += mod
                added += add
                deleted += rem
                files += 1
    except Exception as e:
        print("an error occurred: ", e)
        continue

    rows.append({
        "commit_id": r.commit_id,
        "num_lines_added": added,
        "num_lines_deleted": deleted,
        "num_lines_modified": modified,
        "files_changed": files
    })

out = pd.DataFrame(rows)
out.to_csv("data/intermediate/churn_linux.csv", index=False)
