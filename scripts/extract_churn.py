import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm

df = pd.read_csv("data/intermediate/commits_ds_apache.csv")

rows = []

for _, r in tqdm(df.iterrows(), total=len(df)):
    repo_path = Path("repos") / r.project
    if not repo_path.exists():
        continue
    else:
        repo_path = str(repo_path)

    added = deleted = files = 0

    try:
        for c in Repository(repo_path, single=r.commit_id).traverse_commits():
            for m in c.modified_files:
                print(m)
                if r.language == "Java" and not m.filename.endswith(".java"):
                    continue
                if r.language == "C++" and not m.filename.endswith((".c",".cpp",".h",".hpp")):
                    continue
                added += m.added_lines
                deleted += m.deleted_lines
                files += 1
    except Exception as e:
        print("an error occurred: ", e)
        continue

    rows.append({
        "commit_id": r.commit_id,
        "loc_added": added,
        "loc_deleted": deleted,
        "churn": added + deleted,
        "files_changed": files
    })

out = pd.DataFrame(rows)
out.to_csv("data/intermediate/churn.csv", index=False)
