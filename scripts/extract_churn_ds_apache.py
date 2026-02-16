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
                if r.language == "Java" and not m.filename.endswith(".java"):
                    continue
                added += m.added_lines
                deleted += m.deleted_lines
                files += 1
    except Exception as e:
        print("an error occurred: ", e)
        continue

    rows.append({
        "commit_id": r.commit_id,
        "num_lines_added": added,
        "num_lines_deleted": deleted,
        "num_lines_changed": added + deleted,
        "files_changed": files,
        "Type": r.commit_role
    })

out = pd.DataFrame(rows)
out = out.drop_duplicates(subset=['commit_id', 'Type'])
out.to_csv("data/intermediate/churn_ds_apache.csv", index=False)
