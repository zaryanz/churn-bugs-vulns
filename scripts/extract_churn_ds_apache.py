import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm

from utils.git_utils import get_diffstat_metrics, get_string_matching_metrics

df = pd.read_csv("data/intermediate/commits_ds_apache.csv")

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
                if r.language == "Java" and not m.filename.endswith(".java"):
                    continue
                mod, add, rem = get_diffstat_metrics(m.added_lines, m.deleted_lines)

                # # Extract the actual text content of the changes
                # added_content = [line[1] for line in m.diff_parsed['added']]
                # deleted_content = [line[1] for line in m.diff_parsed['deleted']]
                
                # # Use the new string matching utility
                # mod, add, rem = get_string_matching_metrics(added_content, deleted_content)

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
        "files_changed": files,
        "Type": r.commit_role
    })

out = pd.DataFrame(rows)
out = out.drop_duplicates(subset=['commit_id', 'Type'])
out.to_csv("data/intermediate/churn_ds_apache.csv", index=False)
