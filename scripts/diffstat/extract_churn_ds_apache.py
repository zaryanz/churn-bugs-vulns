import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm
import os

from utils.diffstat import get_diffstat_metrics
from utils.file import is_test_file, is_source_code

df = pd.read_csv("data/intermediate/commits_ds_apache.csv")

rows = []

for _, r in tqdm(df.iterrows(), total=len(df), desc="Processing Apache (Java)"):
    repo_path = Path("repos") / r.project
    if not repo_path.exists():
        continue
    
    repo_path_str = str(repo_path)
    added = deleted = modified = files = 0

    try:
        for c in Repository(repo_path_str, single=r.commit_id).traverse_commits():
            for m in c.modified_files:
                file_path = m.new_path if m.new_path else ""
                
                if not is_source_code(m.filename, "Java"):
                    continue
        
                if is_test_file(file_path, m.filename):
                    continue
                
                add, rem, mod = get_diffstat_metrics(m.diff)

                added += add
                deleted += rem
                modified += mod
                files += 1
                
    except Exception as e:
        print(f"An error occurred at commit {r.commit_id}: {e}")
        continue

    # Append results if files were processed
    if files > 0:
        rows.append({
            "commit_id": r.commit_id,
            "lines_added": added,
            "lines_deleted": deleted,
            "lines_modified": modified,
            "files_changed": files,
            "commit_role": r.commit_role,
            "project": r.project
        })

out = pd.DataFrame(rows)
out = out.drop_duplicates(subset=['commit_id', 'commit_role'])
out.to_csv("data/intermediate/churn_ds_apache.csv", index=False)

print(f"\nCompleted! Saved results for {len(out)} Apache commits.")