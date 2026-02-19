import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm
import os

from utils.diffstat import get_diffstat_metrics
from utils.file import is_test_file, is_source_code

INPUT_CSV = "data/intermediate/commits_tosem.csv"
REPO_BASE_DIR = Path("repos")
OUTPUT_PATH = "data/intermediate/churn_tosem.csv"

df = pd.read_csv(INPUT_CSV)

rows = []

for _, r in tqdm(df.iterrows(), total=len(df), desc="Processing TOSEM Java (diffstat -m)"):
    repo_path = REPO_BASE_DIR / r.project
    
    if not repo_path.exists():
        print(f"Missing repo: {repo_path}")
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
        print(f"\nError in commit {r.commit_id}: {e}")
        continue

    # Only append if we actually found and processed files
    if files > 0:
        rows.append({
            "commit_id": r.commit_id,
            "lines_added": added,
            "lines_removed": deleted,
            "lines_modified": modified,
            "files_changed": files,
            "commit_role": r.commit_role,
            "project": r.project,
            "dataset_source": "TOSEM"
        })

out = pd.DataFrame(rows)

out = out.drop_duplicates(subset=['commit_id', 'commit_role'])

out.to_csv(OUTPUT_PATH, index=False)

print(f"\nFinished! Processed {len(out)} commits.")