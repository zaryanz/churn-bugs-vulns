import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm
import os

# Import the fast string matching method
from utils.git_utils import get_string_matching_metrics_fast 

# Config paths
INPUT_CSV = "data/intermediate/commits_icvul_restricted.csv"
REPO_BASE_DIR = Path("data/repos")
OUTPUT_PATH = "data/intermediate/icvul_semantic.csv"

# Load normalized metadata
df_full = pd.read_csv(INPUT_CSV)

df = df_full.groupby('commit_role', group_keys=False).sample(n=500, random_state=42)

rows = []

for _, r in tqdm(df.iterrows(), total=len(df), desc="Processing ICVul (C/C++)"):
    repo_path = REPO_BASE_DIR / r.project
    
    if not repo_path.exists():
        continue
    
    repo_path_str = str(repo_path)
    added = deleted = modified = files = 0

    try:
        # traverse_commits for specific commit hash
        for c in Repository(repo_path_str, single=r.commit_id).traverse_commits():
            for m in c.modified_files:
                file_path = m.new_path if m.new_path else ""
                file_path_lower = file_path.lower()
                
                # 1. C/C++ specific filter
                if not m.filename.lower().endswith((".c", ".cpp", ".cc", ".h", ".hpp")):
                    continue
                
                # 2. Test exclusion (adapted for C/C++ project structures)
                is_test = (
                    "/test/" in file_path_lower or 
                    "/tests/" in file_path_lower or 
                    "/unit_test/" in file_path_lower or
                    m.filename.lower().startswith("test_")
                )
                
                if is_test:
                    continue

                # Extract content
                added_text = [l[1] for l in m.diff_parsed['added']]
                deleted_text = [l[1] for l in m.diff_parsed['deleted']]

                # Method 2: String Matching
                mod, add, rem = get_string_matching_metrics_fast(added_text, deleted_text)
                
                modified += mod
                added += add
                deleted += rem
                files += 1
                
    except Exception as e:
        # Silently skip errors to keep progress moving
        continue

    if files > 0:
        rows.append({
            "commit_id": r.commit_id,
            "lines_added": added,
            "lines_removed": deleted,
            "lines_modified": modified,
            "lines_changed": added + deleted + modified,
            "files_changed": files,
            "commit_role": r.commit_role,
            "project": r.project,
            "dataset_source": "ICVul",
            "cve_id": r.get('cve_id', 'N/A')
        })

# Create and clean dataframe
out = pd.DataFrame(rows)
out = out.drop_duplicates(subset=['commit_id', 'commit_role'])
out.to_csv(OUTPUT_PATH, index=False)

# Quick sanity check printout
print(f"\nCompleted! Saved {len(out)} commits to {OUTPUT_PATH}")