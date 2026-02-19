import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm
import os

from utils.semantic import get_string_matching_metrics, get_hunks_from_diff
from utils.file import is_test_file, is_source_code


# Config paths
INPUT_CSV = "data/intermediate/commits_tosem.csv"
REPO_BASE_DIR = Path("repos")
OUTPUT_PATH = "data/intermediate/churn_tosem_semantic.csv"

# Load the normalized metadata
df = pd.read_csv(INPUT_CSV)

rows = []

for _, r in tqdm(df.iterrows(), total=len(df), desc="Processing TOSEM Java"):
    # The 'project' column was created during normalization to match folder names
    repo_path = REPO_BASE_DIR / r.project
    
    if not repo_path.exists():
        print(f"Missing repo: {repo_path}")
        continue
    
    repo_path_str = str(repo_path)
    added = deleted = modified = files = 0

    try:
        # traverse_commits with single=hash is the fastest way to target one commit
        for c in Repository(repo_path_str, single=r.commit_id).traverse_commits():
            for m in c.modified_files:
                file_path = m.new_path if m.new_path else ""
                
                if not is_source_code(m.filename, "Java"):
                    continue
        
                if is_test_file(file_path, m.filename):
                    continue
                
                hunks = get_hunks_from_diff(m.diff)

                for hunk_content in hunks:
                    lines = hunk_content.splitlines()
                    
                    hunk_added = [l[1:] for l in lines if l.startswith('+') and not l.startswith('+++')]
                    hunk_deleted = [l[1:] for l in lines if l.startswith('-') and not l.startswith('---')]
                    
                    if not hunk_added and not hunk_deleted:
                        continue

                    # Apply similarity matching ONLY to this hunk
                    mod, add, rem = get_string_matching_metrics(hunk_added, hunk_deleted)

                    modified += mod
                    added += add
                    deleted += rem
                    
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
            "lines_changed": added + deleted + modified,
            "files_changed": files,
            "commit_role": r.commit_role,
            "project": r.project,
            "dataset_source": "TOSEM"
        })

# Create final dataframe
out = pd.DataFrame(rows)

# Drop duplicates just in case (same commit might appear twice in normalized csv)
out = out.drop_duplicates(subset=['commit_id', 'commit_role'])

out.to_csv(OUTPUT_PATH, index=False)

print(f"\nFinished! Processed {len(out)} commits.")