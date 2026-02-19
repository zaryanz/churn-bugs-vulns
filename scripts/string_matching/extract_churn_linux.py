import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm

from utils.semantic import get_string_matching_metrics, get_hunks_from_diff
from utils.file import is_test_file, is_source_code

# Load original dataset
df_full = pd.read_csv("data/intermediate/commits_dataset_linux.csv")

# 2. CHANGE: Sample 1000 commits (500 BFC / 500 BIC) to match your other datasets
df = df_full.groupby('commit_role', group_keys=False).sample(n=5000, random_state=42)

rows = []

OUTPUT_PATH = "data/intermediate/churn_linux_semantic.csv"

for _, r in tqdm(df.iterrows(), total=len(df), desc="Processing Linux Sample"):
    repo_path = Path("repos") / r.project
    if not repo_path.exists():
        continue
    else:
        repo_path = str(repo_path)

    added = deleted = modified = files = 0

    try:
        for c in Repository(repo_path, single=r.commit_id).traverse_commits():
            for m in c.modified_files:
                file_path = m.new_path if m.new_path else ""
                
                if not is_source_code(m.filename, "C"):
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
        print(f"An error occurred with commit {r.commit_id}: {e}")
        continue

    rows.append({
        "commit_id": r.commit_id,
        "lines_added": added,
        "lines_removed": deleted,
        "lines_modified": modified,
        "files_changed": files,
        "commit_role": r.commit_role
    })

out = pd.DataFrame(rows)

out.to_csv(OUTPUT_PATH, index=False)

print("\nProcessing Complete. Sample stats:")
print(out.groupby('commit_role')['lines_modified'].describe())