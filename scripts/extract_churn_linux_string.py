import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm

from utils.git_utils import get_string_matching_metrics

# Load original dataset
df_full = pd.read_csv("data/intermediate/commits_dataset_linux.csv")

# 2. CHANGE: Sample 1000 commits (500 BFC / 500 BIC) to match your other datasets
df = df_full.groupby('commit_role', group_keys=False).sample(n=5000, random_state=42)

rows = []

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
                # Language filter (Note: Linux is primarily .c and .h)
                if not m.filename.endswith((".c", ".cpp", ".h", ".hpp")):
                    continue
                
                # 3. CHANGE: Extract the actual text lines for string matching
                # PyDriller gives us (line_number, line_text) in diff_parsed
                added_text = [l[1] for l in m.diff_parsed['added']]
                deleted_text = [l[1] for l in m.diff_parsed['deleted']]

                # 4. CHANGE: Use String Matching instead of Greedy
                mod, add, rem = get_string_matching_metrics(added_text, deleted_text)
                
                modified += mod
                added += add
                deleted += rem
                files += 1
                
    except Exception as e:
        print(f"An error occurred with commit {r.commit_id}: {e}")
        continue

    # Keep column names consistent with your TOSEM/Apache scripts
    rows.append({
        "commit_id": r.commit_id,
        "lines_added": added,
        "lines_removed": deleted,
        "lines_modified": modified,
        "files_changed": files,
        "commit_role": r.commit_role # Helpful to keep for stats
    })

out = pd.DataFrame(rows)
# Save as a specific "semantic" file so you don't overwrite your greedy results
out.to_csv("data/intermediate/churn_linux_semantic_sample.csv", index=False)

print("\nProcessing Complete. Sample stats:")
print(out.groupby('commit_role')['lines_modified'].describe())