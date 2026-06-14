import pandas as pd
import sys
import numpy as np
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm
import signal

from utils.semantic import get_string_matching_metrics, get_hunks_from_diff
from utils.file import is_test_file, is_source_code

def timeout_handler(signum, frame):
    raise TimeoutError("Commit processing timed out!")

# Check for valid command line argument (0-9)
if len(sys.argv) < 2 or not sys.argv[1].isdigit() or not (0 <= int(sys.argv[1]) <= 9):
    print("Error: Please provide a part number between 0 and 9. Example: python script.py 0")
    sys.exit(1)

part_idx = int(sys.argv[1])

# Load original dataset
df_full = pd.read_csv("data/intermediate/commits_dataset_linux.csv")

# Split dataframe into 10 parts and select the requested one
df_parts = np.array_split(df_full, 10)
df = df_parts[part_idx]

rows = []

# Dynamically change output path based on the part number
OUTPUT_PATH = f"data/intermediate/churn_linux_semantic_part_{part_idx}.csv"

signal.signal(signal.SIGALRM, timeout_handler)
repo_path = Path("/work/cps/cgk3480/churn-bugs-vulns/repos/linux_worker_{part_idx}")

if not repo_path.exists():
    raise FileNotFoundError(f"Directory missing: {repo_path}")

for _, r in tqdm(df.iterrows(), total=len(df), desc=f"Processing Linux Part {part_idx}"):
    added = deleted = modified = files = 0

    try:
        signal.alarm(120)

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
    except TimeoutError:
        print(f"\nSkipping commit {r.commit_id} because it took longer than 120 seconds.")
        continue     
    except Exception as e:
        print(f"An error occurred with commit {r.commit_id}: {e}")
        continue
    finally:
        signal.alarm(0)

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