import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm
import os
import argparse

from utils.diffstat import get_diffstat_metrics
from utils.file import is_test_file, is_source_code

parser = argparse.ArgumentParser()
parser.add_argument("--chunk", type=int, required=True)
parser.add_argument("--num-chunks", type=int, default=10)

args = parser.parse_args()

chunk_id = args.chunk
num_chunks = args.num_chunks

df = pd.read_csv("data/intermediate/commits_dataset_linux.csv")

# Shuffle before splitting (recommended)
df = df.sample(
    frac=1,
    random_state=42
).reset_index(drop=True)

chunk_size = len(df) // num_chunks

start = chunk_id * chunk_size

if chunk_id == num_chunks - 1:
    end = len(df)
else:
    end = (chunk_id + 1) * chunk_size

df = df.iloc[start:end]

print(
    f"Processing chunk "
    f"{chunk_id + 1}/{num_chunks} "
    f"({len(df):,} commits)"
)

rows = []

for _, r in tqdm(df.iterrows(), total=len(df), desc="Processing Linux Kernel"):
    repo_path = Path("repos") / r.project
    if not repo_path.exists():
        continue
    
    repo_path_str = str(repo_path)
    added = deleted = modified = files = 0

    try:
        for c in Repository(repo_path_str, single=r.commit_id).traverse_commits():
            for m in c.modified_files:
                file_path = m.new_path if m.new_path else ""
                
                if not is_source_code(m.filename, "C"):
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

    if files > 0:
        rows.append({
            "commit_id": r.commit_id,
            "lines_added": added,
            "lines_removed": deleted,
            "lines_modified": modified,
            "files_changed": files,
            "project": r.project,
            "commit_role": r.commit_role
        })

out = pd.DataFrame(rows)

output_path = (
    f"data/intermediate/"
    f"churn_linux_chunk_{chunk_id}.csv"
)

out.to_csv(output_path, index=False)

print(
    f"\nCompleted! Saved results for "
    f"{len(out)} Linux commits."
)

print(f"Output: {output_path}")

print(f"\nCompleted! Saved results for {len(out)} Linux commits.")