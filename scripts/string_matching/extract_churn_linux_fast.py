import os
import sys
import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

from utils.semantic import get_string_matching_metrics, get_hunks_from_diff
from utils.file import is_test_file, is_source_code

INPUT_CSV = "data/intermediate/commits_dataset_linux.csv"
OUTPUT_PATH = "data/intermediate/churn_linux_semantic.csv"

def process_single_commit(r):
    """Worker function that processes exactly one commit row."""
    repo_path = Path("repos") / r.project
    if not repo_path.exists():
        return None

    added = deleted = modified = files = 0

    try:
        # Pydriller traversal
        for c in Repository(str(repo_path), single=r.commit_id).traverse_commits():
            for m in c.modified_files:
                file_path = m.new_path if m.new_path else ""
                
                if not is_source_code(m.filename, "C") or is_test_file(file_path, m.filename):
                    continue
                
                if m.diff is None:
                    continue

                # Safety check for massive diffs (very common in Linux)
                if len(m.diff) > 500000:
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
        return None

    return {
        "commit_id": r.commit_id,
        "lines_added": added,
        "lines_removed": deleted,
        "lines_modified": modified,
        "files_changed": files,
        "commit_role": r.commit_role
    }


if __name__ == "__main__":
    # 1. Load original dataset
    df_full = pd.read_csv(INPUT_CSV)

    # 2. Sample 5000 commits
    df = df_full.groupby('commit_role', group_keys=False).sample(n=5000, random_state=42)

    # --- SLURM TEST GUARD ---
    if "--test" in sys.argv:
        print("⚠️ RUNNING IN TEST MODE: Limiting to first 5 rows.")
        df = df.head(5)
    # ------------------------

    tasks = [r for _, r in df.iterrows()]

    # Create the output directory and CSV file with a header immediately
    Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    header = ["commit_id", "lines_added", "lines_removed", "lines_modified", "files_changed", "commit_role"]
    
    # Only write header if we are starting fresh (not a test run overlapping)
    if not os.path.exists(OUTPUT_PATH) or "--test" in sys.argv:
        pd.DataFrame(columns=header).to_csv(OUTPUT_PATH, index=False)

    # Dynamically determine the number of cores allocated by Slurm (defaults to 4 locally)
    num_workers = int(os.environ.get("SLURM_CPUS_PER_TASK", cpu_count()))
    print(f"Starting execution pool using {num_workers} workers...")

    # Using Multiprocessing Pool
    with Pool(num_workers) as pool:
        # imap_unordered + chunksize=1 streams data to disk as soon as any worker finishes
        for result in tqdm(pool.imap_unordered(process_single_commit, tasks, chunksize=1), 
                           total=len(tasks), desc="Processing Linux Sample"):
            if result:
                # Save-As-You-Go: Append directly to CSV
                pd.DataFrame([result]).to_csv(OUTPUT_PATH, mode='a', header=False, index=False)

    print("\nProcessing Complete.")