import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm
import os
from multiprocessing import Pool, cpu_count, Lock
import multiprocessing as mp

from utils.semantic import get_string_matching_metrics, get_hunks_from_diff
from utils.file import is_test_file, is_source_code

OUTPUT_PATH = "data/intermediate/churn_linux_semantic.csv"

# Global lock variable for background workers
repo_lock = None

def init_pool(lck):
    """Initializes the single global lock inside background worker processes."""
    global repo_lock
    repo_lock = lck

def process_single_row(row_dict):
    """
    Processes a single Linux commit row.
    Uses a global lock strictly around the Git read operation to prevent .git/config.lock crashes.
    """
    global repo_lock
    project = row_dict['project']
    commit_id = row_dict['commit_id']
    
    repo_path = Path("repos") / project
    if not repo_path.exists():
        return None
    else:
        repo_path_str = str(repo_path)

    added = deleted = modified = files = 0

    # Wrap ONLY the PyDriller initialization inside the lock
    with repo_lock:
        try:
            # We fetch and isolate the modifications safely inside the lock
            commit_modifications = []
            for c in Repository(repo_path_str, single=commit_id).traverse_commits():
                for m in c.modified_files:
                    # Capture the data we need and get out of Git immediately
                    commit_modifications.append({
                        'filename': m.filename,
                        'new_path': m.new_path,
                        'diff': m.diff
                    })
        except Exception as e:
            print(f"An error occurred fetching commit {commit_id}: {e}", flush=True)
            return None

    # Heavy text processing runs COMPLETELY outside the lock, utilizing full CPU parallel power!
    for m in commit_modifications:
        file_path = m['new_path'] if m['new_path'] else ""
        
        if not is_source_code(m['filename'], "C"):
            continue
        if is_test_file(file_path, m['filename']):
            continue
        
        hunks = get_hunks_from_diff(m['diff'])

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

    return {
        "commit_id": commit_id,
        "lines_added": added,
        "lines_removed": deleted,
        "lines_modified": modified,
        "files_changed": files,
        "commit_role": row_dict['commit_role']
    }


if __name__ == "__main__":
    # HPC Guardrail: Prevent silent deadlocks on Linux shared filesystems
    try:
        mp.set_start_method('forkserver')
    except RuntimeError:
        pass

    # Load original dataset
    df_full = pd.read_csv("data/intermediate/commits_dataset_linux.csv")

    df = df_full.groupby('commit_role', group_keys=False).sample(n=1000, random_state=42)

    # Transform rows to a list of flat dicts
    tasks = df.to_dict(orient='records')

    # Create a single lock for the entire repository
    single_repo_lock = Lock()

    # Determine CPU worker allocation
    num_workers = int(os.environ.get("SLURM_CPUS_PER_TASK", max(1, cpu_count() - 1)))
    print(f"Spawning {num_workers} synchronized parallel workers to analyze {len(tasks)} Linux commits...")

    rows = []
    # Pass the lock to the pool
    with Pool(num_workers, initializer=init_pool, initargs=(single_repo_lock,)) as pool:
        for result in tqdm(pool.imap(process_single_row, tasks), total=len(tasks), desc="Processing Linux Sample"):
            if result is not None:
                rows.append(result)

    # Convert results back to DataFrame and run original group tracking steps
    out = pd.DataFrame(rows)
    out.to_csv(OUTPUT_PATH, index=False)

    print("\nProcessing Complete. Sample stats:")
    if not out.empty:
        print(out.groupby('commit_role')['lines_modified'].describe())
    else:
        print("No rows were processed successfully.")