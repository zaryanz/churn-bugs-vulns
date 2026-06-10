import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm
import os
from multiprocessing import Pool, cpu_count, Lock
import multiprocessing as mp
import signal

from utils.diffstat import get_diffstat_metrics
from utils.file import is_test_file, is_source_code

# Global config variables
OUTPUT_PATH = "data/intermediate/churn_linux.csv"
repo_lock = None

def init_pool(lck):
    """Initializes the single global lock inside background worker processes."""
    global repo_lock
    repo_lock = lck
    signal.signal(signal.SIGALRM, timeout_handler)

def timeout_handler(signum, frame):
    raise TimeoutError("Commit processing timed out")

def process_single_row(row_dict):
    """
    Processes a single Linux commit row using the exact structural pattern
    of the semantic script. Uses a global lock strictly around the Git read operation.
    """
    global repo_lock
    project = row_dict['project']
    commit_id = row_dict['commit_id']
    
    repo_path = Path("repos") / project
    if not repo_path.exists():
        return None
    
    repo_path_str = str(repo_path)
    added = deleted = modified = files = 0

    # Wrap ONLY the PyDriller initialization inside the lock to prevent file lock crashes
    with repo_lock:
        signal.alarm(120)  # 2-minute safety timeout per commit fetch
        try:
            commit_modifications = []
            for c in Repository(repo_path_str, single=commit_id).traverse_commits():
                for m in c.modified_files:
                    # Capture the data we need and get out of Git immediately
                    commit_modifications.append({
                        'filename': m.filename,
                        'new_path': m.new_path,
                        'diff': m.diff
                    })
        except TimeoutError:
            print(f"Timeout on commit {commit_id}, skipping")
            return None
        except Exception as e:
            print(f"An error occurred fetching commit {commit_id}: {e}", flush=True)
            return None
        finally:
            signal.alarm(0)

    # Diff parsing runs COMPLETELY outside the lock, utilizing full CPU parallel power!
    for m in commit_modifications:
        file_path = m['new_path'] if m['new_path'] else ""
        
        if not is_source_code(m['filename'], "C"):
            continue
        if is_test_file(file_path, m['filename']):
            continue
            
        try:
            add, rem, mod = get_diffstat_metrics(m['diff'])
            
            added += add
            deleted += rem
            modified += mod
            files += 1
        except Exception as e:
            print(f"Error parsing diff data for file {m['filename']} in commit {commit_id}: {e}")
            continue

    # Only return the data if matching files were modified
    if files > 0:
        return {
            "commit_id": commit_id,
            "lines_added": added,
            "lines_removed": deleted,
            "lines_modified": modified,
            "files_changed": files,
            "project": project,
            "commit_role": row_dict['commit_role']
        }
    return None


if __name__ == "__main__":
    # HPC Guardrail: Prevent silent deadlocks on Linux shared filesystems
    try:
        mp.set_start_method('forkserver')
    except RuntimeError:
        pass

    # Load and subset original dataset exactly like your single-threaded script
    df_full = pd.read_csv("data/intermediate/commits_dataset_linux.csv")
    df = df_full.groupby('commit_role', group_keys=False).sample(n=500, random_state=42)

    # Transform DataFrame rows to a list of flat dicts for multiprocessing map
    tasks = df.to_dict(orient='records')

    # Create a single shared lock for repository file safety
    single_repo_lock = Lock()

    # Determine CPU worker allocation favoring SLURM configurations
    num_workers = int(os.environ.get("SLURM_CPUS_PER_TASK", max(1, cpu_count() - 1)))
    print(f"Spawning {num_workers} synchronized parallel workers to analyze {len(tasks)} Linux commits...")

    rows = []
    # Pass the lock to the pool initialization
    with Pool(num_workers, initializer=init_pool, initargs=(single_repo_lock,)) as pool:
        for result in tqdm(pool.imap(process_single_row, tasks), total=len(tasks), desc="Processing Linux Kernel"):
            if result is not None:
                rows.append(result)

    # Convert results back to DataFrame and write to disk
    out = pd.DataFrame(rows)
    out.to_csv(OUTPUT_PATH, index=False)

    print(f"\nCompleted! Saved results for {len(out)} Linux commits.")