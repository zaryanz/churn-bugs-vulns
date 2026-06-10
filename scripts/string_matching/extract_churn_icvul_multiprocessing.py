import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm
import os
from multiprocessing import Pool, cpu_count, Lock
import signal

from utils.semantic import get_string_matching_metrics, get_hunks_from_diff
from utils.file import is_test_file, is_source_code

# Config paths
INPUT_CSV = "data/intermediate/commits_icvul.csv"
REPO_BASE_DIR = Path("repos")
OUTPUT_PATH = "data/intermediate/churn_icvul_semantic.csv"

# Global dictionary to hold locks per repository to prevent Git Lock issues
repo_locks = {}

def timeout_handler(signum, frame):
    raise TimeoutError("Commit processing timed out")

def init_pool(locks):
    """Initializes global locks inside background worker processes."""
    global repo_locks
    repo_locks = locks
    signal.signal(signal.SIGALRM, timeout_handler)

def process_single_row(row_dict):
    """
    Processes a single row exactly like the original sequential loop.
    Uses a dynamic repository lock to prevent concurrent git access errors.
    """
    global repo_locks
    project = row_dict['project']
    commit_id = row_dict['commit_id']
    
    repo_path = REPO_BASE_DIR / project
    if not repo_path.exists():
        return None
    
    repo_path_str = str(repo_path)
    added = deleted = modified = files = 0

    # Acquire the lock for THIS specific repository name before touching Git
    with repo_locks[project]:
        signal.alarm(120)
        try:
            # traverse_commits for specific commit hash
            for c in Repository(repo_path_str, single=commit_id).traverse_commits():
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
            print(f"Timeout on commit {row_dict['commit_id']}, skipping")
            return None 
        except Exception as e:
            print(f"Error in commit {commit_id}: {e}")
            return None
        finally:
            signal.alarm(0)

    if files > 0:
        return {
            "commit_id": commit_id,
            "lines_added": added,
            "lines_removed": deleted,
            "lines_modified": modified,
            "lines_changed": added + deleted + modified,
            "files_changed": files,
            "commit_role": row_dict['commit_role'],
            "project": project,
            "dataset_source": "ICVul",
            "cve_id": row_dict.get('cve_id', 'N/A')
        }
    return None


if __name__ == "__main__":
    # Load normalized metadata
    df = pd.read_csv(INPUT_CSV)
    
    # Transform rows to a list of flat dicts to perfectly preserve original row logic
    tasks = df.to_dict(orient='records')

    # Create an independent multiprocessing Lock for every unique repository name
    unique_projects = df['project'].unique()
    manager_locks = {project: Lock() for project in unique_projects}

    # Determine CPU worker allocation
    num_workers = int(os.environ.get("SLURM_CPUS_PER_TASK", max(1, cpu_count() - 1)))
    print(f"Spawning {num_workers} workers to process {len(tasks)} items across {len(unique_projects)} repos Safely...")

    rows = []
    # Pass our repository locks map into the worker pool initialization
    with Pool(num_workers, initializer=init_pool, initargs=(manager_locks,)) as pool:
        # imap preserves structural data alignment and handles tracking smoothly
        for result in tqdm(pool.imap(process_single_row, tasks), total=len(tasks), desc="Processing ICVul (diffstat -m)"):
            if result is not None:
                rows.append(result)

    # Convert results back to DataFrame and run original deduplication steps
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.drop_duplicates(subset=['commit_id', 'commit_role'])
    out.to_csv(OUTPUT_PATH, index=False)

    print(f"\nCompleted! Saved {len(out)} commits to {OUTPUT_PATH}")