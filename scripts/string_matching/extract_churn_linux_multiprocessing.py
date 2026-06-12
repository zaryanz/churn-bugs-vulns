import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm
import os
from multiprocessing import Pool, cpu_count, Lock
import multiprocessing as mp
import signal

from utils.semantic import get_string_matching_metrics, get_hunks_from_diff
from utils.file import is_test_file, is_source_code

OUTPUT_PATH = "data/intermediate/churn_linux_semantic.csv"

# Global lock variable for background workers
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
        signal.alarm(120)
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
        except TimeoutError:
            print(f"Timeout on commit {commit_id}, skipping")
            return None
        except Exception as e:
            print(f"An error occurred fetching commit {commit_id}: {e}", flush=True)
            return None
        finally:
            signal.alarm(0)

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

    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--chunk",
        type=int,
        required=True,
        help="Chunk number to process (0-based)"
    )

    parser.add_argument(
        "--num-chunks",
        type=int,
        default=10,
        help="Total number of chunks"
    )

    args = parser.parse_args()

    chunk_id = args.chunk
    num_chunks = args.num_chunks

    # HPC Guardrail
    try:
        mp.set_start_method('forkserver')
    except RuntimeError:
        pass

    # -----------------------------
    # Load and split dataset
    # -----------------------------
    df = pd.read_csv(
        "data/intermediate/commits_dataset_linux.csv"
    )

    # Shuffle before splitting
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

    tasks = df.to_dict(orient='records')

    # -----------------------------
    # Multiprocessing setup
    # -----------------------------
    single_repo_lock = Lock()

    num_workers = int(
        os.environ.get(
            "SLURM_CPUS_PER_TASK",
            max(1, cpu_count() - 1)
        )
    )

    print(
        f"Spawning {num_workers} synchronized "
        f"parallel workers to analyze "
        f"{len(tasks)} Linux commits..."
    )

    rows = []

    with Pool(
        num_workers,
        initializer=init_pool,
        initargs=(single_repo_lock,)
    ) as pool:

        for result in tqdm(
            pool.imap(
                process_single_row,
                tasks
            ),
            total=len(tasks),
            desc=f"Chunk {chunk_id}"
        ):

            if result is not None:
                rows.append(result)

    out = pd.DataFrame(rows)

    output_path = (
        f"data/intermediate/"
        f"churn_linux_semantic_chunk_{chunk_id}.csv"
    )

    out.to_csv(output_path, index=False)

    print(
        f"\nCompleted! Saved results for "
        f"{len(out)} commits."
    )

    print(f"Output: {output_path}")

    if not out.empty:
        print(
            out.groupby('commit_role')[
                'lines_modified'
            ].describe()
        )