import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm
import os
from multiprocessing import Pool, cpu_count
from itertools import chain
import multiprocessing as mp
import signal

from utils.diffstat import get_diffstat_metrics
from utils.file import is_test_file, is_source_code

# Global config variables
OUTPUT_PATH = "data/intermediate/churn_linux.csv"

def timeout_handler(signum, frame):
    raise TimeoutError("Commit processing timed out")

def init_pool():
    signal.signal(signal.SIGALRM, timeout_handler)

def process_chunk(args):
    commits, repo_path_str = args
    repo_path = Path(repo_path_str)
    total = len(commits)

    results = []
    for i, row_dict in enumerate(commits):
        if i % 5 == 0:
            pct = (i / total) * 100
            print(f"Worker {repo_path.name}: {i}/{total} ({pct:.1f}%)", flush=True)
        
        commit_id = row_dict['commit_id']

        if not repo_path.exists():
            continue

        added = deleted = modified = files = 0

        signal.alarm(120)
        try:
            commit_modifications = []
            for c in Repository(str(repo_path), single=commit_id).traverse_commits():
                for m in c.modified_files:
                    commit_modifications.append({
                        'filename': m.filename,
                        'new_path': m.new_path,
                        'diff': m.diff
                    })
        except TimeoutError:
            print(f"Timeout on commit {commit_id}, skipping")
            continue
        except Exception as e:
            print(f"Error fetching commit {commit_id}: {e}", flush=True)
            continue
        finally:
            signal.alarm(0)

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
                print(f"Error parsing diff for {m['filename']} in {commit_id}: {e}")
                continue

        if files > 0:
            results.append({
                "commit_id":      commit_id,
                "lines_added":    added,
                "lines_removed":  deleted,
                "lines_modified": modified,
                "files_changed":  files,
                "project":        row_dict['project'],
                "commit_role":    row_dict['commit_role']
            })

    return results


if __name__ == "__main__":
    try:
        mp.set_start_method('forkserver')
    except RuntimeError:
        pass

    df_full = pd.read_csv("data/intermediate/commits_dataset_linux.csv")
    df = df_full.groupby('commit_role', group_keys=False).sample(n=500, random_state=42)
    tasks = df.to_dict(orient='records')

    num_workers = int(os.environ.get("SLURM_CPUS_PER_TASK", max(1, cpu_count() - 1)))

    worker_repos = [
        str(Path("repos") / f"linux_worker_{i}")
        for i in range(num_workers)
    ]
    print(f"Using {num_workers} pre-existing worker repos")

    chunks = [
        (tasks[i::num_workers], worker_repos[i])
        for i in range(num_workers)
    ]

    print(f"Spawning {num_workers} workers across {len(tasks)} commits (~{len(chunks[0][0])} each)...")

    with Pool(num_workers, initializer=init_pool) as pool:
        nested = list(tqdm(
            pool.imap_unordered(process_chunk, chunks),
            total=num_workers,
            desc="Processing Linux Kernel"
        ))

    rows = list(chain.from_iterable(nested))
    out = pd.DataFrame(rows)
    out.to_csv(OUTPUT_PATH, index=False)

    print(f"\nCompleted! Saved results for {len(out)} Linux commits.")