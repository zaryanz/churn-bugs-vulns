import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm
import os
from multiprocessing import Pool, cpu_count, Lock
import signal

from utils.diffstat import get_diffstat_metrics
from utils.file import is_test_file, is_source_code

# Config paths
INPUT_CSV = "data/intermediate/commits_icvul.csv"
REPO_BASE_DIR = Path("repos")
OUTPUT_PATH = "data/intermediate/churn_icvul.csv"

# Global repository lock dictionary
repo_locks = {}

def timeout_handler(signum, frame):
    raise TimeoutError("Commit processing timed out")

def init_pool(locks):
    global repo_locks
    repo_locks = locks
    signal.signal(signal.SIGALRM, timeout_handler)

def process_single_row(row_dict):
    global repo_locks

    project = row_dict["project"]
    commit_id = row_dict["commit_id"]

    repo_path = REPO_BASE_DIR / project

    if not repo_path.exists():
        return None

    repo_path_str = str(repo_path)

    added = 0
    deleted = 0
    modified = 0
    files = 0

    with repo_locks[project]:
        signal.alarm(120)

        try:
            for c in Repository(repo_path_str, single=commit_id).traverse_commits():
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

        except TimeoutError:
            print(f"Timeout on commit {commit_id}, skipping")
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
            "commit_role": row_dict["commit_role"],
            "project": project,
            "dataset_source": "ICVul",
            "cve_id": row_dict.get("cve_id", "N/A")
        }

    return None


if __name__ == "__main__":

    df = pd.read_csv(INPUT_CSV)

    tasks = df.to_dict(orient="records")

    unique_projects = df["project"].unique()
    manager_locks = {project: Lock() for project in unique_projects}

    num_workers = int(
        os.environ.get(
            "SLURM_CPUS_PER_TASK",
            max(1, cpu_count() - 1)
        )
    )

    print(
        f"Spawning {num_workers} workers to process "
        f"{len(tasks)} commits across {len(unique_projects)} repos..."
    )

    rows = []

    with Pool(
        num_workers,
        initializer=init_pool,
        initargs=(manager_locks,)
    ) as pool:

        for result in tqdm(
            pool.imap(process_single_row, tasks),
            total=len(tasks),
            desc="Processing ICVul"
        ):
            if result is not None:
                rows.append(result)

    out = pd.DataFrame(rows)

    if not out.empty:
        out = out.drop_duplicates(
            subset=["commit_id", "commit_role"]
        )

    out.to_csv(OUTPUT_PATH, index=False)

    print(f"\nCompleted! Saved {len(out)} commits to {OUTPUT_PATH}")