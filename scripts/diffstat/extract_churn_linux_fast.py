import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm
import os, time
from multiprocessing import Pool, cpu_count

from utils.diffstat import get_diffstat_metrics
from utils.file import is_test_file, is_source_code

def process_commit(r):
    repo_path = Path("repos") / r.project
    if not repo_path.exists():
        return None

    added = deleted = modified = files = 0

    try:
        for c in Repository(str(repo_path), single=r.commit_id).traverse_commits():
            for m in c.modified_files:
                file_path = m.new_path if m.new_path else ""

                if not is_source_code(m.filename, "C"):
                    continue

                if is_test_file(file_path, m.filename):
                    continue

                if len(m.diff) > 1000000:
                    print(f"Skipping massive diff in {r.commit_id} ({len(m.diff)} chars)")
                    continue

                add, rem, mod = get_diffstat_metrics(m.diff)

                added += add
                deleted += rem
                modified += mod
                files += 1

    except Exception as e:
        if "config.lock" in str(e):
            time.sleep(0.5)
            return process_commit(r)   # retry once
        print(f"An error occurred at commit {r.commit_id}: {e}")
        return None

    if files > 0:
        return {
            "commit_id": r.commit_id,
            "lines_added": added,
            "lines_removed": deleted,
            "lines_modified": modified,
            "files_changed": files,
            "project": r.project,
            "commit_role": r.commit_role
        }
    return None

if __name__ == "__main__":
    df = pd.read_csv("data/intermediate/commits_dataset_linux.csv")
    rows_iter = [r for _, r in df.iterrows()]

    num_workers = 4
    print(f"Using {num_workers} workers")

    with Pool(num_workers) as pool:
        results = list(tqdm(pool.imap(process_commit, rows_iter), total=len(rows_iter), desc="Processing Linux Kernel"))

    rows = [r for r in results if r is not None]
    out = pd.DataFrame(rows)
    out.to_csv("data/intermediate/churn_linux_full_dataset.csv", index=False)

    print(f"\nCompleted! Saved results for {len(out)} Linux commits.")