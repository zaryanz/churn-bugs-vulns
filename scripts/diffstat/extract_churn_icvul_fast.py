import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm
import os
from multiprocessing import Pool, cpu_count

from utils.diffstat import get_diffstat_metrics
from utils.file import is_test_file, is_source_code

INPUT_CSV = "data/intermediate/commits_icvul_repo_restricted.csv"
REPO_BASE_DIR = Path("repos")
OUTPUT_PATH = "data/intermediate/churn_icvul_repo_restricted.csv"

def process_commit(r):
    repo_path = REPO_BASE_DIR / r.project
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
        print(f"Error in commit {r.commit_id}: {e}")
        return None

    if files > 0:
        return {
            "commit_id": r.commit_id,
            "lines_added": added,
            "lines_removed": deleted,
            "lines_modified": modified,
            "lines_changed": added + deleted + modified,
            "files_changed": files,
            "commit_role": r.commit_role,
            "project": r.project,
            "dataset_source": "ICVul",
            "cve_id": r.get('cve_id', 'N/A')
        }
    return None

if __name__ == "__main__":
    df = pd.read_csv(INPUT_CSV)
    rows_iter = [r for _, r in df.iterrows()]

    num_workers = cpu_count() - 1  # leave one core free
    print(f"Using {num_workers} workers")

    with Pool(num_workers) as pool:
        results = list(tqdm(pool.imap(process_commit, rows_iter), total=len(rows_iter), desc="Processing ICVul"))

    rows = [r for r in results if r is not None]
    out = pd.DataFrame(rows)
    out = out.drop_duplicates(subset=['commit_id', 'commit_role'])
    out.to_csv(OUTPUT_PATH, index=False)
    print(f"\nCompleted! Saved {len(out)} commits to {OUTPUT_PATH}")