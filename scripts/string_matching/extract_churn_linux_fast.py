import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import os

from utils.semantic import get_string_matching_metrics, get_hunks_from_diff
from utils.file import is_test_file, is_source_code

OUTPUT_PATH = "data/intermediate/churn_linux_semantic.csv"

def process_commit(r):
    repo_path = Path("repos") / r.project
    if not repo_path.exists():
        return None

    repo_path = str(repo_path)
    added = deleted = modified = files = 0

    try:
        for c in Repository(repo_path, single=r.commit_id).traverse_commits():
            for m in c.modified_files:
                file_path = m.new_path if m.new_path else ""

                if not is_source_code(m.filename, "C"):
                    continue
                if is_test_file(file_path, m.filename):
                    continue

                hunks = get_hunks_from_diff(m.diff)

                for hunk_content in hunks:
                    lines = hunk_content.splitlines()

                    hunk_added   = [l[1:] for l in lines if l.startswith('+') and not l.startswith('+++')]
                    hunk_deleted = [l[1:] for l in lines if l.startswith('-') and not l.startswith('---')]

                    if not hunk_added and not hunk_deleted:
                        continue

                    mod, add, rem = get_string_matching_metrics(hunk_added, hunk_deleted)

                    modified += mod
                    added    += add
                    deleted  += rem

                files += 1

    except Exception as e:
        print(f"An error occurred with commit {r.commit_id}: {e}")
        return None

    return {
        "commit_id":      r.commit_id,
        "lines_added":    added,
        "lines_removed":  deleted,
        "lines_modified": modified,
        "files_changed":  files,
        "commit_role":    r.commit_role
    }


def main():
    df_full = pd.read_csv("data/intermediate/commits_dataset_linux.csv")
    df = df_full.groupby('commit_role', group_keys=False).sample(n=5000, random_state=42)

    records = list(df.itertuples(index=False))

    n_workers = int(os.environ.get("SLURM_CPUS_PER_TASK", cpu_count()))
    print(f"Using {n_workers} workers for {len(records)} commits")

    with Pool(processes=n_workers) as pool:
        results = list(tqdm(
            pool.imap_unordered(process_commit, records),
            total=len(records),
            desc="Processing Linux Sample"
        ))

    rows = [r for r in results if r is not None]
    out  = pd.DataFrame(rows)
    out.to_csv(OUTPUT_PATH, index=False)

    print("\nProcessing Complete. Sample stats:")
    print(out.groupby('commit_role')['lines_modified'].describe())


if __name__ == "__main__":
    main()