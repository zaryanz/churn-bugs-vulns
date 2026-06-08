import pandas as pd
from pydriller import Repository
from pathlib import Path
from tqdm import tqdm
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

from utils.semantic import get_string_matching_metrics, get_hunks_from_diff
from utils.file import is_test_file, is_source_code

# Config paths
INPUT_CSV = "data/intermediate/commits_icvul_repo_restricted.csv"
REPO_BASE_DIR = Path("repos")
OUTPUT_PATH = "data/intermediate/churn_icvul_semantic_repo_restricted.csv"

def process_single_commit(row_tuple):
    """
    Worker function to process a single commit row.
    Accepts a tuple (index, row_data) from df.iterrows()
    """
    _, r = row_tuple
    repo_path = REPO_BASE_DIR / r.project
    
    if not repo_path.exists():
        return None
    
    repo_path_str = str(repo_path)
    added = deleted = modified = files = 0

    try:
        # traverse_commits for specific commit hash
        for c in Repository(repo_path_str, single=r.commit_id).traverse_commits():
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

                    if len(m.diff) > 1000000: 
                        print(f"Skipping massive diff in {r.commit_id} ({len(m.diff)} chars)")
                        continue

                    # Apply similarity matching ONLY to this hunk
                    mod, add, rem = get_string_matching_metrics(hunk_added, hunk_deleted)
                    
                    modified += mod
                    added += add
                    deleted += rem
                    
                files += 1
                
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
                "cve_id": getattr(r, 'cve_id', 'N/A')
            }
            
    except Exception as e:
        # Note: Printing from multiple processes can sometimes look messy
        # print(f"Error in commit {r.commit_id}: {e}")
        return None
    
    return None

def main():
    # Load normalized metadata
    df = pd.read_csv(INPUT_CSV)
    
    # Convert dataframe rows to a list for the executor
    tasks = list(df.iterrows())
    rows = []

    # Use CPU count (M1 Air has 8 cores)
    num_workers = os.cpu_count()

    print(f"Starting parallel processing with {num_workers} workers...")

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Wrap the executor in tqdm for a progress bar
        futures = [executor.submit(process_single_commit, task) for task in tasks]
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing ICVul"):
            result = future.result()
            if result:
                rows.append(result)

    # Convert results to DataFrame
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.drop_duplicates(subset=['commit_id', 'commit_role'])
        out.to_csv(OUTPUT_PATH, index=False)
        print(f"\nCompleted! Saved {len(out)} commits to {OUTPUT_PATH}")
    else:
        print("No records processed.")

if __name__ == "__main__":
    # Required for multiprocessing on macOS
    main()