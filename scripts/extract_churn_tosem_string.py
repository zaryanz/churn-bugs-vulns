import pandas as pd
import json
import os
from pydriller import Repository
from utils.git_utils import get_string_matching_metrics

# Config
JSON_PATH = 'data/raw/tool_assisted_manual_dataset.json'
REPO_BASE_DIR = 'data/repos' 
OUTPUT_PATH = 'data/intermediate/java_vuln_manual_semantic.csv'

with open(JSON_PATH, 'r') as f:
    data = json.load(f)

rows = []

for entry in data:
    repo_url = entry.get('repository')
    repo_path = os.path.join(REPO_BASE_DIR, repo_url.split('/')[-1])
    
    # Check if repo exists locally
    if not os.path.exists(repo_path):
        continue

    for stat_key, commit_type in [('fixing_stats', 'VFC'), ('intro_stats', 'VIC')]:
        stats_dict = entry.get(stat_key)
        if not isinstance(stats_dict, dict): continue

        for c_hash in stats_dict.keys():
            try:
                for commit in Repository(repo_path, single=c_hash).traverse_commits():
                    c_mod, c_add, c_rem = 0, 0, 0
                    
                    for m in commit.modified_files:
                        # PyDriller diff_parsed gives a list of (line_no, line_text)
                        # We only need the line_text (index 1)
                        added_content = [line[1] for line in m.diff_parsed['added']]
                        deleted_content = [line[1] for line in m.diff_parsed['deleted']]

                        mod, add, rem = get_string_matching_metrics(added_content, deleted_content)
                        c_mod += mod
                        c_add += add
                        c_rem += rem

                    rows.append({
                        'commit_id': c_hash,
                        'lines_added': c_add,
                        'lines_removed': c_rem,
                        'lines_modified': c_mod,
                        'lines_changed': c_add + c_rem + c_mod,
                        'Dataset': 'Java Vuln (Manual)',
                        'Type': commit_type
                    })
            except Exception as e:
                print(f"Error processing {c_hash}: {e}")

df_java_vuln = pd.DataFrame(rows).drop_duplicates(subset=['commit_id', 'Type'])
df_java_vuln.to_csv(OUTPUT_PATH, index=False)