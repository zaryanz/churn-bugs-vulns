import pandas as pd
import json

from utils.git_utils import get_diffstat_metrics

with open('data/raw/tool_assisted_manual_dataset.json', 'r') as f:
    data = json.load(f)

output_path = 'data/intermediate/java_vuln_manual.csv'

rows = []

for entry in data:
    repo = entry.get('repository')
    cve = entry.get('cve')
    
    # --- 1. Extract Fixing Commits ---
    fixing_stats = entry.get('fixing_stats')
    if isinstance(fixing_stats, dict):
        for f_hash, stats in fixing_stats.items():
            raw_add = stats.get('add', 0)
            raw_del = stats.get('del', 0)

            # Apply Greedy Diffstat Logic
            mod, added, removed = get_diffstat_metrics(raw_add, raw_del)

            if (added + removed + mod) > 0:
                rows.append({
                    'commit_id': f_hash,
                    'lines_added': added,
                    'lines_removed': removed,
                    'lines_modified': mod,  # New Column
                    'Dataset': 'Java Vuln (Manual)',
                    'Type': 'VFC'
                })

    # --- 2. Extract Introducing Commits (VCCs) ---
    intro_stats = entry.get('intro_stats')
    if isinstance(intro_stats, dict):
        for i_hash, stats in intro_stats.items():
            raw_add = stats.get('add', 0)
            raw_del = stats.get('del', 0)

            # Apply Greedy Diffstat Logic
            mod, added, removed = get_diffstat_metrics(raw_add, raw_del)

            if (added + removed + mod) > 0:
                rows.append({
                    'commit_id': i_hash,
                    'lines_added': added,
                    'lines_removed': removed,
                    'lines_modified': mod,  # New Column
                    'Dataset': 'Java Vuln (Manual)',
                    'Type': 'VIC'
                })

# Create DataFrame
df_java_vuln = pd.DataFrame(rows)
df_java_vuln = df_java_vuln.drop_duplicates(subset=['commit_id', 'Type'])

# Export
df_java_vuln.to_csv(output_path, index=False)

print(f"Successfully exported {len(df_java_vuln)} Java fixing commits to: {output_path}")

print("\nPreview of exported data:")
print(df_java_vuln.head())