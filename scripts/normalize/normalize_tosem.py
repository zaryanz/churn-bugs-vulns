import json
import pandas as pd

with open('data/raw/tool_assisted_manual_dataset.json', 'r') as f:
    data = json.load(f)

rows = []

for entry in data:
    repo_url = entry.get('repository')
    # Extract project name from URL (e.g., 'hawtio' from 'https://github.com/hawtio/hawtio')
    project_name = repo_url.split('/')[-1].replace('.git', '')
    
    # --- Process Fixing (VFC) ---
    fixing = entry.get('fixing', [])
    if isinstance(fixing, list):
        for f_hash in fixing:
            rows.append({
                'commit_id': f_hash,
                'repo_url': repo_url,
                'project': project_name,
                'label_type': 'vulnerability', # More specific than 'bug'
                'commit_role': 'VFC',          # Standardized role
                'language': 'java',            # This dataset is Java-specific
                'dataset_source': 'TOSEM'      # Source identification
            })
            
    # --- Process Introducing (VIC) ---
    intro = entry.get('introducing')
    if intro:
        rows.append({
            'commit_id': intro,
            'repo_url': repo_url,
            'project': project_name,
            'label_type': 'vulnerability',
            'commit_role': 'VIC',
            'language': 'java',
            'dataset_source': 'TOSEM'
        })

# Create DataFrame
df_tosem = pd.DataFrame(rows)

# Drop duplicates: Multiple CVEs might be fixed by the same commit
df_tosem = df_tosem.drop_duplicates(subset=['commit_id', 'commit_role'])

# Export
output_path = 'data/intermediate/commits_tosem.csv'
df_tosem.to_csv(output_path, index=False)

print(f"Normalized {len(df_tosem)} entries with columns: {list(df_tosem.columns)}")