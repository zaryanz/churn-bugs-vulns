import pandas as pd
from pathlib import Path

files = sorted(Path("data/intermediate").glob("churn_linux_chunk_*.csv"))

dfs = [pd.read_csv(f) for f in files]

combined = pd.concat(dfs, ignore_index=True)

print(f"Rows before deduplication: {len(combined)}")

combined = combined.drop_duplicates(subset=["commit_id"])

print(f"Rows after deduplication: {len(combined)}")

combined.to_csv(
    "data/intermediate/churn_linux.csv",
    index=False
)

print("Done.")