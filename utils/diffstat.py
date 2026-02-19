import subprocess

def get_diffstat_metrics(raw_diff):
    """
    Strictly parses diffstat -m output. 
    Fails loudly if the diff is invalid or the binary is missing.
    """
    # Use -m (merged logic) and -t (table/csv format)
    process = subprocess.Popen(
        ['diffstat', '-m', '-t'],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    stdout, stderr = process.communicate(input=raw_diff)
    
    # If the process returned an error code, raise an exception immediately
    if process.returncode != 0:
        raise RuntimeError(f"diffstat error: {stderr}")

    added, removed, modified = 0, 0, 0
    lines = stdout.strip().split('\n')
    
    for line in lines:
        # Skip the header row: INSERTED,DELETED,MODIFIED,FILENAME
        if line.startswith("INSERTED") or not line:
            continue
        
        parts = line.split(',')
        # Expecting at least 4 parts: INS, DEL, MOD, FILE
        added += int(parts[0])
        removed += int(parts[1])
        modified += int(parts[2])
            
    return added, removed, modified