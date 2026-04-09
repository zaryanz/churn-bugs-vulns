import subprocess
import os

def get_context_all_metrics(commit_id, project_name, debug=False):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    repo_path = os.path.join(base_dir, 'repos', project_name)
    
    total_mods, total_adds, total_rems = 0, 0, 0
    
    try:
        files_result = subprocess.run(
            ["git", "diff", "--name-only", f"{commit_id}^", commit_id],
            cwd=repo_path, capture_output=True, text=True, check=True
        )
        files = files_result.stdout.splitlines()

        for f in files:
            # Using your successful bash command
            cmd = f"diff -c <(git show {commit_id}^:'{f}') <(git show {commit_id}:'{f}')"
            
            diff_result = subprocess.run(
                cmd, cwd=repo_path, capture_output=True, text=True, shell=True, executable='/bin/bash'
            )
            
            lines = diff_result.stdout.splitlines()
            
            # --- DEBUG LOGIC ---
            if debug:
                mod_lines = [l for l in lines if l.startswith('! ')]
                if len(mod_lines) > 0:
                    print(f"\n[DEBUG] File: {f}")
                    print(f"[DEBUG] Found {len(mod_lines)} '!' markers (Raw)")
                    # Print first 5 modified lines to see what they actually contain
                    for ml in mod_lines[:5]:
                        print(f"  > {ml}")
            # -------------------

            # Context diffs show the changed line in the 'Old' block AND the 'New' block
            # So we divide by 2 to get the actual number of lines changed.
            total_mods += sum(1 for l in lines if l.startswith('! ')) // 2
            total_adds += sum(1 for l in lines if l.startswith('+ '))
            total_rems += sum(1 for l in lines if l.startswith('- '))

        return total_mods, total_adds, total_rems

    except Exception as e:
        if debug: print(f"[DEBUG] Error: {e}")
        return 0, 0, 0