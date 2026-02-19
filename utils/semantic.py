import difflib
import re

def get_hunks_from_diff(diff_text):
    """Splits raw diff into hunk contents using headers."""
    hunk_split_regex = r'^@@ -\d+(?:,\d+)? \+\d+(?:,\d+)? @@.*$'
    parts = re.split(f'({hunk_split_regex})', diff_text, flags=re.MULTILINE)
    
    hunks = []
    # parts[0] is file header; we start at index 1 for actual hunks
    for i in range(1, len(parts), 2):
        content = parts[i+1] if (i+1) < len(parts) else ""
        hunks.append(content)
    return hunks

def get_string_matching_metrics(added_lines_text, deleted_lines_text, threshold=0.7):
    """
    Calculates metrics by comparing the actual strings of lines.
    :param added_lines_text: List of strings (added lines)
    :param deleted_lines_text: List of strings (deleted lines)
    :param threshold: Similarity ratio (0.0 to 1.0) to consider lines a 'match'
    """
    modified = 0
    
    # We copy the lists so we can 'consume' lines as they are matched
    temp_added = list(added_lines_text)
    temp_deleted = list(deleted_lines_text)
    
    for d_line in list(temp_deleted):
        # Find the best match in the remaining added lines
        matches = difflib.get_close_matches(d_line, temp_added, n=1, cutoff=threshold)
        
        if matches:
            modified += 1
            temp_deleted.remove(d_line)
            temp_added.remove(matches[0])
            
    # Remaining lines are 'pure' additions or deletions
    pure_added = len(temp_added)
    pure_deleted = len(temp_deleted)
    
    return modified, pure_added, pure_deleted