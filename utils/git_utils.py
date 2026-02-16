import difflib

def get_diffstat_metrics(added: int, deleted: int):
    """
    Implements the greedy overlap logic from diffstat.c.
    Pairs +/- lines as 'modifications' and treats the remainder as pure churn.
    """
    # Find the 'common' overlap
    modified = min(added, deleted)
    
    # Calculate residuals after pairing modifications
    pure_additions = added - modified
    pure_deletions = deleted - modified
    
    return modified, pure_additions, pure_deletions

def get_string_matching_metrics(added_lines_text, deleted_lines_text, threshold=0.6):
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