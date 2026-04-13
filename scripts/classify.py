import difflib
import re
from pydriller import Repository
from urllib.parse import urlparse
import os


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def get_repo_path(repository_url, base_dir="repos"):
    repo_name = urlparse(repository_url).path.strip("/").split("/")[-1]
    return os.path.join(base_dir, repo_name)

def is_source_code(filename, language="Java"):
    extensions = {"Java": ".java"}
    return filename.endswith(extensions.get(language, ""))


def is_test_file(file_path, filename):
    return "test" in file_path.lower() or "test" in filename.lower()


def parse_line_ranges(range_str):
    """
    Parses line range strings into a set of line numbers.
    Handles: '172-177', '111', '59-62;64-65', '695;710'
    """
    line_numbers = set()
    for part in range_str.split(';'):
        part = part.strip()
        if '-' in part:
            start, end = part.split('-')
            line_numbers.update(range(int(start), int(end) + 1))
        else:
            line_numbers.add(int(part))
    return line_numbers


# ─────────────────────────────────────────────
# Diff parsing
# ─────────────────────────────────────────────

def get_hunks_from_diff(diff_text):
    """
    Splits raw diff into (header, content) tuples.
    Header is the @@ line, content is everything until the next @@ line.
    """
    hunk_split_regex = r'^(@@ -\d+(?:,\d+)? \+\d+(?:,\d+)? @@.*$)'
    parts = re.split(hunk_split_regex, diff_text, flags=re.MULTILINE)

    hunks = []
    for i in range(1, len(parts), 2):
        header = parts[i]
        content = parts[i + 1] if (i + 1) < len(parts) else ""
        hunks.append((header, content))
    return hunks


def get_lines_with_numbers(hunk_header, hunk_content):
    """
    Parses a hunk and returns two dicts:
      added:   {new_line_number: line_content}
      deleted: {old_line_number: line_content}
    """
    match = re.match(r'@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@', hunk_header)
    if not match:
        return {}, {}

    old_lineno = int(match.group(1))
    new_lineno = int(match.group(2))

    added = {}
    deleted = {}

    for line in hunk_content.splitlines():
        if line.startswith('+') and not line.startswith('+++'):
            added[new_lineno] = line[1:]
            new_lineno += 1
        elif line.startswith('-') and not line.startswith('---'):
            deleted[old_lineno] = line[1:]
            old_lineno += 1
        else:
            old_lineno += 1
            new_lineno += 1

    return added, deleted


# ─────────────────────────────────────────────
# String matching metrics
# ─────────────────────────────────────────────

def filter_with_tolerance(line_dict, target_lines, tolerance=1):
    expanded = set()
    for t in target_lines:
        expanded.update(range(t - tolerance, t + tolerance + 1))
    return {ln: c for ln, c in line_dict.items() if ln in expanded}

def get_string_matching_metrics(added_lines_text, deleted_lines_text, threshold=0.7):
    """
    Classifies line changes as modifications, pure additions, or pure deletions
    using string similarity matching.
    """
    with open("modification_audit_log.txt", "a", encoding="utf-8") as my_log:
        modified = 0

        temp_added = list(added_lines_text)
        temp_deleted = list(deleted_lines_text)

        for d_line in list(temp_deleted):
            matches = difflib.get_close_matches(d_line, temp_added, n=1, cutoff=threshold)

            if matches:
                my_log.write(f"[DEBUG] Modification Match (Score > {threshold})\n")
                my_log.write(f"  [-] {d_line.strip()}\n")
                my_log.write(f"  [+] {matches[0].strip()}\n")
                my_log.write("-" * 30 + "\n")
                modified += 1
                temp_deleted.remove(d_line)
                temp_added.remove(matches[0])

        pure_added = len(temp_added)
        pure_deleted = len(temp_deleted)

        return modified, pure_added, pure_deleted


# ─────────────────────────────────────────────
# Core: classify specific lines in a commit
# ─────────────────────────────────────────────

def classify_commit_lines(repo_path_str, commit_id, target_files_lines):
    """
    Classifies specific lines in a commit's diff.

    :param repo_path_str: Path to the local git repository
    :param commit_id: The commit hash to analyse
    :param target_files_lines: Dict mapping filename fragments to line range strings
                               e.g. {"GitServlet.java": "172-177"}
    :return: Dict mapping filename to {"modified": n, "added": n, "deleted": n}
    """
    results = {}

    try:
        for c in Repository(repo_path_str, single=commit_id).traverse_commits():
            for m in c.modified_files:
                file_path = m.new_path if m.new_path else (m.old_path if m.old_path else "")

                if not is_source_code(m.filename, "Java"):
                    continue

                if is_test_file(file_path, m.filename):
                    continue

                # Match this file against target_files_lines keys
                matched_key = None
                for key in target_files_lines:
                    if key in file_path or key in m.filename:
                        matched_key = key
                        break

                if matched_key is None:
                    continue

                target_lines = parse_line_ranges(target_files_lines[matched_key])

                total_mod, total_add, total_del = 0, 0, 0

                for header, content in get_hunks_from_diff(m.diff):
                    added, deleted = get_lines_with_numbers(header, content)

                    filtered_added = filter_with_tolerance(added, target_lines)
                    filtered_deleted = filter_with_tolerance(deleted, target_lines)

                    # ADD DEBUG HERE
                    print(f"[DEBUG] file: {matched_key}")
                    print(f"[DEBUG] target lines: {target_lines}")
                    print(f"[DEBUG] added line numbers in hunk: {set(added.keys())}")
                    print(f"[DEBUG] deleted line numbers in hunk: {set(deleted.keys())}")
                    print(f"[DEBUG] filtered added: {set(filtered_added.keys())}")
                    print(f"[DEBUG] filtered deleted: {set(filtered_deleted.keys())}")
                    print("-" * 40)

                    if not filtered_added and not filtered_deleted:
                        continue

                    mod, add, rem = get_string_matching_metrics(
                        list(filtered_added.values()),
                        list(filtered_deleted.values())
                    )

                    total_mod += mod
                    total_add += add
                    total_del += rem

                results[matched_key] = {
                    "modified": total_mod,
                    "added": total_add,
                    "deleted": total_del
                }

    except Exception as e:
        print(f"[ERROR] commit {commit_id}: {e}")

    return results


# ─────────────────────────────────────────────
# Entry point: process a dataset entry
# ─────────────────────────────────────────────

def classify_entry(entry):
    """
    Processes a single JSON dataset entry and returns classification
    results for both its introducing and fixing commits.

    :param repo_path_str: Path to the local git repository
    :param entry: A dict matching the dataset JSON schema
    :return: {
        "introducing": {commit_id: {file: {modified, added, deleted}}},
        "fixing":      {commit_id: {file: {modified, added, deleted}}}
    }
    """
    repo_path_str = get_repo_path(entry["repository"])
    output = {"introducing": {}, "fixing": {}}

    # Classify introducing commit
    intro_commit = entry.get("introducing")
    intro_lines = entry.get("introducing_lines", {})
    if intro_commit and intro_lines:
        output["introducing"][intro_commit] = classify_commit_lines(
            repo_path_str, intro_commit, intro_lines
        )

    # Classify fixing commits (can be multiple)
    fixing_commits = entry.get("fixing", [])
    fixing_lines = entry.get("fixing_lines", {})
    for fix_commit in fixing_commits:
        if fixing_lines:
            output["fixing"][fix_commit] = classify_commit_lines(
                repo_path_str, fix_commit, fixing_lines
            )

    return output

if __name__ == "__main__":
    import json

    with open("data/raw/tool_assisted_manual_dataset.json", "r", encoding="utf-8") as f:
        dataset = json.load(f)

    all_results = {}

    for entry in dataset:
        cve = entry.get("cve", "unknown")
        print(f"[INFO] Processing {cve}...")
        try:
            results = classify_entry(entry)
            all_results[cve] = results
        except Exception as e:
            print(f"[ERROR] Failed on {cve}: {e}")
            all_results[cve] = {"error": str(e)}

    with open("data/processed/classification_results.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)

    print("[DONE] Results saved to data/processed/classification_results.json")