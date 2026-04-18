#!/usr/bin/python3
# git_stats.py: A git commit to hours tracker
#  Invocation: git_stats.py <dir1> <dir2> <dir3> <dir4>
#  Example: git_stats.py portfolio react_portfolio/ portfolio-infra/ crawl_portfolio/
#  Emits: CSV of Date, contributiom, person, class, value.
import argparse
import subprocess
import datetime
import csv
import os
import sys
from collections import defaultdict

# Extensions considered "source code" for difficulty scoring
SOURCE_EXTENSIONS = {'.go', '.sql', '.ts', '.py', '.js', '.sh', '.java', '.c', '.cpp', '.h', '.rs', '.rb', '.php'}

def score_commit(changed_files, commit_message, time_since_prev_seconds):
    """
    Calculates the 'hours' score for a single commit.

    Difficulty factors:
    - Presence of source code (.py, .go, .sh, etc.)
    - Amount of churn (lines added/deleted) in source files only.
    - Follow-up logic: Commits within 1 hour add to the previous work block instead of starting a new one.
    """
    # Filter for source code changes only
    source_changes = [f for f in changed_files if os.path.splitext(f['path'])[1].lower() in SOURCE_EXTENSIONS]

    has_source = len(source_changes) > 0
    total_source_churn = sum(f['additions'] + f['deletions'] for f in source_changes)

    # Base Hour Calculation
    if time_since_prev_seconds is not None and time_since_prev_seconds < 3600:
        # Quick follow-up: adds 0.25 hours (15 mins) to the current "effort"
        base_hours = 0.25
    else:
        # New work session: starts with a base of 1 hour
        base_hours = 1.0

    # Difficulty Multiplier (Source Code Only)
    # We ignore .csv, .json, .md etc. for "difficulty" scaling
    multiplier = 1.0
    if has_source:
        multiplier += 0.5  # Bonus for touching code

        # Scale by churn in source files
        if total_source_churn > 100:
            multiplier += 0.5
        if total_source_churn > 500:
            multiplier += 1.0

    # Final Score
    score = base_hours * multiplier

    # Cap individual commit score to avoid outliers (e.g. 8 hours max for one commit)
    return min(score, 8.0)

def get_friday_end_of_week(dt):
    """Returns the ISO date of the Friday for the week containing the given datetime."""
    # weekday(): Monday=0, Friday=4, Sunday=6.
    days_to_friday = (4 - dt.weekday())
    friday = dt + datetime.timedelta(days=days_to_friday)
    return friday.date().isoformat()

def get_git_logs(repo_path):
    """Executes git log with numstat to get churn and file paths."""
    # %H: hash, %ae: author email, %at: timestamp, %s: subject
    cmd = ["git", "log", "--pretty=format:COMMIT|%ae|%at|%s", "--numstat"]
    try:
        result = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True, check=True)
    except Exception as e:
        print(f"Error reading {repo_path}: {e}", file=sys.stderr)
        return []

    commits = []
    current_commit = None

    for line in result.stdout.splitlines():
        if line.startswith("COMMIT|"):
            if current_commit:
                commits.append(current_commit)
            parts = line.split('|')
            current_commit = {
                'author': parts[1],
                'timestamp': int(parts[2]),
                'subject': parts[3] if len(parts) > 3 else "",
                'files': []
            }
        elif line.strip():
            parts = line.split()
            if len(parts) == 3:
                # numstat format: additions deletions path
                try:
                    current_commit['files'].append({
                        'additions': int(parts[0]) if parts[0] != '-' else 0,
                        'deletions': int(parts[1]) if parts[1] != '-' else 0,
                        'path': parts[2]
                    })
                except ValueError:
                    continue

    if current_commit:
        commits.append(current_commit)

    return commits

def main():
    parser = argparse.ArgumentParser(description="Git commit hour tracker.")
    parser.add_argument("directories", nargs="+", help="Directories to scan")
    args = parser.parse_args()

    author_commits = defaultdict(list)

    for d in args.directories:
        if os.path.isdir(d):
            commits = get_git_logs(d)
            for c in commits:
                author_commits[c['author']].append(c)

    # Output structure: "Date", "Contribution", "Class", "Person", "Value"
    # Mapping to: Friday, "git-commits", "Product hours", author, score

    writer = csv.writer(sys.stdout)
    writer.writerow(["Date", "Contribution", "Class", "Person", "Value"])

    weekly_stats = defaultdict(lambda: defaultdict(float))

    for author, commits in author_commits.items():
        # Sort chronologically to detect follow-ups
        commits.sort(key=lambda x: x['timestamp'])

        prev_ts = None
        for c in commits:
            dt = datetime.datetime.fromtimestamp(c['timestamp'])
            friday = get_friday_end_of_week(dt)

            time_diff = None
            if prev_ts is not None:
                # Only consider it a follow-up if on the same day
                if dt.date() == datetime.datetime.fromtimestamp(prev_ts).date():
                    time_diff = c['timestamp'] - prev_ts

            score = score_commit(c['files'], c['subject'], time_diff)
            weekly_stats[friday][author] += score
            prev_ts = c['timestamp']

    for friday in sorted(weekly_stats.keys()):
        for author in sorted(weekly_stats[friday].keys()):
            writer.writerow([
                friday,
                "git-commits",
                "Product hours",
                author,
                round(weekly_stats[friday][author], 2)
            ])
if __name__ == "__main__":
    main()
