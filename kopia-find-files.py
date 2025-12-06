#!/usr/bin/env python
"""
kopia-find-files.py - Search for files across Kopia backup snapshots.

Searches for files by filename pattern across all configured repositories and
snapshots. Uses 'kopia ls -l -r' to get file metadata directly (no mounting).

Features:
- Filename pattern matching using fnmatch (like 'find -name')
- Searches across multiple repositories and snapshots
- Shows file versions with modification times and sizes
- Interactive mount option to browse/restore files
- Configurable snapshot search limit (default: 100 most recent)

Pattern Syntax (fnmatch wildcards, like 'find -name'):
    *        Matches any characters (zero or more), but not /
    ?        Matches exactly one character
    [abc]    Matches one character from the set (a, b, or c)
    [a-z]    Matches one character in the range

Matching modes:
    Default     Matches filename only (like find -name)
    --path      Matches full path (like find -path)

Usage:
    python kopia-find-files.py "*.py"                     # Find all .py files (not .pyc)
    python kopia-find-files.py "report*.pdf"              # Find PDFs starting with 'report'
    python kopia-find-files.py "kopia/*.py" --path        # .py files in kopia/ directory
    python kopia-find-files.py "*.pdf" --repo mysrc,docs  # Search specific repos
    python kopia-find-files.py --mount Z: --repo myrepo   # Mount for browsing

Requires:
    - kopia-helpers.yaml with repository definitions
    - Password configured via yaml, environment variable, or .env file
    - kopia CLI installed and in PATH
    - WinFsp for mount functionality (Windows)
"""
import json
import subprocess
import os
import sys
import logging
import argparse
import time
import re
import fnmatch
from datetime import datetime
from pathlib import Path
import kopia_utils as utils
from typing import List, Dict, Any, Optional, Tuple

# Logging configured in main() after argument parsing

# Exit codes
EXIT_SUCCESS = 0
EXIT_TOOL_ERROR = 1
EXIT_CONFIG_ERROR = 2
EXIT_NO_RESULTS = 3


def parse_ls_line(line: str) -> Optional[Tuple[int, str, str]]:
    """Parse a kopia ls -l --no-human-readable line.

    Format: <perms> <size> <date> <time> <tz> <hash> <path>
    Example: -rw-rw-rw-   24155 2025-11-22 21:39:58 AEDT 749004d41187bf2322037fecbb5022af   kopia/file.py

    Returns (size, modified_str, path) or None if parse fails or is a directory.
    """
    # Regex to parse the line
    # Group 1: perms
    # Group 2: size
    # Group 3: date
    # Group 4: time
    # Group 5: tz
    # Group 6: hash
    # Group 7: path
    pattern = r"^([d-][rwx-]{9})\s+(\d+)\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s+(\w+)\s+([a-f0-9]+)\s+(.+)$"
    match = re.match(pattern, line.strip())

    if not match:
        return None

    perms = match.group(1)
    if perms.startswith('d'):
        return None

    try:
        size = int(match.group(2))
        date = match.group(3)
        time_str = match.group(4)
        tz = match.group(5)
        path = match.group(7)

        modified_str = f"{date} {time_str} {tz}"
        return (size, modified_str, path)
    except (ValueError, IndexError):
        return None


def find_in_repo(runner: utils.KopiaRunner, repo_config: Dict[str, Any], filename_pattern: str, max_snapshots: Optional[int] = None, match_path: bool = False, verbose: bool = False) -> List[Dict[str, Any]]:
    """Search for files using kopia ls -l -r (gets file details directly, no mounting)."""
    name = repo_config['name']
    config_file = repo_config['repo_config']

    print(f"  Searching in {name}...")

    # List snapshots
    success, stdout, stderr, _ = runner.run(
        ["snapshot", "list", "--json", "--config-file", config_file],
        repo_config=repo_config, readonly=True
    )

    matches = []

    if not success:
        print(f"    ERROR: Could not list snapshots. {stderr.strip()}")
        return []

    try:
        snapshots = json.loads(stdout)
    except json.JSONDecodeError:
        print(f"    Failed to parse snapshot list JSON.")
        if verbose:
            print(f"    Stdout: {stdout}")
            print(f"    Stderr: {stderr}")
        return []

    if not snapshots:
        print("    No snapshots found in this repository.")
        return []

    # Sort snapshots by startTime descending (newest first)
    snapshots.sort(key=lambda s: s.get('startTime', ''), reverse=True)

    # Limit to max_snapshots (None means all)
    if max_snapshots is not None:
        snapshots = snapshots[:max_snapshots]
        logging.debug(f"Limiting search to {len(snapshots)} most recent snapshots")

    for snap in snapshots:
        snap_id = snap.get('id')
        snap_time = snap.get('startTime', '')  # e.g., "2025-11-22T05:08:53Z"

        if not snap_id: continue

        # Get file listing with details (size, mtime) - no mounting needed!
        success, ls_out, _, _ = runner.run(
            ["ls", "-l", "--no-human-readable", "-r", snap_id, "--config-file", config_file],
            repo_config=repo_config, readonly=True
        )
        if success:
            for line in ls_out.splitlines():
                line = line.strip()
                if not line: continue

                parsed = parse_ls_line(line)
                if not parsed:
                    continue

                size, modified_str, file_path = parsed

                # Match against filename or full path based on --path flag
                if match_path:
                    match_against = file_path
                else:
                    match_against = os.path.basename(file_path)

                if fnmatch.fnmatch(match_against, filename_pattern):
                    if verbose:
                        print(f"    Match: {file_path}")

                    # Parse the timestamp
                    try:
                        # Format: "2025-11-22 21:39:58 AEDT"
                        # Remove timezone name for parsing, keep date and time
                        dt_parts = modified_str.split()
                        modified = datetime.strptime(f"{dt_parts[0]} {dt_parts[1]}", "%Y-%m-%d %H:%M:%S")
                    except (ValueError, IndexError):
                        modified = None

                    matches.append({
                        'repo': name,
                        'snapshot_id': snap_id,
                        'snapshot_time': snap_time,
                        'path': file_path,
                        'size': size,
                        'modified': modified,
                        'modified_str': modified_str,
                    })

    return matches


def mount_repository(runner: utils.KopiaRunner, repo_config: Dict[str, Any], drive_letter: str, verbose: bool = False):
    name = repo_config['name']
    config_file = repo_config['repo_config']

    print(f"\nPreparing to mount '{name}' to {drive_letter}...")

    # Verify repository is accessible
    success, _, stderr, _ = runner.run(
        ["snapshot", "list", "--json", "--config-file", config_file],
        repo_config=repo_config, readonly=True
    )
    if not success:
        print(f"  ERROR: Could not access repository. {stderr.strip()}")
        return

    # Mount 'all' snapshots
    print(f"Mounting all snapshots to {drive_letter}...")
    print("Press Ctrl+C to unmount and exit.")

    success, _, err, process = runner.run(
        ["mount", "all", drive_letter, "--config-file", config_file],
        repo_config=repo_config, wait=False
    )

    if not success or process is None:
        print(f"Failed to start mount process: {err}")
        return

    try:
        # Wait for user to interrupt
        while True:
            time.sleep(1)
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                print(f"Mount process exited unexpectedly.")
                if verbose:
                    print(f"Stdout: {stdout}")
                    print(f"Stderr: {stderr}")
                break
    except KeyboardInterrupt:
        print("\nUnmounting...")
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
        print("Done.")


def find_free_drive_letter() -> Optional[str]:
    """Find an available drive letter for mounting (searches Z: down to E:)."""
    import string

    # Get drives in use from net use (includes stale WebDAV mounts)
    mapped_drives = set()
    try:
        result = subprocess.run(['net', 'use'], capture_output=True, text=True, encoding='utf-8')
        for line in result.stdout.splitlines():
            parts = line.split()
            for part in parts:
                if len(part) == 2 and part[1] == ':' and part[0].isalpha():
                    mapped_drives.add(part[0].upper())
    except Exception:
        pass

    # Get drives that are actually accessible via filesystem
    accessible_drives = set()
    for letter in string.ascii_uppercase:
        if os.path.exists(f"{letter}:\\"):
            accessible_drives.add(letter)

    # A drive is "in use" if it's EITHER mapped OR accessible
    used_drives = mapped_drives | accessible_drives

    # Find first free drive letter (Z: down to E:)
    for letter in string.ascii_uppercase[::-1]:
        if letter in 'ABCD':
            continue
        if letter not in used_drives:
            return f"{letter}:"

    return None


def list_repositories(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    print("\nAvailable Repositories:")
    for i, repo in enumerate(config['repositories']):
        print(f"[{i}] {repo['name']} ({repo['repo_destination']})")
    return config['repositories']


def main():
    parser = argparse.ArgumentParser(description="Find files or Mount Kopia backups")
    parser.add_argument("pattern", nargs='?', help="Filename pattern (fnmatch: *.py, report*, [a-z]*.txt)")
    parser.add_argument("--mount", nargs='?', const='auto', default=None,
                        help="Mount repository (optionally specify drive letter, e.g. --mount Z:)")
    parser.add_argument("--repo", help="Comma-separated list of repo names to search (default: all repos)")
    parser.add_argument("--restore", help="Restore selected file to this directory (default: Downloads)", const=str(Path.home() / "Downloads"), nargs='?')
    parser.add_argument("-n", "--last", type=int, default=100,
                        help="Number of recent snapshots to search (default: 100)")
    parser.add_argument("--all", action="store_true", dest="search_all",
                        help="Search all snapshots (no limit)")
    parser.add_argument("--path", action="store_true", help="Match against full path (default: filename only)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show executed commands")
    parser.add_argument("--log-level", default="WARNING",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Set logging level (default: WARNING)")
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    # Pre-flight check: ensure kopia is available
    kopia_ok, kopia_result = utils.check_tool_available(utils.KOPIA_EXE)
    if not kopia_ok:
        print(f"ERROR: {kopia_result}", file=sys.stderr)
        print(utils.get_kopia_install_instructions(), file=sys.stderr)
        sys.exit(EXIT_TOOL_ERROR)

    # Initialize KopiaRunner
    try:
        runner = utils.KopiaRunner()
        config = runner.config
    except SystemExit:
        sys.exit(EXIT_CONFIG_ERROR)

    repos = config['repositories']

    # Determine snapshot limit
    max_snapshots = None if args.search_all else args.last

    # Filter repos if --repo is specified
    if args.repo:
        repo_filter = [r.strip() for r in args.repo.split(',')]
        repos = [r for r in repos if r['name'] in repo_filter]
        if not repos:
            print(f"No matching repositories found for: {args.repo}")
            return

    if args.mount and not args.pattern:
        # Mount-only Mode (no search)
        selected_repo = None
        if len(repos) == 1:
            selected_repo = repos[0]
        else:
            # Interactive selection for mount
            print("Multiple repositories found. Please select one to mount:")
            repos_list = list_repositories(config)
            try:
                choice = int(input("\nSelect repository number: "))
                if 0 <= choice < len(repos_list):
                    selected_repo = repos_list[choice]
                else:
                    print("Invalid selection.")
                    return
            except ValueError:
                print("Invalid input.")
                return

        # Get drive letter (auto-find or user-specified)
        if args.mount == 'auto':
            drive = find_free_drive_letter()
            if not drive:
                print("Error: No available drive letters (E-Z all in use)")
                return
        else:
            drive = args.mount
            if not drive.endswith(":"):
                drive += ":"

        mount_repository(runner, selected_repo, drive, verbose=args.verbose)
        return

    # Search Mode
    if not args.pattern:
        parser.print_help()
        print("\nError: You must provide a filename pattern to search, or use --mount.")
        return

    # Step 1: Fast search using kopia ls -r (no mounting)
    print(f"\nSearching for files matching: {args.pattern}")

    all_matches = []
    header_printed = False

    for repo in repos:
        matches = find_in_repo(runner, repo, args.pattern, max_snapshots=max_snapshots, match_path=args.path, verbose=args.verbose)

        if matches:
            # Print header on first results
            if not header_printed:
                print(f"\n{'='*115}")
                print(f"{'#':<4} {'Modified':<26} {'Size (KB)':>14} {'Repo':<12} {'Path'}")
                print(f"{'-'*115}")
                header_printed = True

            # Sort this repo's matches by modification time (newest first)
            matches.sort(key=lambda x: x['modified'] or datetime.min, reverse=True)

            # Print results immediately
            for m in matches:
                idx = len(all_matches)
                size_kb = m['size'] / 1024
                print(f"{idx:<4} {m['modified_str']:<26} {size_kb:>14.3f} {m['repo']:<12} {m['path']}")
                all_matches.append(m)

    if not all_matches:
        print("\nNo matching files found.")
        return

    print(f"{'='*115}")
    print(f"Found {len(all_matches)} version(s)")

    # Skip mount prompt unless --mount flag provided
    if not args.mount:
        return

    # Find unique repositories from matches
    unique_repos = {}
    for m in all_matches:
        repo_name = m['repo']
        if repo_name not in unique_repos:
            for repo in repos:
                if repo['name'] == repo_name:
                    unique_repos[repo_name] = repo
                    break

    # Get drive letter (auto-find or user-specified)
    if args.mount == 'auto':
        drive = find_free_drive_letter()
        if not drive:
            print("Error: No available drive letters (E-Z all in use)")
            return
    else:
        drive = args.mount
        if not drive.endswith(":"):
            drive += ":"

    # Offer to mount for browsing
    try:
        if len(unique_repos) == 1:
            repo_to_mount = list(unique_repos.values())[0]
            confirm = input(f"\nMount '{repo_to_mount['name']}' to {drive}? (y/N): ").strip().lower()
            if confirm == 'y':
                mount_repository(runner, repo_to_mount, drive, verbose=args.verbose)
        else:
            # Multiple repos, let user choose
            print(f"\nRepositories with matches (will mount to {drive}):")
            repo_list = list(unique_repos.items())
            for i, (name, repo) in enumerate(repo_list):
                count = sum(1 for m in all_matches if m['repo'] == name)
                print(f"  [{i}] {name} ({count} version(s))")

            choice = input("\nSelect repository number to mount (or Enter to skip): ").strip()
            if choice:
                repo_choice = int(choice)
                if 0 <= repo_choice < len(repo_list):
                    repo_to_mount = repo_list[repo_choice][1]
                    mount_repository(runner, repo_to_mount, drive, verbose=args.verbose)
    except (ValueError, KeyboardInterrupt):
        print("\nExiting.")


if __name__ == "__main__":
    main()
