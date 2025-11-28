#!/usr/bin/env python
"""
kopia-health-check.py - Monitor Kopia backup health and view details.

Combines health monitoring (alerts) and detailed status viewing.

Modes:
1. Health Check (default):
   Checks for stale backups or recent failures. Shows Windows toast notifications.
   - --all (default): Warn only if ALL scoped repos have no recent backups
   - --any: Warn if ANY scoped repo has no recent backups

2. Detailed Status (--details / -d):
   Shows latest snapshots, file counts, sizes, and diffs for changed files.
   - Similar to the old kopia-check-backups.py

Usage:
    python kopia-health-check.py                      # Run health check (default)
    python kopia-health-check.py --details            # Show detailed snapshot info
    python kopia-health-check.py --details -n 10      # Show last 10 snapshots
    python kopia-health-check.py --details -z         # Show times in UTC
    python kopia-health-check.py --any                # Health check: Warn if any repo stale
    python kopia-health-check.py --repo mysrc         # Scope to specific repo
    python kopia-health-check.py --no-toast           # Health check: Print instead of toast
    python kopia-health-check.py --register           # Register scheduled task

Configuration (kopia-configs.yaml):
    health_check_stale_minutes: 10080        (default: 7 days)
    health_check_interval_minutes: 180       (default: 3 hours)
    health_check_mode: all|any               (default: all)
    health_check_repos: repo1,repo2          (optional)

Requires:
    - kopia-configs.yaml
    - kopia CLI in PATH
"""
import json
import subprocess
import os
import sys
import logging
import argparse
import re
from datetime import datetime, timedelta, timezone
import kopia_utils as utils
from typing import List, Dict, Any, Optional, Tuple

# Defaults (can be overridden in kopia-configs.yaml)
DEFAULT_STALE_MINUTES = 10080  # 7 days
DEFAULT_INTERVAL_MINUTES = 180  # 3 hours
TASK_NAME = "KopiaHelperHealth"

# Set from config at runtime
aggregated_dirs: List[str] = []


def parse_diff_line(line: str) -> Optional[Tuple[str, str, str]]:
    """Parse a kopia diff line. Returns (change_type, path, formatted_line) or None."""
    line = line.strip()
    if not line:
        return None

    BLANK_TS = ' ' * 25

    # Regex for "changed" lines
    # changed ./path at 2025-11-22 16:09:30.2325872 +1100 AEDT (size 11258 -> 12051)
    changed_pattern = r"^changed\s+(.+?)\s+at\s+(.+?)(?:\s+\(size\s+(.+?)\))?$"
    
    # Regex for "added file" lines
    # added file ./path (1234 bytes)
    added_pattern = r"^added file\s+(.+?)\s+\((\d+)\s+bytes\)$"
    
    # Regex for "removed file" lines
    # removed file ./path
    removed_pattern = r"^removed file\s+(.+)$"

    match_changed = re.match(changed_pattern, line)
    if match_changed:
        path = match_changed.group(1)
        ts_part = match_changed.group(2)
        size_part = match_changed.group(3) or ""
        
        ts = utils.format_kopia_verbose_timestamp(ts_part)
        size_str = size_part.split('->')[-1].strip() if '->' in size_part else size_part
        
        return ('changed', path, f"      c {ts} | {size_str:15s} | {path}")

    match_added = re.match(added_pattern, line)
    if match_added:
        path = match_added.group(1)
        size = match_added.group(2)
        return ('added', path, f"      + {BLANK_TS} | {size:15s} | {path}")

    match_removed = re.match(removed_pattern, line)
    if match_removed:
        path = match_removed.group(1)
        return ('removed', path, f"      - {BLANK_TS} | {' ':15s} | {path}")

    return None


def match_aggregated_dir(path: str) -> Optional[str]:
    """Check if path is in an aggregated directory. Returns dir name or None."""
    for d in aggregated_dirs:
        if f'/{d}/' in path or f'\\{d}\\' in path or path.startswith(f'{d}/') or path.startswith(f'{d}\\'):
            return d
    return None


def get_latest_snapshot_time(runner: utils.KopiaRunner, repo_config: Dict[str, Any]) -> Tuple[Optional[datetime], Optional[str]]:
    """Get the timestamp of the most recent snapshot for a repository."""
    config_file = repo_config['local_config_file_path']

    success, stdout, stderr, _ = runner.run(
        ["snapshot", "list", "--config-file", config_file, "--json"],
        repo_config=repo_config, readonly=True
    )

    if not success:
        return None, stderr or "Could not list snapshots"

    try:
        snapshots = json.loads(stdout)
        if not snapshots:
            return None, "No snapshots found"

        latest = snapshots[-1]
        time_str = latest.get('startTime', '')

        if not time_str:
            return None, "No timestamp in snapshot"

        dt_utc = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        return dt_utc, None

    except json.JSONDecodeError:
        return None, "Could not parse snapshot list"
    except Exception as e:
        return None, str(e)


def check_recent_snapshots(
    runner: utils.KopiaRunner, 
    config: Dict[str, Any], 
    lookback_minutes: int, 
    repo_filter: Optional[List[str]] = None
) -> Tuple[List[Dict], List[Dict], Dict]:
    """Check snapshots within the lookback window, returning all snapshots and failures."""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=lookback_minutes)
    failures = []
    all_snapshots = []
    repo_status = {}

    repos = config['repositories']
    if repo_filter:
        repos = [r for r in repos if r['name'] in repo_filter]

    for repo in repos:
        repo_name = repo.get('name', 'unknown')
        config_file = repo['local_config_file_path']
        repo_status[repo_name] = {'last_backup': None, 'error': None, 'snapshots_in_window': 0}

        success, stdout, stderr, _ = runner.run(
            ["snapshot", "list", "--config-file", config_file, "--json", "--all"],
            repo_config=repo, readonly=True
        )

        if not success:
            error_msg = stderr or "Could not list snapshots"
            repo_status[repo_name]['error'] = error_msg
            failures.append({
                'repo': repo_name,
                'time': now,
                'reason': error_msg,
                'source': 'unknown'
            })
            continue

        try:
            snapshots = json.loads(stdout) if stdout.strip() else []

            for snap in snapshots:
                time_str = snap.get('startTime', '')
                if not time_str:
                    continue

                snap_time = datetime.fromisoformat(time_str.replace('Z', '+00:00'))

                if repo_status[repo_name]['last_backup'] is None or snap_time > repo_status[repo_name]['last_backup']:
                    repo_status[repo_name]['last_backup'] = snap_time

                if snap_time < cutoff:
                    continue

                source_path = snap.get('source', {}).get('path', 'unknown')
                incomplete_reason = snap.get('incompleteReason', '')

                repo_status[repo_name]['snapshots_in_window'] += 1

                all_snapshots.append({
                    'repo': repo_name,
                    'time': snap_time,
                    'source': source_path,
                    'failed': bool(incomplete_reason),
                    'reason': incomplete_reason
                })

                if incomplete_reason:
                    failures.append({
                        'repo': repo_name,
                        'time': snap_time,
                        'reason': incomplete_reason,
                        'source': source_path
                    })

        except json.JSONDecodeError:
            repo_status[repo_name]['error'] = "Could not parse snapshot list"
            failures.append({
                'repo': repo_name,
                'time': now,
                'reason': "Could not parse snapshot list",
                'source': 'unknown'
            })
        except Exception as e:
            repo_status[repo_name]['error'] = str(e)
            failures.append({
                'repo': repo_name,
                'time': now,
                'reason': str(e),
                'source': 'unknown'
            })

    return failures, all_snapshots, repo_status


def check_all_repositories(
    runner: utils.KopiaRunner, 
    config: Dict[str, Any], 
    mode: str = 'all', 
    repo_filter: Optional[List[str]] = None
) -> Tuple[bool, Optional[datetime], List[str]]:
    """Check repository backup activity."""
    settings = config.get('settings') or {}
    stale_threshold = settings.get('health_check_stale_minutes', DEFAULT_STALE_MINUTES)

    now = datetime.now(timezone.utc)
    threshold = now - timedelta(minutes=stale_threshold)

    most_recent_backup = None
    stale_repos = []
    recent_repos = []

    repos = config['repositories']
    if repo_filter:
        repos = [r for r in repos if r['name'] in repo_filter]

    for repo in repos:
        repo_name = repo.get('name', 'unknown')
        latest_time, _ = get_latest_snapshot_time(runner, repo)

        if latest_time is not None:
            if most_recent_backup is None or latest_time > most_recent_backup:
                most_recent_backup = latest_time

            if latest_time >= threshold:
                recent_repos.append(repo_name)
            else:
                stale_repos.append(repo_name)
        else:
            stale_repos.append(repo_name)

    if mode == 'any':
        is_healthy = len(stale_repos) == 0
    else:
        is_healthy = len(recent_repos) > 0

    return is_healthy, most_recent_backup, stale_repos


def get_repository_size(runner: utils.KopiaRunner, repo_config: Dict[str, Any]) -> Optional[int]:
    """Get total repository disk size in bytes using blob stats --raw."""
    config_file = repo_config['local_config_file_path']

    success, stdout, stderr, _ = runner.run(
        ["blob", "stats", "--config-file", config_file, "--raw"],
        repo_config=repo_config, readonly=True
    )

    if not success:
        return None

    # Parse "Total: 613154858" from --raw output
    for line in stdout.splitlines():
        if line.startswith("Total:"):
            try:
                return int(line.split(":")[1].strip())
            except (ValueError, IndexError):
                return None
    return None


def format_size(size_bytes: int) -> str:
    """Format size in bytes to human readable string."""
    if size_bytes >= 1024 * 1024 * 1024:
        return f"{size_bytes / (1024*1024*1024):.2f} GB"
    elif size_bytes >= 1024 * 1024:
        return f"{size_bytes / (1024*1024):.2f} MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.2f} KB"
    else:
        return f"{size_bytes} B"


def show_detailed_status(runner: utils.KopiaRunner, repo_config: Dict[str, Any], zulu: bool, last: int, max_files: int) -> Optional[int]:
    """Show detailed snapshot history for a repository (replacing kopia-check-backups.py).

    Returns the repository disk size in bytes, or None if unavailable.
    """
    name = repo_config['name']
    config_file = repo_config['local_config_file_path']
    repo_path = repo_config['local_destination_repo']

    print(f"\n{'='*20} {name} {'='*20}")
    print(f"Local Repository: {repo_path}")
    if utils.has_remote_destination(repo_config):
        remote_dest = repo_config['remote_destination_repo']
        cloud_status = utils.load_cloud_sync_status()
        repo_cloud = cloud_status.get(name, {})

        if repo_cloud:
            if not repo_cloud.get('success', True):
                print(f"Cloud Sync: ❌ FAILED → {remote_dest}")
                print(f"            Error: {repo_cloud.get('error', 'Unknown')}")
            else:
                last_sync_str = repo_cloud.get('last_sync')
                if last_sync_str:
                    try:
                        last_sync = datetime.fromisoformat(last_sync_str.replace('Z', '+00:00'))
                        sync_time = last_sync.astimezone().strftime("%Y-%m-%d %H:%M")
                        print(f"Cloud Sync: ✓ OK [{sync_time}] → {remote_dest}")
                    except (ValueError, TypeError):
                        print(f"Cloud Sync: ✓ OK → {remote_dest}")
                else:
                    print(f"Cloud Sync: ✓ OK → {remote_dest}")
        else:
            print(f"Cloud Sync: (no sync recorded) → {remote_dest}")

    # Get and display repository disk size
    repo_size = get_repository_size(runner, repo_config)
    if repo_size is not None:
        print(f"Repository Disk Usage: {format_size(repo_size)}")
    print("Latest Snapshots:")
    
    success, stdout, stderr, _ = runner.run(
        ["snapshot", "list", "--config-file", config_file, "--json"],
        repo_config=repo_config, readonly=True
    )

    if not success:
        print(f"  ERROR: Could not list snapshots.")
        if stderr:
            print(f"  Details: {stderr.strip()}")
        print(f"\n  Troubleshooting:")
        print(f"    1. Verify config file exists: {config_file}")
        print(f"    2. Check password in kopia-configs.yaml for '{name}'")
        print(f"    3. Verify local repository path exists: {repo_path}")
        return None

    try:
        snapshots = json.loads(stdout)
        if not snapshots:
            print("  No snapshots found.")
        else:
            total_count = len(snapshots)
            print(f"  Total snapshots: {total_count} (showing last {last})")
            print()

            for snap in snapshots[-last:]:
                source = snap.get('source', {})
                path = source.get('path', 'Unknown')
                time_str = snap.get('startTime', '')
                
                stats = snap.get('stats', {})
                root_entry = snap.get('rootEntry', {})
                summ = root_entry.get('summ', {})
                
                total_size = summ.get('size', 0)
                total_files = summ.get('files', 0)
                changed_files = stats.get('fileCount', 0)
                
                time_display = utils.format_timestamp_utc(time_str) if zulu else utils.format_timestamp_local(time_str)

                print(f"  Snapshot Source: {path}")
                print(f"    Time: {time_display}")
                print(f"    Total Files: {total_files}")
                print(f"    Total Size: {total_size / (1024*1024):.2f} MB")
                print(f"    Changed Files: {changed_files}")
                
                if changed_files > 0:
                    current_id = snap.get('id')
                    try:
                        idx = snapshots.index(snap)
                        if idx > 0:
                            prev_id = snapshots[idx-1].get('id')
                            # Use runner.run(wait=False) to get process handle
                            success_diff, _, _, process = runner.run(
                                ["diff", "--no-progress", prev_id, current_id, "--config-file", config_file],
                                repo_config=repo_config,
                                readonly=True,
                                wait=False
                            )

                            if process:
                                print("    Changes:")
                                current_agg_dir = None
                                agg_counts = {'added': 0, 'changed': 0, 'removed': 0}
                                total_counts = {'added': 0, 'changed': 0, 'removed': 0}
                                regular_count = 0
                                truncated = False

                                def print_agg_summary(agg_dir, counts):
                                    """Print aggregated summary for a directory."""
                                    if sum(counts.values()) > 0:
                                        symbols = {'added': '+', 'changed': 'c', 'removed': '-'}
                                        summary = ', '.join(f"{symbols[k]}{v}" for k, v in counts.items() if v)
                                        print(f"      * {' '*25} | {' ':15s} | {agg_dir}/* ({summary})")

                                try:
                                    # Stream output line by line, printing as we go
                                    if process.stdout:
                                        for line in process.stdout:
                                            parsed = parse_diff_line(line)
                                            if not parsed:
                                                continue
                                            
                                            change_type, fpath, formatted = parsed
                                            agg_dir = match_aggregated_dir(fpath)
                                            
                                            # Track in total counts
                                            total_counts[change_type] += 1
                                            
                                            if agg_dir:
                                                # If directory changed, print previous summary
                                                if current_agg_dir and current_agg_dir != agg_dir:
                                                    print_agg_summary(current_agg_dir, agg_counts)
                                                    agg_counts = {'added': 0, 'changed': 0, 'removed': 0}
                                                
                                                current_agg_dir = agg_dir
                                                agg_counts[change_type] += 1
                                            else:
                                                # Non-aggregated file
                                                # If we had pending aggregated items, print them first
                                                if current_agg_dir:
                                                    print_agg_summary(current_agg_dir, agg_counts)
                                                    current_agg_dir = None
                                                    agg_counts = {'added': 0, 'changed': 0, 'removed': 0}
                                                
                                                if regular_count < max_files:
                                                    print(formatted)
                                                    regular_count += 1
                                                else:
                                                    truncated = True
                                                    process.terminate()
                                                    break
                                    
                                    # Clean up
                                    process.wait(timeout=1)
                                    
                                    # Print any remaining aggregated summary
                                    if current_agg_dir:
                                        print_agg_summary(current_agg_dir, agg_counts)
                                    
                                    if truncated:
                                        print(f"      ... (output truncated after {max_files} files)")
                                    elif sum(total_counts.values()) > 0:
                                        # Print total summary at the end (only if not truncated)
                                        summary = f"added {total_counts['added']}, changed {total_counts['changed']}, removed {total_counts['removed']}"
                                        print(f"    Total: {summary}")
                                        sys.stdout.flush()

                                except Exception as e:
                                    print(f"      Error reading diff output: {e}")
                    except ValueError:
                        pass
                        
                print("-" * 10)
    except json.JSONDecodeError:
        print("  Could not parse snapshot list.")
        print(stdout)

    return repo_size


def show_toast_notification(title: str, message: str):
    """Show a Windows toast notification using PowerShell."""
    ps_script = f'''
[Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] | Out-Null
[Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom.XmlDocument, ContentType = WindowsRuntime] | Out-Null

$template = @"
<toast>
    <visual>
        <binding template="ToastText02">
            <text id="1">{title}</text>
            <text id="2">{message}</text>
        </binding>
    </visual>
</toast>
"@

$xml = New-Object Windows.Data.Xml.Dom.XmlDocument
$xml.LoadXml($template)
$toast = [Windows.UI.Notifications.ToastNotification]::new($xml)
[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("Kopia Backup").Show($toast)
'''
    try:
        creation_flags = 0x08000000 if sys.platform == "win32" else 0  # CREATE_NO_WINDOW
        result = subprocess.run(
            ["powershell", "-ExecutionPolicy", "Bypass", "-Command", ps_script],
            capture_output=True,
            text=True,
            creationflags=creation_flags
        )
        if result.returncode != 0:
            print(f"Toast failed: {result.stderr}")
    except Exception as e:
        print(f"Could not show notification: {e}")
        print(f"Title: {title}")
        print(f"Message: {message}")


def check_cloud_sync_status(
    config: Dict[str, Any],
    stale_threshold_minutes: int,
    repo_filter: Optional[List[str]] = None
) -> Tuple[List[Dict], List[Dict]]:
    """Check cloud sync status for repositories with cloud destinations.

    Returns:
        Tuple of (failures, stale_syncs) where each is a list of dicts with repo info.
    """
    status = utils.load_cloud_sync_status()
    if not status:
        return [], []

    now = datetime.now(timezone.utc)
    threshold = now - timedelta(minutes=stale_threshold_minutes)

    failures = []
    stale_syncs = []

    # Get cloud repos from config (repos with remote_repository_path)
    cloud_repos = []
    for repo in config.get('repositories', []):
        if utils.has_remote_destination(repo):
            if repo_filter is None or repo['name'] in repo_filter:
                cloud_repos.append(repo['name'])

    for repo_name in cloud_repos:
        if repo_name not in status:
            # No sync status recorded - could be first run
            continue

        repo_status = status[repo_name]

        # Check for sync failures
        if not repo_status.get('success', True):
            failures.append({
                'repo': repo_name,
                'remote': repo_status.get('remote', 'unknown'),
                'error': repo_status.get('error', 'Unknown error'),
                'last_sync': repo_status.get('last_sync')
            })
        else:
            # Check for stale syncs
            last_sync_str = repo_status.get('last_sync')
            if last_sync_str:
                try:
                    last_sync = datetime.fromisoformat(last_sync_str.replace('Z', '+00:00'))
                    if last_sync < threshold:
                        stale_syncs.append({
                            'repo': repo_name,
                            'remote': repo_status.get('remote', 'unknown'),
                            'last_sync': last_sync
                        })
                except (ValueError, TypeError):
                    pass

    return failures, stale_syncs


def register_health_task(config: Dict[str, Any], mode: str = 'all', repo_filter: Optional[List[str]] = None) -> bool:
    """Register this script to run periodically via Windows Task Scheduler."""
    settings = config.get('settings') or {}
    check_interval = settings.get('health_check_interval_minutes', DEFAULT_INTERVAL_MINUTES)
    script_path = os.path.abspath(__file__)

    extra_args = []
    if mode == 'any':
        extra_args.append('--any')
    if repo_filter:
        extra_args.append(f'--repo {",".join(repo_filter)}')

    mode_desc = "warn if ANY repo stale" if mode == 'any' else "warn only if ALL repos stale"
    repo_desc = f"repos: {', '.join(repo_filter)}" if repo_filter else "all repos"
    print(f"  Mode: {mode} ({mode_desc})")
    print(f"  Scope: {repo_desc}")

    return utils.register_scheduled_task(
        task_name=TASK_NAME,
        script_path=script_path,
        interval_minutes=check_interval,
        extra_args=extra_args if extra_args else None,
        run_elevated=False
    )


def main():
    parser = argparse.ArgumentParser(
        description="Kopia Backup Health Check & Status Viewer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python kopia-health-check.py                  Run health check (default action)
  python kopia-health-check.py --details        Show detailed snapshot history
  python kopia-health-check.py --no-toast       Print result instead of toast
  python kopia-health-check.py --test           Show fake failure to test notifications
  python kopia-health-check.py --register       Register scheduled task only
"""
    )
    # Actions
    parser.add_argument("-d", "--details", action="store_true",
                        help="Show detailed snapshot history (replaces kopia-check-backups.py)")
    parser.add_argument("--check", action="store_true",
                        help="Run health check (default if no other action)")
    parser.add_argument("--test", action="store_true",
                        help="Show a fake failure notification to test toast works")
    parser.add_argument("--register", action="store_true",
                        help="Register scheduled task only")
    
    # Options for --details
    parser.add_argument("-n", "--last", type=int, default=5,
                        help="Number of recent snapshots to show (default: 5)")
    parser.add_argument("-z", "--zulu", action="store_true",
                        help="Show times in UTC (Zulu) instead of local time")
    parser.add_argument("--max-files", type=int, default=10,
                        help="Maximum number of changed files to show per snapshot (default: 10)")

    # Options for --check
    parser.add_argument("--scheduled", action="store_true",
                        help="Running from scheduler (silent unless warning)")
    parser.add_argument("--no-toast", action="store_true",
                        help="Print warnings instead of toast notification")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Show detailed status of each repo checked (during health check)")

    # Common Options
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--all", dest="mode", action="store_const", const="all",
                           help="Warn only if ALL repos are stale (default)")
    mode_group.add_argument("--any", dest="mode", action="store_const", const="any",
                           help="Warn if ANY repo is stale")
    parser.set_defaults(mode="all")

    parser.add_argument("--repo", type=str, default=None,
                        help="Comma-separated list of repo names to check (default: all repos)")
    parser.add_argument("--log-level", default="WARNING",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Set logging level (default: WARNING)")
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=args.log_level,
        format='%(message)s',
        stream=sys.stdout
    )

    # Initialize KopiaRunner
    runner = utils.KopiaRunner()
    config = runner.config

    # Get settings
    settings = config.get('settings') or {}
    mode = args.mode or settings.get('health_check_mode', 'all')

    # Load aggregate directories (strip trailing slashes for matching)
    global aggregated_dirs
    raw_aggregate = settings.get('aggregate', [])
    aggregated_dirs = [d.rstrip('/\\') for d in raw_aggregate]

    # Parse repo filter
    repo_filter = None
    if args.repo:
        repo_filter = [r.strip() for r in args.repo.split(',')]
    elif settings.get('health_check_repos'):
        repo_filter = [r.strip() for r in settings['health_check_repos'].split(',')]

    # 1. Register Task
    if args.register:
        register_health_task(config, mode=mode, repo_filter=repo_filter)
        return

    # 2. Detailed Status View
    if args.details:
        repos = config['repositories']
        if repo_filter:
            repos = [r for r in repos if r['name'] in repo_filter]

        repo_sizes = []
        for repo in repos:
            size = show_detailed_status(runner, repo, zulu=args.zulu, last=args.last, max_files=args.max_files)
            if size is not None:
                repo_sizes.append((repo['name'], size))

        # Print summary if multiple repos or at least one repo has size info
        if repo_sizes:
            print(f"\n{'='*20} Summary Disk Usage {'='*20}")
            total_size = 0
            for name, size in repo_sizes:
                print(f"  {name}: {format_size(size)}")
                total_size += size
            if len(repo_sizes) > 1:
                print(f"  {'-'*30}")
                print(f"  Total: {format_size(total_size)}")
        return

    # Parse repo filter
    repo_filter = None
    if args.repo:
        repo_filter = [r.strip() for r in args.repo.split(',')]
    elif settings.get('health_check_repos'):
        repo_filter = [r.strip() for r in settings['health_check_repos'].split(',')]

    # 1. Register Task
    if args.register:
        register_health_task(config, mode=mode, repo_filter=repo_filter)
        return

    # 2. Detailed Status View
    if args.details:
        repos = config['repositories']
        if repo_filter:
            repos = [r for r in repos if r['name'] in repo_filter]
        
        for repo in repos:
            show_detailed_status(runner, repo, zulu=args.zulu, last=args.last, max_files=args.max_files)
        return

    # 3. Test Mode
    if args.test:
        message = "test-repo: simulated failure reason"
        if args.no_toast:
            print("Kopia Backup Failed (TEST)")
            print(message)
        else:
            show_toast_notification("❌ Kopia Backup Failed (TEST)", message)
        return

    # 4. Health Check (Default)
    is_healthy, last_backup, stale_repos = check_all_repositories(runner, config, mode=mode, repo_filter=repo_filter)
    
    check_interval = settings.get('health_check_interval_minutes', DEFAULT_INTERVAL_MINUTES)
    recent_failures, all_snapshots, repo_status = check_recent_snapshots(runner, config, check_interval, repo_filter=repo_filter)

    # Handle stale repos warning
    if not is_healthy:
        if last_backup is None:
            message = "No backups found in any repository!"
        elif mode == 'any' and stale_repos:
            days_ago = (datetime.now(timezone.utc) - last_backup).days
            if len(stale_repos) == 1:
                message = f"Repo '{stale_repos[0]}' has no recent backup!"
            else:
                message = f"{len(stale_repos)} repos have no recent backup: {', '.join(stale_repos)}"
        else:
            days_ago = (datetime.now(timezone.utc) - last_backup).days
            message = f"No backup activity in {days_ago} days!"

        if args.no_toast:
            print("Kopia Backup Warning")
            print(message)
        else:
            show_toast_notification("⚠️ Kopia Backup Warning", message)

    # Handle recent failures warning
    if recent_failures:
        if len(recent_failures) == 1:
            f = recent_failures[0]
            failure_msg = f"{f['repo']}: {f['reason']}"
        else:
            repo_failures = {}
            for f in recent_failures:
                repo_name = f['repo']
                if repo_name not in repo_failures:
                    repo_failures[repo_name] = []
                repo_failures[repo_name].append(f['reason'])

            failure_parts = []
            for repo, reasons in repo_failures.items():
                unique_reasons = list(set(reasons))
                if len(unique_reasons) == 1:
                    failure_parts.append(f"{repo}: {unique_reasons[0]}")
                else:
                    failure_parts.append(f"{repo}: {len(reasons)} failures")
            failure_msg = "; ".join(failure_parts)

        if args.no_toast:
            print(f"Kopia Backup Failed ({len(recent_failures)})")
            print(failure_msg)
            for f in recent_failures:
                local_time = f['time'].astimezone().strftime("%H:%M")
                print(f"  - [{local_time}] {f['repo']}: {f['reason']} ({f['source']})")
        else:
            show_toast_notification(f"❌ Kopia Backup Failed ({len(recent_failures)})", failure_msg)

    # Check cloud sync status
    stale_threshold = settings.get('health_check_stale_minutes', DEFAULT_STALE_MINUTES)
    cloud_failures, stale_syncs = check_cloud_sync_status(config, stale_threshold, repo_filter)

    if cloud_failures:
        if len(cloud_failures) == 1:
            f = cloud_failures[0]
            sync_msg = f"{f['repo']}: {f['error']}"
        else:
            sync_msg = "; ".join(f"{f['repo']}: {f['error']}" for f in cloud_failures)

        if args.no_toast:
            print(f"Cloud Sync Failed ({len(cloud_failures)})")
            print(sync_msg)
            for f in cloud_failures:
                print(f"  - {f['repo']}: {f['error']} ({f['remote']})")
        else:
            show_toast_notification(f"❌ Cloud Sync Failed ({len(cloud_failures)})", sync_msg)

    if stale_syncs and not cloud_failures:
        # Only warn about stale syncs if there are no outright failures
        if len(stale_syncs) == 1:
            s = stale_syncs[0]
            age = datetime.now(timezone.utc) - s['last_sync']
            days = age.days
            sync_msg = f"{s['repo']}: no sync in {days} days"
        else:
            sync_msg = f"{len(stale_syncs)} repos have stale cloud syncs"

        if args.no_toast:
            print("Cloud Sync Warning")
            print(sync_msg)
        else:
            show_toast_notification("⚠️ Cloud Sync Warning", sync_msg)

    # Verbose mode
    if args.verbose:
        now = datetime.now(timezone.utc)
        cloud_status = utils.load_cloud_sync_status()

        # Build lookup for repos with cloud destinations
        cloud_repos_config = {}
        for repo in config.get('repositories', []):
            if utils.has_remote_destination(repo):
                cloud_repos_config[repo['name']] = repo.get('remote_destination_repo', '')

        print(f"Checked {len(repo_status)} repo(s) for failures in last {check_interval} min:\n")

        first_repo = True
        for repo_name, status in repo_status.items():
            # Add blank line between repos
            if not first_repo:
                print()
            first_repo = False

            if status['error']:
                print(f"  {repo_name}: ERROR ({status['error']})")
                continue

            repo_snaps = [s for s in all_snapshots if s['repo'] == repo_name]
            repo_snaps.sort(key=lambda x: x['time'], reverse=True)

            if repo_snaps:
                print(f"  {repo_name}: {len(repo_snaps)} snapshot(s) in last {check_interval} min")
                for snap in repo_snaps:
                    local_time = snap['time'].astimezone().strftime("%H:%M")
                    if snap['failed']:
                        print(f"    [{local_time}] FAILED: {snap['reason']} ({snap['source']})")
                    else:
                        print(f"    [{local_time}] OK ({snap['source']})")
            else:
                last = status['last_backup']
                if last:
                    age = now - last
                    if age.days > 0:
                        age_str = f"{age.days}d ago"
                    elif age.seconds >= 3600:
                        age_str = f"{age.seconds // 3600}h ago"
                    else:
                        age_str = f"{age.seconds // 60}m ago"
                    print(f"  {repo_name}: no snapshots in window (last backup {age_str})")
                else:
                    print(f"  {repo_name}: no snapshots found")

            # Show cloud sync status for this repo (if it has cloud destination)
            if repo_name in cloud_repos_config:
                remote_dest = cloud_repos_config[repo_name]
                repo_cloud = cloud_status.get(repo_name, {})

                if repo_cloud:
                    if not repo_cloud.get('success', True):
                        print(f"    [cloud] ❌ FAILED → {remote_dest}")
                        print(f"            Error: {repo_cloud.get('error', 'Unknown')}")
                    else:
                        last_sync_str = repo_cloud.get('last_sync')
                        if last_sync_str:
                            try:
                                last_sync = datetime.fromisoformat(last_sync_str.replace('Z', '+00:00'))
                                sync_time = last_sync.astimezone().strftime("%H:%M")
                                print(f"    [cloud] ✓ OK [{sync_time}] → {remote_dest}")
                            except (ValueError, TypeError):
                                print(f"    [cloud] ✓ OK → {remote_dest}")
                        else:
                            print(f"    [cloud] ✓ OK → {remote_dest}")
                else:
                    print(f"    [cloud] (no sync recorded) → {remote_dest}")

    elif is_healthy and not recent_failures and not cloud_failures:
        if not args.scheduled and last_backup:
            local_time = last_backup.astimezone().strftime("%Y-%m-%d %H:%M")
            print(f"OK: Last backup at {local_time}")


if __name__ == "__main__":
    main()
