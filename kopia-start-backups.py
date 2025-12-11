#!/usr/bin/env python

"""
kopia-start-backups.py - Create Kopia backups with auto-scheduling.

Creates snapshots for all configured repositories and sources. Features:
- Automatic repository connection/creation if needed
- Policy management (retention, snapshot intervals)
- Windows Task Scheduler auto-registration (runs every 15 minutes)
- Optional full maintenance run
- Admin elevation for VSS support

Usage:
    python kopia-start-backups.py              # Run backups (auto-registers on first admin run)
    python kopia-start-backups.py --dry-run    # Show what would be done
    python kopia-start-backups.py --maintenance # Also run full maintenance
    python kopia-start-backups.py --no-elevation  # Skip admin elevation (no VSS support)
    python kopia-start-backups.py --register   # Register scheduled task only (no backup)

On first admin run, auto-registers 'KopiaHelperBackup' task to run every 15 minutes.

Requires:
    - kopia-helpers.yaml with repository definitions
    - Password configured via yaml, environment variable, or .env file
    - kopia CLI installed and in PATH
    - Admin rights for VSS and Task Scheduler
"""
import os
import sys
import logging
import argparse
import kopia_utils as utils
from typing import Dict, Any, List, Tuple

# Logging configured in main() after argument parsing

# Default (can be overridden in kopia-helpers.yaml under 'settings')
DEFAULT_BACKUP_INTERVAL_MINUTES = 15
TASK_NAME = "KopiaHelperBackup"


def preflight_checks(config: Dict[str, Any], runner: utils.KopiaRunner) -> Tuple[bool, List[str]]:
    """Perform pre-flight validation before starting backups.

    Checks:
    1. Kopia is installed and available
    2. Rclone is installed if any repo needs cloud sync
    3. All repository passwords are available
    4. Source paths exist (warnings only)

    Args:
        config: Loaded configuration dict
        runner: KopiaRunner instance (for password lookup)

    Returns:
        Tuple of (all_checks_passed, list_of_error_messages)
    """
    errors = []
    warnings = []

    # Check if any repo has sync-to destinations using rclone
    needs_rclone = False
    for repo in config.get('repositories', []):
        for dest in repo.get('sync-to', []):
            if dest.get('type') == 'rclone':
                needs_rclone = True
                break
        if needs_rclone:
            break

    # 1. Validate required tools
    tools_ok, tool_errors = utils.validate_required_tools(require_rclone=needs_rclone)
    if not tools_ok:
        errors.extend(tool_errors)

    # 2. Validate passwords for all repositories
    for repo in config.get('repositories', []):
        repo_name = repo.get('name', 'unknown')
        password = runner.get_password(repo)
        if not password:
            errors.append(f"ERROR: No password configured for repository '{repo_name}'")

    # 3. Check source paths exist (warnings, not errors)
    for repo in config.get('repositories', []):
        repo_name = repo.get('name', 'unknown')
        for source in repo.get('sources', []):
            if not os.path.exists(source):
                warnings.append(f"Warning: Source path does not exist for '{repo_name}': {source}")

    # 4. If rclone is needed, validate remotes are configured
    if needs_rclone and tools_ok:
        sync_runner = utils.KopiaSyncRunner()
        for repo in config.get('repositories', []):
            repo_name = repo.get('name', 'unknown')
            for dest in repo.get('sync-to', []):
                if dest.get('type') == 'rclone':
                    remote_path = dest.get('remote-path', '')
                    if ':' in remote_path:
                        remote_name = remote_path.split(':')[0]
                        if not sync_runner.check_rclone_remote_configured(remote_name):
                            errors.append(f"ERROR: rclone remote '{remote_name}' not configured for '{repo_name}'")
                            errors.append(f"  Run 'rclone config' to set up the '{remote_name}' remote.")

    # Print warnings (but don't fail)
    for warning in warnings:
        print(warning, file=sys.stderr)

    return len(errors) == 0, errors


def ensure_repository_connected(runner: utils.KopiaRunner, repo_config: Dict[str, Any], dry_run: bool = False) -> bool:
    """Connects to the repository if not already connected."""
    repo_path = repo_config['repo_destination']
    config_file = repo_config['repo_config']

    # Ensure config dir exists
    config_dir = os.path.dirname(config_file)
    if not os.path.exists(config_dir) and not dry_run:
        os.makedirs(config_dir, exist_ok=True)

    # Ensure repo dir exists
    if not os.path.exists(repo_path) and not dry_run:
        os.makedirs(repo_path, exist_ok=True)

    # Check status first to see if connected
    success, _, stderr, _ = runner.run(
        ["repository", "status", "--config-file", config_file],
        repo_config=repo_config, dry_run=dry_run, readonly=True
    )

    if success:
        print(f"[kopia] Connected to repository at {repo_path}")
        return True

    # Check for password error (returned by runner when repo_config password lookup fails)
    if "No password found" in stderr:
        print(f"[kopia] ERROR: {stderr}")
        return False

    # If not connected, try to connect
    success, _, stderr, _ = runner.run(
        ["repository", "connect", "filesystem",
         "--path", repo_path, "--config-file", config_file],
        repo_config=repo_config, dry_run=dry_run
    )

    if success:
        print(f"[kopia] Connected to repository at {repo_path}")
        return True

    # If connect failed, maybe it doesn't exist? Try create
    success, _, stderr, _ = runner.run(
        ["repository", "create", "filesystem",
         "--path", repo_path, "--config-file", config_file],
        repo_config=repo_config, dry_run=dry_run
    )
    if success:
        print(f"[kopia] Created new repository at {repo_path}")
    else:
        print(f"[kopia] ERROR: Failed to create repository: {stderr}")

    return success

def run_backup_job(
    runner: utils.KopiaRunner,
    repo_config: Dict[str, Any],
    scheduled: bool = False,
    dry_run: bool = False
) -> bool:
    """Runs the backup for a single repository configuration.

    Args:
        runner: KopiaRunner instance
        repo_config: Repository configuration dict
        scheduled: If True, running from Task Scheduler (quiet cloud sync)
        dry_run: If True, simulate without making changes

    Returns:
        True if backup (and cloud sync if configured) succeeded.
    """
    repo_name = repo_config['name']
    sources = repo_config.get('sources', [])
    has_cloud = utils.has_remote_destination(repo_config)

    # Visual header for repo
    print(f"\n{'='*60}")
    print(f"  {repo_name}")
    print(f"{'='*60}")
    print(f"Sources: {', '.join(sources)}")
    if has_cloud:
        sync_dests = [utils.KopiaSyncRunner.get_destination_id(d) for d in repo_config.get('sync-to', [])]
        print(f"Sync destinations: {', '.join(sync_dests)}")
    print()

    if not ensure_repository_connected(runner, repo_config, dry_run):
        print(f"[ERROR] Skipping {repo_name} due to connection failure.")
        return False

    config_file = repo_config['repo_config']
    policies = repo_config.get('policies', {})

    # Track errors per source
    snapshot_errors: List[str] = []
    policy_warnings: List[str] = []

    for source in repo_config['sources']:
        # Check if source path exists before trying to back it up
        if not os.path.exists(source):
            error_msg = f"Source path does not exist: {source}"
            logging.error(f"[kopia] {error_msg}")
            snapshot_errors.append(error_msg)
            continue

        # 1. Set Policy (Idempotent-ish, but good to ensure)
        print(f"[kopia] Setting policies for {source}...")
        policy_cmd = ["policy", "set", source, "--config-file", config_file]

        # Add retention flags
        if 'keep-annual' in policies: policy_cmd.extend(["--keep-annual", str(policies['keep-annual'])])
        if 'keep-monthly' in policies: policy_cmd.extend(["--keep-monthly", str(policies['keep-monthly'])])
        if 'keep-daily' in policies: policy_cmd.extend(["--keep-daily", str(policies['keep-daily'])])
        if 'keep-hourly' in policies: policy_cmd.extend(["--keep-hourly", str(policies['keep-hourly'])])
        if 'keep-latest' in policies: policy_cmd.extend(["--keep-latest", str(policies['keep-latest'])])
        if 'snapshot-interval' in policies: policy_cmd.extend(["--snapshot-interval", str(policies['snapshot-interval'])])

        # Run main policy command (retention, etc.)
        success, _, stderr, _ = runner.run(policy_cmd, repo_config=repo_config, dry_run=dry_run)
        if not success:
            warning = f"Policy set failed for {source}: {stderr}"
            print(f"[kopia] Warning: {warning}")
            policy_warnings.append(warning)

        # Handle ignore patterns - must be separate commands because Kopia
        # processes --clear-ignore AFTER --add-ignore when on same command line
        clear_ignore_cmd = ["policy", "set", source, "--config-file", config_file, "--clear-ignore"]
        success, _, stderr, _ = runner.run(clear_ignore_cmd, repo_config=repo_config, dry_run=dry_run)
        if not success:
            warning = f"Clear ignore failed for {source}: {stderr}"
            print(f"[kopia] Warning: {warning}")
            policy_warnings.append(warning)

        if 'ignore' in policies and policies['ignore']:
            add_ignore_cmd = ["policy", "set", source, "--config-file", config_file]
            for pattern in policies['ignore']:
                add_ignore_cmd.extend(["--add-ignore", pattern])
            success, _, stderr, _ = runner.run(add_ignore_cmd, repo_config=repo_config, dry_run=dry_run)
            if not success:
                warning = f"Add ignore patterns failed for {source}: {stderr}"
                print(f"[kopia] Warning: {warning}")
                policy_warnings.append(warning)

        # Handle dot-ignore files (e.g., .gitignore, .kopiaignore)
        # These tell kopia to read ignore patterns from files in source directories
        if 'dot-ignore' in policies and policies['dot-ignore']:
            dot_ignore_cmd = ["policy", "set", source, "--config-file", config_file]
            for filename in policies['dot-ignore']:
                dot_ignore_cmd.extend(["--add-dot-ignore", filename])
            success, _, stderr, _ = runner.run(dot_ignore_cmd, repo_config=repo_config, dry_run=dry_run)
            if not success:
                warning = f"Add dot-ignore failed for {source}: {stderr}"
                print(f"[kopia] Warning: {warning}")
                policy_warnings.append(warning)

        # 2. Create Snapshot
        print(f"[kopia] Creating snapshot for {source}...")
        snapshot_cmd = ["snapshot", "create", source, "--config-file", config_file]

        success, _, stderr, _ = runner.run(snapshot_cmd, repo_config=repo_config, dry_run=dry_run)
        if not success:
            error_msg = f"Snapshot failed for {source}: {stderr}"
            logging.error(f"[kopia] {error_msg}")
            snapshot_errors.append(error_msg)
        else:
            print(f"[kopia] Snapshot complete for {source}")

    # 3. Sync to destinations if configured (sync-to is set)
    cloud_success = True
    if has_cloud:
        cloud_success = sync_to_cloud(repo_config, runner, scheduled=scheduled, dry_run=dry_run)

    # Summarize results for this repo
    backup_success = len(snapshot_errors) == 0 and cloud_success

    if backup_success:
        if policy_warnings:
            print(f"\n[WARN] {repo_name} complete with {len(policy_warnings)} policy warning(s)")
        else:
            print(f"\n[OK] {repo_name} complete")
    else:
        print(f"\n[FAIL] {repo_name} FAILED")
        if snapshot_errors:
            print(f"  Snapshot errors:")
            for err in snapshot_errors:
                print(f"    - {err}")
        if not cloud_success:
            print(f"  Cloud sync failed")

    return backup_success


def parse_interval(interval_str: str) -> int:
    """Parse interval string (e.g., '60m', '1h', '30s') to seconds."""
    if not interval_str:
        return 3600  # default 1 hour
    interval_str = interval_str.strip().lower()
    if interval_str.endswith('h'):
        return int(interval_str[:-1]) * 3600
    elif interval_str.endswith('m'):
        return int(interval_str[:-1]) * 60
    elif interval_str.endswith('s'):
        return int(interval_str[:-1])
    else:
        return int(interval_str)  # assume seconds


def sync_to_cloud(repo_config: Dict[str, Any], runner: utils.KopiaRunner, scheduled: bool = False, dry_run: bool = False) -> bool:
    """Sync local repository to all configured destinations using kopia repository sync-to.

    Args:
        repo_config: Repository configuration dict
        runner: KopiaRunner instance (for password lookup)
        scheduled: If True, quiet mode (no progress output)
        dry_run: If True, simulate without making changes

    Returns:
        True if all syncs succeeded (or were skipped), False if any failed.
    """
    from datetime import datetime, timezone

    repo_name = repo_config['name']
    sync_destinations = repo_config.get('sync-to', [])

    if not sync_destinations:
        return True  # No destinations configured, nothing to do

    config_file = utils.get_config_file_path(repo_config)
    password = runner.get_password(repo_config)

    if not password:
        print(f"[sync] ERROR: No password for repository '{repo_name}'")
        return False

    sync_runner = utils.KopiaSyncRunner()
    all_success = True

    for dest_config in sync_destinations:
        dest_id = sync_runner.get_destination_id(dest_config)
        interval_str = dest_config.get('interval', '60m')  # default 1 hour
        interval_seconds = parse_interval(interval_str)

        # Check if we should skip based on last sync time for this destination
        dest_status = utils.get_sync_status_for_destination(repo_name, dest_id)
        if dest_status and dest_status.get('success', False):
            last_sync_str = dest_status.get('last_sync')
            if last_sync_str:
                try:
                    last_sync = datetime.fromisoformat(last_sync_str.replace('Z', '+00:00'))
                    now = datetime.now(timezone.utc)
                    elapsed = (now - last_sync).total_seconds()
                    if elapsed < interval_seconds:
                        remaining = int((interval_seconds - elapsed) / 60)
                        print(f"[sync] Skipping {dest_id} (last sync {int(elapsed/60)}m ago, next in ~{remaining}m)")
                        continue  # Skip this destination, check next
                except (ValueError, TypeError):
                    pass  # Can't parse, proceed with sync

        print(f"\n[sync] Syncing to {dest_id}...")
        success, error = sync_runner.sync(
            dest_config=dest_config,
            config_file=config_file,
            password=password,
            quiet=scheduled,
            dry_run=dry_run
        )

        # Update status file for health check
        utils.update_cloud_sync_status(
            repo_name, dest_id, success=success,
            error=error if not success else None
        )

        if success:
            print(f"[sync] Sync to {dest_id} complete.")
        else:
            print(f"[sync] ERROR syncing to {dest_id}: {error}")
            all_success = False

    return all_success


def register_backup_task(config: Dict[str, Any]) -> bool:
    """Register this script to run periodically via Windows Task Scheduler (with Admin rights)."""
    if not utils.is_admin():
        logging.error("Cannot register task - run as Administrator")
        return False

    settings = config.get('settings') or {}
    backup_interval = settings.get('backup_interval_minutes', DEFAULT_BACKUP_INTERVAL_MINUTES)
    script_path = os.path.abspath(__file__)

    return utils.register_scheduled_task(
        task_name=TASK_NAME,
        script_path=script_path,
        interval_minutes=backup_interval,
        run_elevated=True  # Needs admin for VSS
    )

def main():
    parser = argparse.ArgumentParser(description="Automated Kopia Backup Script")
    parser.add_argument("--dry-run", action="store_true", help="Simulate commands without executing")
    parser.add_argument("--no-elevation", action="store_true", help="Skip admin elevation (VSS won't work without admin)")
    parser.add_argument("--register", action="store_true", help="Register scheduled task only (no backup)")
    parser.add_argument("--scheduled", action="store_true", help="Script is being run by scheduler (skip registration checks)")
    parser.add_argument("--maintenance", action="store_true", help="Run full maintenance")
    parser.add_argument("--repo", type=str, default=None,
                        help="Comma-separated list of repo names to backup now (default: all repos)")
    parser.add_argument("--skip-preflight", action="store_true",
                        help="Skip pre-flight validation checks (not recommended)")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Set logging level (default: INFO)")
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    if not utils.is_admin():
        if args.dry_run or args.no_elevation:
            if args.no_elevation:
                logging.info("Running without elevation (--no-elevation). VSS snapshots won't work.")
            else:
                logging.warning("Running in dry-run mode without Admin privileges. VSS checks might be inaccurate.")
        else:
            logging.error("Administrator privileges required for VSS snapshots and Task Scheduler.")
            logging.error("Please re-run as Administrator, or use --no-elevation to skip VSS.")
            sys.exit(1)


    # Initialize KopiaRunner (loads config)
    runner = utils.KopiaRunner()
    config = runner.config

    # Cleanup stale status entries (repos no longer in config)
    all_repo_names = [r['name'] for r in config['repositories']]
    utils.cleanup_status_file(all_repo_names)

    # Filter repos if --repo is specified
    if args.repo:
        repo_filter = [r.strip() for r in args.repo.split(',')]
        config['repositories'] = [r for r in config['repositories'] if r['name'] in repo_filter]
        if not config['repositories']:
            logging.error(f"No matching repositories found for: {args.repo}")
            sys.exit(1)

    # Perform pre-flight checks (unless skipped, registering, or scheduled mode)
    if not args.skip_preflight and not args.register and not args.scheduled:
        preflight_ok, preflight_errors = preflight_checks(config, runner)
        if not preflight_ok:
            print("\n" + "="*60, file=sys.stderr)
            print("PRE-FLIGHT CHECKS FAILED", file=sys.stderr)
            print("="*60, file=sys.stderr)
            for error in preflight_errors:
                print(error, file=sys.stderr)
            print("\nUse --skip-preflight to bypass these checks (not recommended).", file=sys.stderr)
            sys.exit(1)

    if args.register:
        success = register_backup_task(config)
        # Show what repos are configured (even on failure, so user can verify)
        logging.info(f"Configured {len(config['repositories'])} repo(s):")
        for repo in config['repositories']:
            logging.info(f"  {repo['name']}:")
            for src in repo.get('sources', []):
                logging.info(f"    - {src}")
        if not args.maintenance:
            sys.exit(0 if success else 1)

    # Auto-register if not already registered (only when running as admin and not launched by scheduler)
    if utils.is_admin() and not args.scheduled and not utils.is_task_registered(TASK_NAME):
        logging.info("Task not registered. Auto-registering scheduled task...")
        if register_backup_task(config):
            logging.info("Auto-registration complete. Continuing with backup...")
        else:
            logging.error("Auto-registration failed. Continuing with backup anyway...")

    # Track overall success across all repos
    overall_success = True
    failed_repos: List[str] = []
    successful_repos: List[str] = []

    for repo in config['repositories']:
        repo_name = repo.get('name', 'unknown')

        # Skip backups if only --register --maintenance was requested
        if not args.register:
            backup_success = run_backup_job(runner, repo, scheduled=args.scheduled, dry_run=args.dry_run)
            if backup_success:
                successful_repos.append(repo_name)
            else:
                failed_repos.append(repo_name)
                overall_success = False

        if args.maintenance:
            logging.info(f"Running maintenance for {repo_name}...")
            maint_success, _, maint_stderr, _ = runner.run(
                ["maintenance", "run", "--full", "--config-file", repo['repo_config']],
                repo_config=repo, dry_run=args.dry_run
            )
            if not maint_success:
                logging.error(f"Maintenance failed for {repo_name}: {maint_stderr}")
                if repo_name not in failed_repos:
                    failed_repos.append(repo_name)
                overall_success = False

    # Print summary
    if not args.register:
        print(f"\n{'='*60}")
        print("BACKUP SUMMARY")
        print(f"{'='*60}")
        if successful_repos:
            print(f"Successful: {', '.join(successful_repos)}")
        if failed_repos:
            print(f"Failed: {', '.join(failed_repos)}")
        print(f"{'='*60}")

    # Exit with proper code
    sys.exit(0 if overall_success else 1)


if __name__ == "__main__":
    main()
