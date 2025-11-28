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
    - kopia-configs.yaml with repository definitions
    - Password configured via yaml, environment variable, or .env file
    - kopia CLI installed and in PATH
    - Admin rights for VSS and Task Scheduler
"""
import os
import sys
import logging
import argparse
import kopia_utils as utils
from typing import Dict, Any

# Logging configured in main() after argument parsing

# Default (can be overridden in kopia-configs.yaml under 'settings')
DEFAULT_BACKUP_INTERVAL_MINUTES = 15
TASK_NAME = "KopiaHelperBackup"


def ensure_repository_connected(runner: utils.KopiaRunner, repo_config: Dict[str, Any], dry_run: bool = False) -> bool:
    """Connects to the repository if not already connected."""
    repo_path = repo_config['local_destination_repo']
    config_file = repo_config['local_config_file_path']

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
        print(f"Cloud sync: {repo_config['remote_destination_repo']}")
    print()

    if not ensure_repository_connected(runner, repo_config, dry_run):
        print(f"Skipping {repo_name} due to connection failure.")
        return False

    config_file = repo_config['local_config_file_path']
    policies = repo_config.get('policies', {})
    backup_success = True

    for source in repo_config['sources']:
        # 1. Set Policy (Idempotent-ish, but good to ensure)
        print(f"[kopia] Setting policies...")
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
            print(f"[kopia] Warning: Policy set failed: {stderr}")

        # Handle ignore patterns - must be separate commands because Kopia
        # processes --clear-ignore AFTER --add-ignore when on same command line
        clear_ignore_cmd = ["policy", "set", source, "--config-file", config_file, "--clear-ignore"]
        success, _, stderr, _ = runner.run(clear_ignore_cmd, repo_config=repo_config, dry_run=dry_run)
        if not success:
            print(f"[kopia] Warning: Clear ignore failed: {stderr}")

        if 'ignore' in policies and policies['ignore']:
            add_ignore_cmd = ["policy", "set", source, "--config-file", config_file]
            for pattern in policies['ignore']:
                add_ignore_cmd.extend(["--add-ignore", pattern])
            success, _, stderr, _ = runner.run(add_ignore_cmd, repo_config=repo_config, dry_run=dry_run)
            if not success:
                print(f"[kopia] Warning: Add ignore patterns failed: {stderr}")

        # 2. Create Snapshot
        print(f"[kopia] Creating snapshot...")
        snapshot_cmd = ["snapshot", "create", source, "--config-file", config_file]

        success, _, stderr, _ = runner.run(snapshot_cmd, repo_config=repo_config, dry_run=dry_run)
        if not success:
            logging.error(f"[kopia] Snapshot failed: {stderr}")
            backup_success = False
        else:
            print(f"[kopia] Snapshot complete.")

    # 3. Sync to cloud if configured (remote_destination_repo is set)
    if has_cloud:
        if not sync_to_cloud(repo_config, scheduled=scheduled, dry_run=dry_run):
            return False

    print(f"\n✓ {repo_name} complete")
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


def sync_to_cloud(repo_config: Dict[str, Any], scheduled: bool = False, dry_run: bool = False) -> bool:
    """Sync local repository to remote destination using rclone.

    Args:
        repo_config: Repository configuration dict
        scheduled: If True, quiet mode (no progress output)
        dry_run: If True, simulate without making changes

    Returns:
        True if sync succeeded, or None if skipped due to interval.
    """
    from datetime import datetime, timezone

    repo_name = repo_config['name']
    policies = repo_config.get('policies', {})
    interval_str = policies.get('remote-copy-interval', '60m')  # default 1 hour
    interval_seconds = parse_interval(interval_str)

    # Check if we should skip based on last sync time
    cloud_status = utils.load_cloud_sync_status()
    repo_cloud = cloud_status.get(repo_name, {})
    if repo_cloud and repo_cloud.get('success', False):
        last_sync_str = repo_cloud.get('last_sync')
        if last_sync_str:
            try:
                last_sync = datetime.fromisoformat(last_sync_str.replace('Z', '+00:00'))
                now = datetime.now(timezone.utc)
                elapsed = (now - last_sync).total_seconds()
                if elapsed < interval_seconds:
                    remaining = int((interval_seconds - elapsed) / 60)
                    print(f"[rclone] Skipping sync (last sync {int(elapsed/60)}m ago, interval {interval_str}, next in ~{remaining}m)")
                    return True  # Not a failure, just skipped
            except (ValueError, TypeError):
                pass  # Can't parse, proceed with sync

    rclone = utils.RcloneRunner()
    cloud_dest = repo_config['remote_destination_repo']
    local_path = repo_config['local_destination_repo']
    remote_name = cloud_dest.split(':')[0]

    # Check if rclone remote is configured
    if not rclone.check_remote_configured(remote_name):
        print(f"[rclone] ERROR: Remote '{remote_name}' not configured.")
        print(rclone.get_setup_instructions(remote_name))
        utils.update_cloud_sync_status(
            repo_config['name'], cloud_dest, success=False,
            error=f"Remote '{remote_name}' not configured"
        )
        return False

    print(f"\n[rclone] Syncing to {cloud_dest}...")
    success, error = rclone.sync(local_path, cloud_dest, quiet=scheduled, dry_run=dry_run)

    # Update status file for health check
    utils.update_cloud_sync_status(
        repo_config['name'], cloud_dest, success=success,
        error=error if not success else None
    )

    if success:
        print(f"[rclone] Sync complete.")
    else:
        print(f"[rclone] ERROR: {error}")

    return success


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
            return

    if args.register:
        success = register_backup_task(config)
        # Show what repos are configured (even on failure, so user can verify)
        logging.info(f"Configured {len(config['repositories'])} repo(s):")
        for repo in config['repositories']:
            logging.info(f"  {repo['name']}:")
            for src in repo.get('sources', []):
                logging.info(f"    - {src}")
        if not args.maintenance:
            return  # Exit after registration (unless maintenance also requested)

    # Auto-register if not already registered (only when running as admin and not launched by scheduler)
    if utils.is_admin() and not args.scheduled and not utils.is_task_registered(TASK_NAME):
        logging.info("Task not registered. Auto-registering scheduled task...")
        if register_backup_task(config):
            logging.info("Auto-registration complete. Continuing with backup...")
        else:
            logging.error("Auto-registration failed. Continuing with backup anyway...")
    
    for repo in config['repositories']:
        # Skip backups if only --register --maintenance was requested
        if not args.register:
            run_backup_job(runner, repo, scheduled=args.scheduled, dry_run=args.dry_run)

        if args.maintenance:
            logging.info(f"Running maintenance for {repo['name']}...")
            runner.run(
                ["maintenance", "run", "--full", "--config-file", repo['local_config_file_path']],
                repo_config=repo, dry_run=args.dry_run
            )

if __name__ == "__main__":
    main()
