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
import ctypes
import kopia_utils as utils
from typing import Dict, Any

# Logging configured in main() after argument parsing

# Default (can be overridden in kopia-configs.yaml under 'settings')
DEFAULT_BACKUP_INTERVAL_MINUTES = 15
TASK_NAME = "KopiaHelperBackup"


def ensure_repository_connected(runner: utils.KopiaRunner, repo_config: Dict[str, Any], dry_run: bool = False) -> bool:
    """Connects to the repository if not already connected."""
    repo_path = repo_config['repository_path']
    config_file = repo_config['config_file_path']

    # Ensure config dir exists
    config_dir = os.path.dirname(config_file)
    if not os.path.exists(config_dir) and not dry_run:
        os.makedirs(config_dir, exist_ok=True)

    # Check status first to see if connected
    logging.info(f"Checking connection for {repo_config['name']}...")
    success, _, stderr, _ = runner.run(
        ["repository", "status", "--config-file", config_file],
        repo_config=repo_config, dry_run=dry_run, readonly=True
    )

    if success:
        logging.info(f"Already connected to {repo_config['name']}")
        return True

    # Check for password error (returned by runner when repo_config password lookup fails)
    if "No password found" in stderr:
        logging.error(stderr)
        return False

    # If not connected, try to connect
    logging.info(f"Connecting to filesystem repository at {repo_path}...")
    success, _, stderr, _ = runner.run(
        ["repository", "connect", "filesystem",
         "--path", repo_path, "--config-file", config_file],
        repo_config=repo_config, dry_run=dry_run
    )

    if not success:
        # If connect failed, maybe it doesn't exist? Try create?
        logging.info(f"Connection failed. Attempting to CREATE repository at {repo_path}...")
        success, _, stderr, _ = runner.run(
            ["repository", "create", "filesystem",
             "--path", repo_path, "--config-file", config_file],
            repo_config=repo_config, dry_run=dry_run
        )
        if not success:
            logging.error(f"Failed to create repository: {stderr}")

    return success

def run_backup_job(runner: utils.KopiaRunner, repo_config: Dict[str, Any], dry_run: bool = False) -> None:
    """Runs the backup for a single repository configuration."""
    sources = repo_config.get('sources', [])
    logging.info(f"Backing up: {repo_config['name']} ({len(sources)} source(s))")
    for src in sources:
        logging.info(f"  - {src}")

    if not ensure_repository_connected(runner, repo_config, dry_run):
        logging.error(f"Skipping job {repo_config['name']} due to connection failure.")
        return

    config_file = repo_config['config_file_path']
    policies = repo_config.get('policies', {})

    for source in repo_config['sources']:
        # 1. Set Policy (Idempotent-ish, but good to ensure)
        logging.info(f"Ensuring policies for {source}...")
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
            logging.warning(f"Policy set failed: {stderr}")

        # Handle ignore patterns - must be separate commands because Kopia
        # processes --clear-ignore AFTER --add-ignore when on same command line
        clear_ignore_cmd = ["policy", "set", source, "--config-file", config_file, "--clear-ignore"]
        success, _, stderr, _ = runner.run(clear_ignore_cmd, repo_config=repo_config, dry_run=dry_run)
        if not success:
            logging.warning(f"Clear ignore failed: {stderr}")

        if 'ignore' in policies and policies['ignore']:
            add_ignore_cmd = ["policy", "set", source, "--config-file", config_file]
            for pattern in policies['ignore']:
                add_ignore_cmd.extend(["--add-ignore", pattern])
            success, _, stderr, _ = runner.run(add_ignore_cmd, repo_config=repo_config, dry_run=dry_run)
            if not success:
                logging.warning(f"Add ignore patterns failed: {stderr}")

        # 2. Create Snapshot
        logging.info(f"Creating snapshot for {source}...")
        snapshot_cmd = ["snapshot", "create", source, "--config-file", config_file]

        success, _, stderr, _ = runner.run(snapshot_cmd, repo_config=repo_config, dry_run=dry_run)
        if not success:
            logging.error(f"Snapshot failed: {stderr}")

def is_admin() -> bool:
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False

def register_backup_task(config: Dict[str, Any]) -> bool:
    """Register this script to run periodically via Windows Task Scheduler (with Admin rights)."""
    if not is_admin():
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

    if not is_admin():
        if args.dry_run or args.no_elevation:
            if args.no_elevation:
                logging.info("Running without elevation (--no-elevation). VSS snapshots won't work.")
            else:
                logging.warning("Running in dry-run mode without Admin privileges. VSS checks might be inaccurate.")
        else:
            # Re-run the script with Admin privileges
            logging.info("Requesting Administrator privileges...")
            try:
                # Re-construct command line arguments
                params = " ".join([f'"{arg}"' for arg in sys.argv[1:]])
                ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, f'"{__file__}" {params}', None, 1)
                sys.exit(0) # Exit this instance, the elevated one will run
            except Exception as e:
                logging.error(f"Failed to elevate privileges: {e}")
                logging.error("Please run this script as Administrator.")
                sys.exit(1)


    # Initialize KopiaRunner (loads config)
    runner = utils.KopiaRunner()
    config = runner.config

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
    if is_admin() and not args.scheduled and not utils.is_task_registered(TASK_NAME):
        logging.info("Task not registered. Auto-registering scheduled task...")
        if register_backup_task(config):
            logging.info("Auto-registration complete. Continuing with backup...")
        else:
            logging.error("Auto-registration failed. Continuing with backup anyway...")
    
    for repo in config['repositories']:
        # Skip backups if only --register --maintenance was requested
        if not args.register:
            run_backup_job(runner, repo, dry_run=args.dry_run)

        if args.maintenance:
            logging.info(f"Running maintenance for {repo['name']}...")
            runner.run(
                ["maintenance", "run", "--full", "--config-file", repo['config_file_path']],
                repo_config=repo, dry_run=args.dry_run
            )

if __name__ == "__main__":
    main()
