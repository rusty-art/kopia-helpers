#!/usr/bin/env python
"""
kopia-stop-backups.py - Unregister Kopia scheduled tasks.

Removes the Windows Task Scheduler tasks for backups and health checks.

Usage:
    python kopia-stop-backups.py              # Unregister all tasks
    python kopia-stop-backups.py --backups    # Unregister backup task only
    python kopia-stop-backups.py --health     # Unregister health check task only

Requires:
    - Admin rights to modify Task Scheduler
    - Windows OS (uses schtasks)
"""
import sys
import argparse
import subprocess
import kopia_utils as utils

# Exit codes
EXIT_SUCCESS = 0
EXIT_NOT_ADMIN = 1
EXIT_TASK_ERROR = 2
EXIT_NOT_WINDOWS = 3

BACKUP_TASK_NAME = "KopiaHelperBackup"
HEALTH_TASK_NAME = "KopiaHelperHealth"


def unregister_task(task_name: str) -> bool:
    """Unregister a Windows Task Scheduler task.

    Args:
        task_name: Name of the task to remove

    Returns:
        True if task was removed or didn't exist, False on error.
    """
    try:
        # Check if task exists first
        check_result = subprocess.run(
            ["schtasks", "/Query", "/TN", task_name],
            capture_output=True,
            text=True
        )

        if check_result.returncode != 0:
            print(f"Task '{task_name}' not found (already unregistered)")
            return True

        # Delete the task
        result = subprocess.run(
            ["schtasks", "/Delete", "/TN", task_name, "/F"],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            print(f"Successfully unregistered '{task_name}'")
            return True
        else:
            print(f"Failed to unregister '{task_name}': {result.stderr.strip()}")
            return False

    except Exception as e:
        print(f"Error unregistering '{task_name}': {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Unregister Kopia scheduled tasks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python kopia-stop-backups.py              Unregister all tasks
  python kopia-stop-backups.py --backups    Unregister backup task only
  python kopia-stop-backups.py --health     Unregister health check task only
"""
    )
    parser.add_argument("--backups", action="store_true",
                        help="Unregister backup task only")
    parser.add_argument("--health", action="store_true",
                        help="Unregister health check task only")
    args = parser.parse_args()

    # If neither flag specified, unregister both
    unregister_backups = args.backups or (not args.backups and not args.health)
    unregister_health = args.health or (not args.backups and not args.health)

    # Check Windows platform
    if sys.platform != "win32":
        print("This script is only supported on Windows.")
        print("On other platforms, use your system's cron or systemd to manage scheduled tasks.")
        sys.exit(EXIT_NOT_WINDOWS)

    # Check admin up-front before doing any work
    if not utils.is_admin():
        print("Administrator privileges required to modify Task Scheduler.")
        print("Please re-run as Administrator.")
        sys.exit(EXIT_NOT_ADMIN)

    success = True

    if unregister_backups:
        if not unregister_task(BACKUP_TASK_NAME):
            success = False

    if unregister_health:
        if not unregister_task(HEALTH_TASK_NAME):
            success = False

    if success:
        print("\nScheduled tasks unregistered. Backups will no longer run automatically.")
        print("To re-enable, run: python kopia-start-backups.py")
        sys.exit(EXIT_SUCCESS)
    else:
        print("\nSome tasks could not be unregistered.")
        sys.exit(EXIT_TASK_ERROR)


if __name__ == "__main__":
    main()
