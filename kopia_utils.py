"""
kopia_utils.py - Shared utilities for Kopia backup scripts.

Provides common functionality used across all Kopia scripts:
- Configuration loading from YAML (with path normalization)
- KopiaRunner class for command execution and password management
- Timestamp formatting (local or UTC)
- pythonw.exe detection for background task scheduling
- Windows Task Scheduler registration

Password lookup order:
    1. yaml config: password field
    2. ENV: KOPIA_PASSWORD_{REPO_NAME}
    3. .env.local: KOPIA_PASSWORD_{REPO_NAME}
    4. .env: KOPIA_PASSWORD_{REPO_NAME}
    5. ENV: KOPIA_PASSWORD (global fallback)
    6. .env.local: KOPIA_PASSWORD
    7. .env: KOPIA_PASSWORD
"""
import os
import sys
import subprocess
import logging
import yaml
import re
from datetime import datetime
from pathlib import Path
from typing import Tuple, Optional, List, Dict, Any, Union

# Determine script directory for relative paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

KOPIA_EXE = "kopia"
CONFIG_FILE = os.path.join(SCRIPT_DIR, "kopia-configs.yaml")


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load and parse the YAML config file.

    Normalizes all paths for the current OS (Windows/Linux).
    """
    if config_path is None:
        config_path = CONFIG_FILE
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: Config file not found: {config_path}", file=sys.stderr)
        print(f"  Copy kopia-configs.template.yaml to kopia-configs.yaml and edit it.", file=sys.stderr)
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error: Invalid YAML in {config_path}", file=sys.stderr)
        print(f"  {e}", file=sys.stderr)
        sys.exit(1)

    # Normalize all paths for current OS
    for repo in config.get('repositories', []):
        if 'repository_path' in repo:
            repo['repository_path'] = os.path.normpath(repo['repository_path'])
        if 'config_file_path' in repo:
            repo['config_file_path'] = os.path.normpath(repo['config_file_path'])
        if 'sources' in repo:
            repo['sources'] = [os.path.normpath(s) for s in repo['sources']]

    return config


class KopiaRunner:
    """Handles Kopia command execution and configuration."""

    def __init__(self, config_path: Optional[str] = None):
        self.config = load_config(config_path)
        self._kopia_not_found_warned = False

    def run(
        self,
        args: List[str],
        env: Optional[Dict[str, str]] = None,
        repo_config: Optional[Dict[str, Any]] = None,
        dry_run: bool = False,
        wait: bool = True,
        readonly: bool = False,
        log_level: Optional[str] = "error"
    ) -> Tuple[bool, str, str, Optional[subprocess.Popen]]:
        """Run a kopia command with the given arguments.

        Args:
            args: List of command arguments (without 'kopia' prefix)
            env: Environment dict (default: inherit from os.environ)
            repo_config: Repository config dict - if provided, automatically gets
                         password and sets up environment. Mutually exclusive with env.
            dry_run: If True, log command but don't execute
            wait: If True, wait for completion. If False, return Popen process.
            readonly: If True, disable content logging (for ls, snapshot list, diff, etc.)
            log_level: Kopia log level (default: "error" to reduce noise). Set to None to
                       use kopia's default. Options: debug, info, warning, error

        Returns:
            (success, stdout, stderr, process) - process is None for sync calls
        """
        # Handle repo_config - automatically sets up environment with password
        if repo_config is not None:
            if env is not None:
                raise ValueError("Cannot specify both 'env' and 'repo_config'")
            password = self.get_password(repo_config)
            if not password:
                repo_name = repo_config.get('name', 'unknown')
                error_msg = f"No password found for '{repo_name}'"
                return False, "", error_msg, None
            env = os.environ.copy()
            env["KOPIA_PASSWORD"] = password

        # Build command with logging options
        # Global flags must come before the subcommand
        cmd = [KOPIA_EXE]
        if log_level:
            cmd.extend(["--log-level", log_level])
        if readonly:
            cmd.append("--disable-content-log")
        cmd.extend(args)
        cmd_str = subprocess.list2cmdline(cmd)

        if dry_run:
            logging.info(f"[DRY-RUN] Would execute: {cmd_str}")
            return True, "", "", None

        logging.debug(f"Executing: {cmd_str}")

        try:
            # CREATE_NO_WINDOW prevents console popup on Windows
            creation_flags = 0x08000000 if sys.platform == "win32" else 0

            if wait:
                result = subprocess.run(
                    cmd,
                    env=env,
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    creationflags=creation_flags
                )
                return result.returncode == 0, result.stdout, result.stderr, None
            else:
                # For mount operations and streaming - return process for caller to manage
                # Redirect stderr to DEVNULL to prevent terminal contamination
                process = subprocess.Popen(
                    cmd,
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding='utf-8',
                    creationflags=creation_flags
                )
                return True, "", "", process

        except FileNotFoundError:
            if not self._kopia_not_found_warned:
                print("ERROR: 'kopia' not found. Ensure kopia.exe is in your PATH.", file=sys.stderr)
                self._kopia_not_found_warned = True
            return False, "", "kopia_not_found", None
        except Exception as e:
            return False, "", str(e), None

    def get_password(self, repo_config: Dict[str, Any]) -> Optional[str]:
        """Get password for a repository using the lookup chain.

        Lookup order:
        1. yaml config: password field (if present, use it)
        2. ENV: KOPIA_PASSWORD_{REPO_NAME}
        3. .env.local: KOPIA_PASSWORD_{REPO_NAME}
        4. .env: KOPIA_PASSWORD_{REPO_NAME}
        5. ENV: KOPIA_PASSWORD
        6. .env.local: KOPIA_PASSWORD
        7. .env: KOPIA_PASSWORD
        8. None if not found

        Returns the password string or None.
        """
        repo_name = repo_config.get('name', '')
        normalized_name = self._normalize_repo_name(repo_name)
        repo_specific_key = f'KOPIA_PASSWORD_{normalized_name}'
        global_key = 'KOPIA_PASSWORD'

        # 1. Check yaml config first (explicit config wins)
        password = repo_config.get('password')
        if password:
            return password

        # 2. Check ENV for repo-specific key
        password = os.environ.get(repo_specific_key)
        if password:
            return password

        # Parse .env.local once
        env_local = self._parse_env_file(os.path.join(SCRIPT_DIR, '.env.local'))

        # 3. Check .env.local repo-specific key
        password = env_local.get(repo_specific_key)
        if password:
            return password

        # Parse .env once
        env_file = self._parse_env_file(os.path.join(SCRIPT_DIR, '.env'))

        # 3. Check .env repo-specific key
        password = env_file.get(repo_specific_key)
        if password:
            return password

        # 5. Check ENV for global key
        password = os.environ.get(global_key)
        if password:
            return password

        # 6. Check .env.local for global key
        password = env_local.get(global_key)
        if password:
            return password

        # 6. Check .env for global key
        password = env_file.get(global_key)
        if password:
            return password

        # 8. Not found
        print(f"Error: No password found for repository '{repo_name}'", file=sys.stderr)
        print(f"  Set password in kopia-configs.yaml, environment variables, or .env/.env.local files.", file=sys.stderr)
        print(f"  See README.md for details.", file=sys.stderr)
        return None

    def _normalize_repo_name(self, name: str) -> str:
        """Convert repo name to environment variable format.

        Example: 'mysrc-backup' -> 'MYSRC_BACKUP'
        """
        return name.upper().replace('-', '_')

    def _parse_env_file(self, filepath: str) -> Dict[str, str]:
        """Parse env file and return dict of all KOPIA_PASSWORD* keys.

        Uses python-dotenv if available, falls back to simple parsing.
        """
        if not os.path.exists(filepath):
            return {}

        try:
            from dotenv import dotenv_values
            all_vars = dotenv_values(filepath)
            return {k: v for k, v in all_vars.items() if k and v and k.startswith('KOPIA_PASSWORD')}
        except ImportError:
            pass  # Fall back to simple parsing

        # Simple fallback parser
        result = {}
        try:
            with open(filepath, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#') or '=' not in line:
                        continue
                    if not line.startswith('KOPIA_PASSWORD'):
                        continue
                    key, value = line.split('=', 1)
                    key, value = key.strip(), value.strip()
                    if len(value) >= 2 and value[0] == value[-1] and value[0] in '"\'':
                        value = value[1:-1]
                    result[key] = value
        except Exception:
            pass
        return result


def format_timestamp_local(time_str: str) -> str:
    """Format an ISO timestamp to local time.

    Args:
        time_str: ISO timestamp string (e.g., "2025-11-22T05:04:01Z")

    Returns:
        Formatted string like "2025-11-22 16:04:01 +1100"
    """
    try:
        dt_utc = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        dt_local = dt_utc.astimezone()
        return dt_local.strftime("%Y-%m-%d %H:%M:%S %z")
    except Exception:
        return time_str


def format_timestamp_utc(time_str: str) -> str:
    """Format an ISO timestamp to UTC (Zulu) time.

    Args:
        time_str: ISO timestamp string (e.g., "2025-11-22T05:04:01Z")

    Returns:
        Formatted string like "2025-11-22 05:04:01Z"
    """
    try:
        dt_utc = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        return dt_utc.strftime("%Y-%m-%d %H:%M:%S") + "Z"
    except Exception:
        return time_str


def format_kopia_verbose_timestamp(timestamp_str: str) -> str:
    """Clean up verbose kopia timestamp from diff output.

    Args:
        timestamp_str: Verbose timestamp like "2025-11-22 16:09:30.2325872 +1100 AEDT"

    Returns:
        Cleaned string like "2025-11-22 16:09:30 +1100"
    """
    # Use regex to extract date, time (no micros), and timezone offset
    match = re.match(r"^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})(?:\.\d+)?\s+([+-]\d{4})", timestamp_str)
    if match:
        return f"{match.group(1)} {match.group(2)} {match.group(3)}"
    
    # Fallback to simple split if regex fails
    try:
        parts = timestamp_str.split()
        ts = f"{parts[0]} {parts[1].split('.')[0]}"  # date + time without microseconds
        tz = next((p for p in parts if p.startswith(('+', '-'))), '')
        return f"{ts} {tz}".strip()
    except Exception:
        return timestamp_str.split("+")[0].strip()


def get_pythonw_exe() -> str:
    """Get path to pythonw.exe for background execution (no console window).

    Finds pythonw.exe in the same directory as the current Python interpreter,
    regardless of how Python was invoked (python.exe, py.exe, python3.exe, etc.).

    Returns:
        Path to pythonw.exe if it exists, otherwise sys.executable.
    """
    python_dir = Path(sys.executable).parent
    pythonw_exe = python_dir / "pythonw.exe"

    if pythonw_exe.exists():
        return str(pythonw_exe)
    else:
        logging.warning("pythonw.exe not found, using python.exe (console may be visible)")
        return sys.executable


def is_task_registered(task_name: str) -> bool:
    """Check if a Windows Task Scheduler task is registered.

    Args:
        task_name: Name of the scheduled task

    Returns:
        True if task exists, False otherwise.
    """
    try:
        result = subprocess.run(
            ["schtasks", "/query", "/TN", task_name],
            capture_output=True,
            text=True
        )
        return result.returncode == 0
    except Exception:
        return False


def register_scheduled_task(
    task_name: str,
    script_path: str,
    interval_minutes: int,
    extra_args: Optional[List[str]] = None,
    run_elevated: bool = False
) -> bool:
    """Register a script to run periodically via Windows Task Scheduler.

    Args:
        task_name: Name for the scheduled task
        script_path: Absolute path to the Python script
        interval_minutes: How often to run (in minutes)
        extra_args: List of additional arguments to pass to the script
        run_elevated: If True, run with highest privileges (admin)

    Returns:
        True if registration succeeded, False otherwise.
    """
    pythonw_exe = get_pythonw_exe()

    # Build the action command
    action = f'"{pythonw_exe}" "{script_path}" --scheduled'
    if extra_args:
        action += ' ' + ' '.join(extra_args)

    cmd = [
        "schtasks", "/create",
        "/tn", task_name,
        "/tr", action,
        "/sc", "minute",
        "/mo", str(interval_minutes),
        "/f"  # Force overwrite if exists
    ]

    if run_elevated:
        cmd.extend(["/rl", "highest"])

    print(f"Registering task '{task_name}' to run every {interval_minutes} minutes...")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"Successfully registered '{task_name}'")
            return True
        else:
            print(f"Failed to register task: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error registering task: {e}")
        return False
