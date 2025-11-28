"""
kopia_utils.py - Shared utilities for Kopia backup scripts.

Provides common functionality used across all Kopia scripts:
- Configuration loading from YAML (with path normalization and field name migration)
- KopiaRunner class for command execution and password management
- RcloneRunner class for cloud sync operations
- Cloud path detection (is_cloud_path) and remote destination checking
- Timestamp formatting (local or UTC)
- pythonw.exe detection for background task scheduling
- Windows Task Scheduler registration

Config field names (backwards compatible):
    - local_destination_repo (or repository_path, destination_repo) - where Kopia writes locally
    - local_config_file_path (or config_file_path) - Kopia config file
    - remote_destination_repo - rclone destination for cloud sync (optional)

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
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple, Optional, List, Dict, Any, Union

# Determine script directory for relative paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

KOPIA_EXE = "kopia"
RCLONE_EXE = "rclone"
CONFIG_FILE = os.path.join(SCRIPT_DIR, "kopia-configs.yaml")
STATUS_FILE = os.path.join(Path.home(), ".kopia-status.json")


def check_tool_available(tool_name: str) -> Tuple[bool, str]:
    """Check if an external tool is available in PATH.

    Args:
        tool_name: Name of the executable to check (e.g., 'kopia', 'rclone')

    Returns:
        Tuple of (is_available, version_or_error_message)
    """
    try:
        # Use 'where' on Windows, 'which' on Unix
        if sys.platform == "win32":
            result = subprocess.run(
                ["where", tool_name],
                capture_output=True,
                text=True,
                creationflags=0x08000000  # CREATE_NO_WINDOW
            )
        else:
            result = subprocess.run(
                ["which", tool_name],
                capture_output=True,
                text=True
            )

        if result.returncode != 0:
            return False, f"'{tool_name}' not found in PATH"

        # Try to get version
        try:
            version_result = subprocess.run(
                [tool_name, "--version"],
                capture_output=True,
                text=True,
                timeout=10,
                creationflags=0x08000000 if sys.platform == "win32" else 0
            )
            version = version_result.stdout.strip().split('\n')[0] if version_result.stdout else "unknown version"
            return True, version
        except Exception:
            return True, "version unknown"

    except FileNotFoundError:
        return False, f"'{tool_name}' not found in PATH"
    except Exception as e:
        return False, str(e)


def get_kopia_install_instructions() -> str:
    """Get installation instructions for Kopia."""
    if sys.platform == "win32":
        return """
Kopia is not installed or not in PATH.

Installation options:
  1. Winget (recommended):
     winget install kopia

  2. Scoop:
     scoop bucket add extras
     scoop install kopia

  3. Manual download:
     https://github.com/kopia/kopia/releases
     Download kopia-X.X.X-windows-x64.zip, extract, add to PATH

After installation, verify with: kopia --version
"""
    elif sys.platform == "darwin":
        return """
Kopia is not installed or not in PATH.

Installation options:
  1. Homebrew (recommended):
     brew install kopia

  2. Manual download:
     https://github.com/kopia/kopia/releases

After installation, verify with: kopia --version
"""
    else:
        return """
Kopia is not installed or not in PATH.

Installation options:
  1. Package manager (Debian/Ubuntu):
     curl -s https://kopia.io/signing-key | sudo gpg --dearmor -o /etc/apt/keyrings/kopia-keyring.gpg
     echo "deb [signed-by=/etc/apt/keyrings/kopia-keyring.gpg] http://packages.kopia.io/apt/ stable main" | sudo tee /etc/apt/sources.list.d/kopia.list
     sudo apt update && sudo apt install kopia

  2. Manual download:
     https://github.com/kopia/kopia/releases

After installation, verify with: kopia --version
"""


def get_rclone_install_instructions() -> str:
    """Get installation instructions for rclone."""
    if sys.platform == "win32":
        return """
rclone is not installed or not in PATH.

Installation options:
  1. Winget (recommended):
     winget install Rclone.Rclone

  2. Scoop:
     scoop install rclone

  3. Manual download:
     https://rclone.org/downloads/
     Download rclone-vX.X.X-windows-amd64.zip, extract, add to PATH

After installation:
  1. Verify with: rclone --version
  2. Configure remotes with: rclone config
"""
    elif sys.platform == "darwin":
        return """
rclone is not installed or not in PATH.

Installation options:
  1. Homebrew (recommended):
     brew install rclone

  2. Manual download:
     https://rclone.org/downloads/

After installation:
  1. Verify with: rclone --version
  2. Configure remotes with: rclone config
"""
    else:
        return """
rclone is not installed or not in PATH.

Installation options:
  1. Package manager:
     sudo apt install rclone  # Debian/Ubuntu
     sudo dnf install rclone  # Fedora

  2. Official installer (recommended for latest version):
     curl https://rclone.org/install.sh | sudo bash

  3. Manual download:
     https://rclone.org/downloads/

After installation:
  1. Verify with: rclone --version
  2. Configure remotes with: rclone config
"""


def validate_required_tools(require_rclone: bool = False) -> Tuple[bool, List[str]]:
    """Validate that required external tools are available.

    Args:
        require_rclone: If True, also check for rclone availability

    Returns:
        Tuple of (all_tools_available, list_of_error_messages)
    """
    errors = []

    # Check kopia
    kopia_ok, kopia_result = check_tool_available(KOPIA_EXE)
    if not kopia_ok:
        errors.append(f"ERROR: {kopia_result}")
        errors.append(get_kopia_install_instructions())
    else:
        logging.debug(f"Found kopia: {kopia_result}")

    # Check rclone if required
    if require_rclone:
        rclone_ok, rclone_result = check_tool_available(RCLONE_EXE)
        if not rclone_ok:
            errors.append(f"ERROR: {rclone_result}")
            errors.append(get_rclone_install_instructions())
        else:
            logging.debug(f"Found rclone: {rclone_result}")

    return len(errors) == 0, errors


def is_cloud_path(path: str) -> bool:
    """Check if path is a cloud destination (e.g., onedrive:, gdrive:).

    Cloud paths have the format 'remote:path' where remote doesn't look like
    a Windows drive letter (e.g., 'C:').

    Args:
        path: Path string to check

    Returns:
        True if path appears to be a cloud/rclone remote path.
    """
    if not path or ':' not in path:
        return False
    # Windows paths like C:\foo have colon at position 1
    # Cloud paths like onedrive:foo have colon after the remote name
    colon_pos = path.index(':')
    # Windows drive letters are single characters (C:, D:, etc.)
    # Cloud remotes are typically longer (onedrive:, gdrive:, s3:, etc.)
    return colon_pos > 1


def has_remote_destination(repo_config: Dict[str, Any]) -> bool:
    """Check if repository has a remote destination for cloud sync.

    Args:
        repo_config: Repository configuration dict

    Returns:
        True if remote_destination_repo is configured.
    """
    return bool(repo_config.get('remote_destination_repo'))


def get_local_repo_path(repo_config: Dict[str, Any]) -> str:
    """Get the local filesystem path where Kopia writes.

    Args:
        repo_config: Repository configuration dict (uses local_destination_repo)

    Returns:
        Filesystem path for Kopia repository.
    """
    return repo_config.get('local_destination_repo', '')


def get_config_file_path(repo_config: Dict[str, Any]) -> str:
    """Get the config file path for a repository.

    Args:
        repo_config: Repository configuration dict (uses local_config_file_path)

    Returns:
        Path to repository config file.
    """
    return repo_config.get('local_config_file_path', '')


# Valid field names for repository config (for validation)
VALID_REPO_FIELDS = {
    # Current field names
    'name', 'local_destination_repo', 'local_config_file_path', 'remote_destination_repo',
    'password', 'sources', 'policies',
    # Backwards-compatible aliases
    'repository_path', 'config_file_path', 'destination_repo',
}

REQUIRED_REPO_FIELDS = {'name', 'local_destination_repo', 'local_config_file_path', 'sources'}


def validate_source_paths(sources: List[str], repo_name: str) -> Tuple[bool, List[str]]:
    """Validate that source paths exist.

    Args:
        sources: List of source paths to validate
        repo_name: Name of the repository (for error messages)

    Returns:
        Tuple of (all_valid, list_of_warning_messages)
    """
    warnings = []
    for source in sources:
        if not os.path.exists(source):
            warnings.append(f"  Warning: Source path does not exist for '{repo_name}': {source}")
    return len(warnings) == 0, warnings


def load_config(config_path: Optional[str] = None, validate_sources: bool = True) -> Dict[str, Any]:
    """Load and parse the YAML config file.

    Normalizes all paths for the current OS (Windows/Linux).
    Also normalizes field names for backwards compatibility:
      - repository_path -> local_destination_repo
      - config_file_path -> local_config_file_path

    Validates field names and required fields, exits with error on unknown fields.
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

    # Validate and normalize each repository
    for repo in config.get('repositories', []):
        repo_name = repo.get('name', '<unnamed>')

        # Check for unknown fields (typos)
        unknown_fields = set(repo.keys()) - VALID_REPO_FIELDS
        if unknown_fields:
            print(f"Error: Unknown field(s) in repository '{repo_name}': {', '.join(sorted(unknown_fields))}", file=sys.stderr)
            print(f"  Valid fields: {', '.join(sorted(VALID_REPO_FIELDS))}", file=sys.stderr)
            sys.exit(1)

        # Backwards compatibility: accept both old and new field names
        # Prefer new names if both are present

        # repository_path -> local_destination_repo
        if 'local_destination_repo' not in repo and 'repository_path' in repo:
            repo['local_destination_repo'] = repo.pop('repository_path')
        elif 'repository_path' in repo:
            repo.pop('repository_path')  # Remove old name if new name exists

        # destination_repo -> local_destination_repo
        if 'local_destination_repo' not in repo and 'destination_repo' in repo:
            repo['local_destination_repo'] = repo.pop('destination_repo')
        elif 'destination_repo' in repo:
            repo.pop('destination_repo')  # Remove old name if new name exists

        # config_file_path -> local_config_file_path
        if 'local_config_file_path' not in repo and 'config_file_path' in repo:
            repo['local_config_file_path'] = repo.pop('config_file_path')
        elif 'config_file_path' in repo:
            repo.pop('config_file_path')  # Remove old name if new name exists

        # Check required fields (after normalization)
        missing_fields = REQUIRED_REPO_FIELDS - set(repo.keys())
        if missing_fields:
            print(f"Error: Missing required field(s) in repository '{repo_name}': {', '.join(sorted(missing_fields))}", file=sys.stderr)
            sys.exit(1)

        # Normalize paths for current OS
        if 'local_destination_repo' in repo:
            repo['local_destination_repo'] = os.path.normpath(repo['local_destination_repo'])
        if 'local_config_file_path' in repo:
            repo['local_config_file_path'] = os.path.normpath(repo['local_config_file_path'])
        # remote_destination_repo is a cloud path (rclone), don't normalize
        if 'sources' in repo:
            repo['sources'] = [os.path.normpath(s) for s in repo['sources']]

        # Validate source paths exist (optional, but warn if not)
        if validate_sources and 'sources' in repo:
            _, source_warnings = validate_source_paths(repo['sources'], repo_name)
            for warning in source_warnings:
                print(warning, file=sys.stderr)

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


def is_admin() -> bool:
    """Check if the current process is running with Administrator privileges.

    Returns:
        True if running as Administrator, False otherwise.
    """
    if sys.platform != "win32":
        # On non-Windows, check if running as root
        return os.geteuid() == 0 if hasattr(os, 'geteuid') else False

    try:
        import ctypes
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except Exception:
        return False


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


class RcloneRunner:
    """Handles rclone command execution for cloud sync operations."""

    def __init__(self):
        self._rclone_not_found_warned = False

    def check_remote_configured(self, remote_name: str) -> bool:
        """Check if an rclone remote is configured.

        Args:
            remote_name: Name of the remote (e.g., 'onedrive')

        Returns:
            True if remote is configured, False otherwise.
        """
        try:
            result = subprocess.run(
                [RCLONE_EXE, "listremotes"],
                capture_output=True,
                text=True,
                encoding='utf-8'
            )
            if result.returncode != 0:
                return False
            # listremotes outputs "remotename:" on each line
            configured_remotes = [line.rstrip(':') for line in result.stdout.strip().split('\n') if line]
            return remote_name in configured_remotes
        except FileNotFoundError:
            if not self._rclone_not_found_warned:
                logging.error("rclone not found. Install from https://rclone.org/downloads/")
                self._rclone_not_found_warned = True
            return False
        except Exception as e:
            logging.error(f"Error checking rclone remotes: {e}")
            return False

    def sync(
        self,
        local_path: str,
        remote: str,
        quiet: bool = False,
        dry_run: bool = False
    ) -> Tuple[bool, str]:
        """Sync local path to remote using rclone sync.

        Args:
            local_path: Local filesystem path to sync from
            remote: Remote destination (e.g., 'onedrive:Backups/kopia')
            quiet: If True, minimal output (for scheduled runs)
            dry_run: If True, simulate without making changes

        Returns:
            (success, error_message) tuple
        """
        cmd = [
            RCLONE_EXE, "sync",
            local_path, remote,
            "--transfers=4",
            "--checkers=8",
            "--retries=3",
            "--low-level-retries=10",
        ]

        if quiet:
            cmd.append("--quiet")
        else:
            cmd.extend(["--stats=30s", "--progress"])

        if dry_run:
            cmd.append("--dry-run")
            logging.info(f"[DRY-RUN] Would execute: {subprocess.list2cmdline(cmd)}")
            return True, ""

        logging.debug(f"Executing: {subprocess.list2cmdline(cmd)}")

        try:
            if quiet:
                # Scheduled mode: capture output, hide window
                creation_flags = 0x08000000 if sys.platform == "win32" else 0
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    creationflags=creation_flags
                )
                if result.returncode == 0:
                    return True, ""
                else:
                    error_msg = result.stderr.strip() or f"rclone exited with code {result.returncode}"
                    return False, error_msg
            else:
                # Interactive mode: let output flow to terminal for progress display
                result = subprocess.run(cmd)
                if result.returncode == 0:
                    return True, ""
                else:
                    return False, f"rclone exited with code {result.returncode}"

        except KeyboardInterrupt:
            logging.warning("Sync interrupted")
            return False, "Sync interrupted by user"
        except FileNotFoundError:
            if not self._rclone_not_found_warned:
                logging.error("rclone not found. Install from https://rclone.org/downloads/")
                self._rclone_not_found_warned = True
            return False, "rclone_not_found"
        except Exception as e:
            return False, str(e)

    @staticmethod
    def get_setup_instructions(remote_type: str = "onedrive") -> str:
        """Get setup instructions for configuring an rclone remote.

        Args:
            remote_type: Type of remote (default: 'onedrive')

        Returns:
            Multi-line instruction string.
        """
        if remote_type == "onedrive":
            return """
To set up OneDrive with rclone:
  1. Run: rclone config
  2. Choose 'n' for new remote
  3. Name it: onedrive
  4. Choose 'onedrive' as storage type
  5. Follow the browser auth flow
  6. Test with: rclone lsd onedrive:

For detailed instructions: https://rclone.org/onedrive/
"""
        return f"Run 'rclone config' to set up a '{remote_type}' remote."


def _load_status_file() -> Dict[str, Any]:
    """Load the entire status file.

    Returns:
        Full status dict, or empty dict if file doesn't exist.
    """
    if not os.path.exists(STATUS_FILE):
        return {}
    try:
        with open(STATUS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logging.warning(f"Could not load status file: {e}")
        return {}


def _save_status_file(status: Dict[str, Any]) -> None:
    """Save status file atomically (write to temp, then rename).

    Args:
        status: Full status dict to save.
    """
    try:
        # Write to temp file first
        temp_file = STATUS_FILE + ".tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(status, f, indent=2)
        # Atomic rename (on Windows, need to remove target first)
        if sys.platform == "win32" and os.path.exists(STATUS_FILE):
            os.replace(temp_file, STATUS_FILE)
        else:
            os.rename(temp_file, STATUS_FILE)
    except IOError as e:
        logging.error(f"Could not save status file: {e}")


def load_cloud_sync_status() -> Dict[str, Any]:
    """Load cloud sync status from the status file.

    Returns:
        Dict mapping repo names to their sync status, or empty dict if not found.
    """
    status = _load_status_file()
    return status.get("cloud_sync", {})


def update_cloud_sync_status(
    repo_name: str,
    remote: str,
    success: bool,
    error: Optional[str] = None
) -> None:
    """Update the cloud sync status for a repository.

    Args:
        repo_name: Name of the repository
        remote: Remote destination (e.g., 'onedrive:Backups/kopia')
        success: Whether the sync succeeded
        error: Error message if sync failed
    """
    status = _load_status_file()
    if "cloud_sync" not in status:
        status["cloud_sync"] = {}

    status["cloud_sync"][repo_name] = {
        "last_sync": datetime.now(timezone.utc).isoformat(),
        "success": success,
        "remote": remote,
        "error": error
    }
    _save_status_file(status)


def cleanup_status_file(valid_repo_names: List[str]) -> None:
    """Remove status entries for repos that no longer exist in config.

    Args:
        valid_repo_names: List of repo names currently in config.
    """
    status = _load_status_file()
    changed = False

    if "cloud_sync" in status:
        stale_repos = [name for name in status["cloud_sync"] if name not in valid_repo_names]
        for name in stale_repos:
            del status["cloud_sync"][name]
            logging.debug(f"Cleaned up stale status for '{name}'")
            changed = True

    if changed:
        _save_status_file(status)
