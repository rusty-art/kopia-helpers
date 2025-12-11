"""
kopia_utils.py - Shared utilities for Kopia backup scripts.

Provides common functionality used across all Kopia scripts:
- Configuration loading from YAML (with path normalization)
- KopiaRunner class for command execution and password management
- KopiaSyncRunner class for repository sync-to operations
- Timestamp formatting (local or UTC)
- pythonw.exe detection for background task scheduling
- Windows Task Scheduler registration

Config field names:
    - repo_destination - where Kopia writes locally
    - repo_config - Kopia config file path
    - sync-to - list of sync destinations (type, interval, extra-args)

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
CONFIG_FILE = os.path.join(SCRIPT_DIR, "kopia-helpers.yaml")
STATUS_FILE = os.path.join(Path.home(), ".kopia-helpers-status.json")


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
    """Check if repository has any sync-to destinations configured.

    Args:
        repo_config: Repository configuration dict

    Returns:
        True if sync-to list is configured and non-empty.
    """
    sync_to = repo_config.get('sync-to', [])
    return bool(sync_to and len(sync_to) > 0)


def get_local_repo_path(repo_config: Dict[str, Any]) -> str:
    """Get the local filesystem path where Kopia writes.

    Args:
        repo_config: Repository configuration dict (uses repo_destination)

    Returns:
        Filesystem path for Kopia repository.
    """
    return repo_config.get('repo_destination', '')


def get_config_file_path(repo_config: Dict[str, Any]) -> str:
    """Get the config file path for a repository.

    Args:
        repo_config: Repository configuration dict (uses repo_config)

    Returns:
        Path to repository config file.
    """
    return repo_config.get('repo_config', '')


# Valid field names for repository config (for validation)
VALID_REPO_FIELDS = {
    'name', 'repo_destination', 'repo_config', 'repo_password',
    'sources', 'policies', 'sync-from', 'sync-to',
}

REQUIRED_REPO_FIELDS = {'name', 'repo_destination', 'repo_config', 'sources'}


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
    Validates field names and required fields, exits with error on unknown fields.
    """
    if config_path is None:
        config_path = CONFIG_FILE
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: Config file not found: {config_path}", file=sys.stderr)
        print(f"  Copy kopia-helpers.template.yaml to kopia-helpers.yaml and edit it.", file=sys.stderr)
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

        # Check required fields
        missing_fields = REQUIRED_REPO_FIELDS - set(repo.keys())
        if missing_fields:
            print(f"Error: Missing required field(s) in repository '{repo_name}': {', '.join(sorted(missing_fields))}", file=sys.stderr)
            sys.exit(1)

        # Normalize paths for current OS
        if 'repo_destination' in repo:
            repo['repo_destination'] = os.path.normpath(repo['repo_destination'])
        if 'repo_config' in repo:
            repo['repo_config'] = os.path.normpath(repo['repo_config'])
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

    def __init__(self, config_path: Optional[str] = None, skip_config: bool = False):
        self.config = {} if skip_config else load_config(config_path)
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
        password = repo_config.get('repo_password')
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
        print(f"  Set password in kopia-helpers.yaml, environment variables, or .env/.env.local files.", file=sys.stderr)
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


class KopiaSyncRunner:
    """Handles kopia repository sync-to operations for syncing to remote destinations."""

    # Backends that should use --flat flag (cloud storage)
    CLOUD_BACKENDS = {'rclone', 's3', 'gcs', 'azure', 'b2', 'gdrive'}

    # Required parameters for each backend type
    BACKEND_PARAMS = {
        'rclone': {'required': ['remote-path'], 'cmd_prefix': ['--remote-path=']},
        's3': {'required': ['bucket'], 'cmd_prefix': ['--bucket=']},
        'gcs': {'required': ['bucket'], 'cmd_prefix': ['--bucket=']},
        'azure': {'required': ['container', 'storage-account'], 'cmd_prefix': ['--container=', '--storage-account=']},
        'b2': {'required': ['bucket'], 'cmd_prefix': ['--bucket=']},
        'gdrive': {'required': ['folder-id'], 'cmd_prefix': ['--folder-id=']},
        'filesystem': {'required': ['path'], 'cmd_prefix': ['--path=']},
        'sftp': {'required': ['path', 'host', 'username'], 'cmd_prefix': ['--path=', '--host=', '--username=']},
        'webdav': {'required': ['url'], 'cmd_prefix': ['--url=']},
    }

    def __init__(self):
        self._warned_remotes = set()

    @staticmethod
    def get_destination_id(dest_config: Dict[str, Any]) -> str:
        """Generate a unique identifier for a sync destination.

        Args:
            dest_config: Destination configuration dict

        Returns:
            A string identifier like 'rclone:onedrive:mybackups' or 's3:my-bucket'
        """
        dest_type = dest_config.get('type', 'unknown')
        if dest_type == 'rclone':
            return f"rclone:{dest_config.get('remote-path', 'unknown')}"
        elif dest_type in ('s3', 'gcs', 'b2'):
            return f"{dest_type}:{dest_config.get('bucket', 'unknown')}"
        elif dest_type == 'azure':
            return f"azure:{dest_config.get('container', 'unknown')}"
        elif dest_type == 'gdrive':
            return f"gdrive:{dest_config.get('folder-id', 'unknown')}"
        elif dest_type == 'filesystem':
            return f"filesystem:{dest_config.get('path', 'unknown')}"
        elif dest_type == 'sftp':
            return f"sftp:{dest_config.get('host', 'unknown')}:{dest_config.get('path', '')}"
        elif dest_type == 'webdav':
            return f"webdav:{dest_config.get('url', 'unknown')}"
        return f"{dest_type}:unknown"

    def check_rclone_remote_configured(self, remote_name: str) -> bool:
        """Check if an rclone remote is configured.

        Args:
            remote_name: Name of the remote (e.g., 'onedrive')

        Returns:
            True if remote is configured, False otherwise.
        """
        try:
            creation_flags = 0x08000000 if sys.platform == "win32" else 0
            result = subprocess.run(
                [RCLONE_EXE, "listremotes"],
                capture_output=True,
                text=True,
                encoding='utf-8',
                creationflags=creation_flags
            )
            if result.returncode != 0:
                return False
            configured_remotes = [line.rstrip(':') for line in result.stdout.strip().split('\n') if line]
            return remote_name in configured_remotes
        except FileNotFoundError:
            if 'rclone' not in self._warned_remotes:
                logging.error("rclone not found. Install from https://rclone.org/downloads/")
                self._warned_remotes.add('rclone')
            return False
        except Exception as e:
            logging.error(f"Error checking rclone remotes: {e}")
            return False

    @staticmethod
    def _normalize_remote_path(path: str) -> str:
        """Normalize remote path for comparison.

        Strips trailing slashes, quotes, and handles case for consistent matching.
        """
        # Strip quotes that might wrap the path
        path = path.strip('"\'')
        # Strip trailing slashes
        path = path.rstrip('/')
        # On Windows, rclone remote names are case-insensitive
        # But the path after : may be case-sensitive depending on remote
        # For safety, we'll do case-insensitive compare on Windows
        if sys.platform == "win32":
            path = path.lower()
        return path

    def _path_in_cmdline(self, cmdline: str, remote_path: str) -> bool:
        """Check if remote_path appears in rclone webdav serve command.

        Kopia spawns rclone as: rclone -v serve webdav <remote-path> --addr ...
        We look specifically for this pattern to avoid false matches.

        Args:
            cmdline: The command line string to search
            remote_path: The normalized remote path to find

        Returns:
            True if path found in rclone serve webdav command
        """
        import re

        # Escape special regex characters in the path
        escaped_path = re.escape(remote_path)

        # Pattern matches: serve webdav <path> (with optional quotes around path)
        # The path must be followed by whitespace or end of string (not more path segments)
        # Format: rclone [-v] serve webdav <remote:path> [--addr ...]
        if sys.platform == "win32":
            # Windows: only double quotes, case-insensitive
            pattern = rf'serve\s+webdav\s+"?({escaped_path})"?(?:\s|$)'
            flags = re.IGNORECASE
        else:
            # Linux/Unix: single or double quotes, case-sensitive
            pattern = rf'serve\s+webdav\s+["\']?({escaped_path})["\']?(?:\s|$)'
            flags = 0

        return bool(re.search(pattern, cmdline, flags))

    def is_sync_already_running(self, dest_config: Dict[str, Any]) -> Tuple[bool, str]:
        """Check if a sync is already running for this destination.

        For rclone backends, checks for rclone processes with matching remote path.
        Uses normalized path matching to distinguish between different sync jobs.

        Args:
            dest_config: Destination configuration dict

        Returns:
            (is_running, description) tuple
        """
        dest_type = dest_config.get('type', '')

        if dest_type != 'rclone':
            # Only check for rclone backends for now
            return False, ""

        remote_path = dest_config.get('remote-path', '')
        if not remote_path:
            return False, ""

        # Normalize for comparison
        normalized_path = self._normalize_remote_path(remote_path)

        try:
            creation_flags = 0x08000000 if sys.platform == "win32" else 0

            if sys.platform == "win32":
                # Use PowerShell Get-CimInstance (WMIC is deprecated in Windows 11)
                result = subprocess.run(
                    ["powershell", "-Command",
                     "Get-CimInstance Win32_Process -Filter \"Name='rclone.exe'\" | Select-Object -ExpandProperty CommandLine"],
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    creationflags=creation_flags
                )
                if result.returncode == 0:
                    for line in result.stdout.strip().split('\n'):
                        # Look for rclone serving this exact remote path
                        if line.strip() and 'serve' in line.lower():
                            if self._path_in_cmdline(line, normalized_path):
                                return True, f"rclone already syncing to {remote_path}"
            else:
                # Unix: use ps with full command
                result = subprocess.run(
                    ["ps", "aux"],
                    capture_output=True,
                    text=True,
                    encoding='utf-8'
                )
                if result.returncode == 0:
                    for line in result.stdout.strip().split('\n'):
                        if 'rclone' in line and 'serve' in line.lower():
                            if self._path_in_cmdline(line, normalized_path):
                                return True, f"rclone already syncing to {remote_path}"

            return False, ""

        except Exception as e:
            logging.debug(f"Error checking for running syncs: {e}")
            return False, ""

    def build_sync_command(
        self,
        dest_config: Dict[str, Any],
        config_file: str,
        password: str,
        dry_run: bool = False
    ) -> Tuple[Optional[List[str]], Optional[str]]:
        """Build the kopia repository sync-to command.

        Args:
            dest_config: Destination configuration dict with 'type' and backend-specific params
            config_file: Path to kopia config file
            password: Repository password
            dry_run: If True, add --dry-run flag

        Returns:
            (command_list, error_message) - command_list is None if there's an error
        """
        dest_type = dest_config.get('type')
        if not dest_type:
            return None, "Missing 'type' in sync-to destination"

        if dest_type not in self.BACKEND_PARAMS:
            return None, f"Unknown sync-to type: {dest_type}"

        backend_info = self.BACKEND_PARAMS[dest_type]

        # Check required parameters
        missing = [p for p in backend_info['required'] if p not in dest_config]
        if missing:
            return None, f"Missing required parameters for {dest_type}: {', '.join(missing)}"

        # Build command
        cmd = [
            KOPIA_EXE,
            f"--config-file={config_file}",
            f"--password={password}",
            "repository", "sync-to", dest_type
        ]

        # Add required backend parameters
        for param, prefix in zip(backend_info['required'], backend_info['cmd_prefix']):
            cmd.append(f"{prefix}{dest_config[param]}")

        # Add default flags
        cmd.append("--delete")  # Mirror behavior

        # Add --flat for cloud backends
        if dest_type in self.CLOUD_BACKENDS:
            cmd.append("--flat")

        # Workaround for kopia/rclone startup detection issue:
        # Kopia detects rclone startup by parsing its stderr for specific strings.
        # Recent rclone versions may not output these by default, causing timeout.
        # --rclone-debug enables verbose rclone output that kopia can detect.
        if dest_type == 'rclone':
            cmd.append("--rclone-debug")

        # Add dry-run if requested
        if dry_run:
            cmd.append("--dry-run")

        # Handle sync-args based on backend type
        # For rclone: args are passed via --rclone-args=<arg> (e.g., --tpslimit for rate limiting)
        # For other backends: args go directly to kopia sync-to command
        sync_args = dest_config.get('sync-args', [])
        if sync_args:
            if dest_type == 'rclone':
                for arg in sync_args:
                    cmd.append(f"--rclone-args={arg}")
            else:
                cmd.extend(sync_args)

        return cmd, None

    def sync(
        self,
        dest_config: Dict[str, Any],
        config_file: str,
        password: str,
        quiet: bool = False,
        dry_run: bool = False
    ) -> Tuple[bool, str]:
        """Sync repository to a destination using kopia repository sync-to.

        Args:
            dest_config: Destination configuration dict
            config_file: Path to kopia config file
            password: Repository password
            quiet: If True, minimal output (for scheduled runs)
            dry_run: If True, simulate without making changes

        Returns:
            (success, error_message) tuple
        """
        dest_type = dest_config.get('type', 'unknown')

        # Check if sync is already running for this destination
        is_running, running_msg = self.is_sync_already_running(dest_config)
        if is_running:
            return False, f"Skipped: {running_msg} (letting previous sync finish)"

        # For rclone backend, verify remote is configured
        if dest_type == 'rclone':
            remote_path = dest_config.get('remote-path', '')
            remote_name = remote_path.split(':')[0] if ':' in remote_path else remote_path
            if not self.check_rclone_remote_configured(remote_name):
                return False, f"rclone remote '{remote_name}' not configured"

        # Build command
        cmd, error = self.build_sync_command(dest_config, config_file, password, dry_run)
        if cmd is None or error:
            return False, error or "Unknown error building command"

        # Mask password in logs
        cmd_display = [c if not c.startswith('--password=') else '--password=***' for c in cmd]
        logging.debug(f"Executing: {subprocess.list2cmdline(cmd_display)}")

        if dry_run and not quiet:
            print(f"[DRY-RUN] Would execute: {subprocess.list2cmdline(cmd_display)}")

        try:
            creation_flags = 0x08000000 if sys.platform == "win32" else 0

            if quiet:
                # Scheduled mode: capture output, hide window
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
                    error_msg = result.stderr.strip() or result.stdout.strip() or f"kopia exited with code {result.returncode}"
                    return False, error_msg
            else:
                # Interactive mode: let output flow to terminal
                result = subprocess.run(cmd)
                if result.returncode == 0:
                    return True, ""
                else:
                    return False, f"kopia exited with code {result.returncode}"

        except KeyboardInterrupt:
            logging.warning("Sync interrupted")
            return False, "Sync interrupted by user"
        except FileNotFoundError:
            return False, "kopia not found in PATH"
        except Exception as e:
            return False, str(e)

    @staticmethod
    def get_setup_instructions(dest_type: str) -> str:
        """Get setup instructions for a destination type.

        Args:
            dest_type: Type of destination (e.g., 'rclone', 's3')

        Returns:
            Multi-line instruction string.
        """
        if dest_type == 'rclone':
            return """
To set up rclone:
  1. Install: https://rclone.org/downloads/
  2. Run: rclone config
  3. Follow the auth flow for your provider
  4. Test with: rclone lsd <remote>:
"""
        return f"See: kopia repository sync-to {dest_type} --help"


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
    dest_id: str,
    success: bool,
    error: Optional[str] = None
) -> None:
    """Update the cloud sync status for a repository destination.

    Args:
        repo_name: Name of the repository
        dest_id: Destination identifier (e.g., 'rclone:onedrive:mybackups')
        success: Whether the sync succeeded
        error: Error message if sync failed
    """
    status = _load_status_file()
    if "cloud_sync" not in status:
        status["cloud_sync"] = {}

    # Key is now repo_name:dest_id to support multiple destinations
    key = f"{repo_name}:{dest_id}"
    status["cloud_sync"][key] = {
        "last_sync": datetime.now(timezone.utc).isoformat(),
        "success": success,
        "dest_id": dest_id,
        "error": error
    }
    _save_status_file(status)


def get_sync_status_for_destination(repo_name: str, dest_id: str) -> Optional[Dict[str, Any]]:
    """Get the sync status for a specific destination.

    Args:
        repo_name: Name of the repository
        dest_id: Destination identifier

    Returns:
        Status dict or None if not found.
    """
    cloud_status = load_cloud_sync_status()
    key = f"{repo_name}:{dest_id}"
    return cloud_status.get(key)


def cleanup_status_file(valid_repo_names: List[str]) -> None:
    """Remove status entries for repos that no longer exist in config.

    Args:
        valid_repo_names: List of repo names currently in config.
    """
    status = _load_status_file()
    changed = False

    if "cloud_sync" in status:
        # Keys are composite: "repo_name:dest_id" - check if repo_name prefix is valid
        def is_valid_key(key: str) -> bool:
            for repo_name in valid_repo_names:
                if key == repo_name or key.startswith(f"{repo_name}:"):
                    return True
            return False

        stale_keys = [key for key in status["cloud_sync"] if not is_valid_key(key)]
        for key in stale_keys:
            del status["cloud_sync"][key]
            logging.debug(f"Cleaned up stale status for '{key}'")
            changed = True

    if changed:
        _save_status_file(status)
