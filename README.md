# Kopia Backup Scripts

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Tests](https://github.com/rusty-art/kopia-helpers/actions/workflows/tests.yml/badge.svg)](https://github.com/rusty-art/kopia-helpers/actions/workflows/tests.yml)

**Multi-repository backup automation for [Kopia](https://kopia.io/).**

> Currently Windows only, but could be adapted for Linux/macOS.

## Why use this instead of Kopia directly?

Kopia is excellent, but its CLI works with one repository at a time. These scripts solve the "multiple backup destinations" problem:

- **One command, all repositories** — Back up to local NAS, cloud storage, and external drive simultaneously
- **Single YAML config** — All your repositories, sources, and policies in one readable, versionable file
- **Set-and-forget scheduling** — Auto-registers with Windows Task Scheduler
- **Health monitoring** — Toast notifications if backups haven't run in 7 days

If you only have one backup destination, just use Kopia directly. If you follow the 3-2-1 backup rule (3 copies, 2 media types, 1 offsite), this makes your life easier.

## Scripts

| Script | Description |
|--------|-------------|
| `kopia-start-backups.py` | Main backup script - creates snapshots, sets policies, schedules in task-scheduler |
| `kopia-stop-backups.py` | Unregister scheduled tasks (backups and/or health checks) |
| `kopia-health-check.py` | Alert if no backups in 7 days (toast notification), schedules in task-scheduler |
| `kopia-find-files.py` | Search for files across all snapshots |

## Setup

1. Install [Kopia](https://kopia.io/docs/installation/)
2. Copy `kopia-helpers.template.yaml` to `kopia-helpers.yaml`
3. Edit `kopia-helpers.yaml` with your repository paths and sources
4. Set your password (see below)
5. Run `python kopia-start-backups.py` as Administrator
6. Run `python kopia-health-check.py --register` to enable backup monitoring

Your `kopia-helpers.yaml` is re-read each time the scheduled task runs, so config changes take effect automatically. You can also safely re-run steps 5 and 6 anytime to re-register the tasks (but not required). Uou will receive a Windows Toast Notification if there are backup failures or lack of backup activity. 

## Password Configuration

Passwords can be set in multiple ways (checked in this order):

### Option 1: In config file (simplest)
```yaml
# kopia-helpers.yaml
repositories:
  - name: my-backup
    password: your-password-here
    ...
```

### Option 2: Environment variable (safest)
```bash
# Repository-specific (recommended)  
set KOPIA_PASSWORD_MY_BACKUP1=your-password

# Or global fallback (kopia default)
set KOPIA_PASSWORD=your-password
```

### Option 3: .env.local file (git-ignored)
```bash
# .env.local
KOPIA_PASSWORD_MY_BACKUP1=your-password
```
**Note:** The variable name is based on the repository `name` in your `kopia-helpers.yaml`, converted to uppercase with dashes replaced by underscores.
Example: `name: my-backup1` → `KOPIA_PASSWORD_MY_BACKUP1`

## Scheduled Backups

Run as Administrator to auto-register with Windows Task Scheduler:
```
python kopia-start-backups.py
```

This creates a task that runs every 15 minutes (configurable in kopia-helpers.yaml).

## Health Check

Register the health check to alert if backups stop:
```
python kopia-health-check.py --register
```

This checks every 3 hours and shows a toast notification if no backups for 7 days (configurable in kopia-helpers.yaml).

## Syncing to Cloud / Remote Storage

After Kopia creates local snapshots, you can sync them to cloud storage or other destinations using Kopia's built-in `repository sync-to` command. This properly handles repository structure (sharding) and supports multiple destinations per repository.

### How it works

1. Kopia writes snapshots to a local directory repository
2. After backup completes, `kopia repository sync-to` syncs to each configured destination
3. Each destination can have its own sync interval

### Supported backends

| Backend | Description | Setup Required |
|---------|-------------|----------------|
| `rclone` | OneDrive, Dropbox, and [50+ providers](https://rclone.org/overview/) | [rclone](https://rclone.org/downloads/) installed + configured |
| `s3` | Amazon S3 and S3-compatible storage | Access keys |
| `gcs` | Google Cloud Storage | GCP credentials |
| `azure` | Azure Blob Storage | Storage account + key |
| `b2` | Backblaze B2 | Application key |
| `gdrive` | Google Drive (native) | OAuth credentials |
| `filesystem` | Local/network paths | None |
| `sftp` | SSH/SFTP servers | SSH access |
| `webdav` | WebDAV servers | URL + credentials |

### Configuration

Add a `sync-to` list to your repository in `kopia-helpers.yaml`:

```yaml
repositories:
  - name: my-backup
    repo_destination: C:/kopia-cache/mysrc
    repo_config: C:/kopia-cache/mysrc/repository.config
    repo_password: your-password

    sources:
      - C:/Users/username/Documents

    policies:
      # ... retention policies ...

    # Sync to one or more destinations
    sync-to:
      # OneDrive via rclone
      - type: rclone
        remote-path: onedrive:mybackups/kopia
        interval: 60m

      # Local NAS
      - type: filesystem
        path: //nas/backups/kopia
        interval: 30m
```

### Default flags

These flags are always applied (you can override via `extra-args`):
- `--delete` - Mirror behavior (remove files not in source)
- `--flat` - Flat directory structure for cloud backends

### Using rclone backend (OneDrive, Dropbox, etc.)

1. Install rclone: https://rclone.org/downloads/
2. Configure your remote:
   ```bash
   rclone config
   # Choose 'n' for new remote, name it 'onedrive', follow auth flow
   ```
3. Test: `rclone lsd onedrive:`
4. Add to config:
   ```yaml
   sync-to:
     - type: rclone
       remote-path: onedrive:mybackups/kopia
       interval: 60m
   ```

### Using S3 backend

```yaml
sync-to:
  - type: s3
    bucket: my-backup-bucket
    interval: 120m
    extra-args: ["--access-key=AKIAXXXXXXXX", "--secret-access-key=xxx"]
```

### YAML parameters vs extra-args

Each backend has a few **required YAML fields** (the destination identifier). Everything else goes in `extra-args`:

| Backend | Required YAML fields | Example extra-args |
|---------|---------------------|-------------------|
| `rclone` | `remote-path` | `--rclone-args=...` |
| `s3` | `bucket` | `--access-key=...`, `--region=...` |
| `gcs` | `bucket` | `--credentials-file=...` |
| `azure` | `container`, `storage-account` | `--storage-key=...` |
| `b2` | `bucket` | `--key-id=...`, `--key=...` |
| `gdrive` | `folder-id` | `--credentials-file=...` |
| `filesystem` | `path` | |
| `sftp` | `path`, `host`, `username` | `--keyfile=...`, `--password=...` |
| `webdav` | `url` | `--username=...`, `--password=...` |

**Common fields for all backends:**
- `interval` - how often to sync (e.g., `60m`, `2h`)
- `extra-args` - list of additional kopia flags

### Using extra-args

```yaml
sync-to:
  - type: rclone
    remote-path: onedrive:mybackups/kopia
    interval: 60m
    extra-args: ["--no-delete", "--times"]

  - type: s3
    bucket: my-bucket
    interval: 120m
    extra-args: ["--access-key=AKIA...", "--secret-access-key=...", "--region=us-west-2"]

  - type: sftp
    path: /backups/kopia
    host: backup.example.com
    username: backupuser
    interval: 30m
    extra-args: ["--keyfile=/path/to/key"]
```

To see all available options for a backend:
```bash
kopia repository sync-to <type> --help
```

### rclone process cleanup

When using the rclone backend, kopia spawns `rclone serve webdav` processes to communicate with cloud storage. These processes are sometimes left running after kopia exits. The backup script automatically cleans up orphaned kopia-rclone processes:

- After each successful or failed rclone sync
- On Ctrl-C interrupt
- Only kills processes with `kopia-rclone` in their command line (won't affect other rclone servers you may be running)

### Concurrent sync protection

If a sync is already running to a destination (e.g., from a previous backup run), the script will skip that sync rather than starting a conflicting one. This prevents issues when:
- You run the script manually while a scheduled task is already running
- A slow sync from the previous run is still in progress

## Syncing from Remote Sources (sync-from)

You can pull files from a remote location *before* kopia creates a snapshot. This is useful for:
- Backing up WSL/Linux files to a Windows-accessible location
- Pulling files from a NAS before backup
- Creating a local copy of remote files for faster kopia access

### Configuration

Add a `sync-from` list to your repository:

```yaml
repositories:
  - name: linux-backup
    repo_destination: C:/kopia-cache/linux
    repo_config: C:/kopia-cache/linux/repository.config

    sources:
      - C:/local-copy/linux-projects  # Kopia backs up this local copy

    # Pull from remote BEFORE backup
    sync-from:
      - type: rclone
        source: //wsl.localhost/Ubuntu/home/user/projects
        destination: C:/local-copy/linux-projects
        sync-args:
          - "--ignore-case-sync"  # Handle case-sensitive filenames
```

### How it works

1. Before kopia runs, `rclone sync` copies from `source` to `destination`
2. Kopia then backs up the `destination` directory (listed in `sources`)
3. This gives you a local Windows copy plus versioned kopia backups

### sync-from options

| Field | Description |
|-------|-------------|
| `type` | Currently only `rclone` is supported |
| `source` | Remote path (rclone remote or UNC path) |
| `destination` | Local path to sync to |
| `sync-args` | List of extra rclone arguments |

### Respecting ignore files

sync-from respects `.kopiaignore` files in the **top-level** source directory. Due to rclone limitations, nested ignore files are not parsed recursively (unlike kopia/git).

```yaml
policies:
  dot-ignore:
    - ".kopiaignore"  # Also used by sync-from for exclusions
```

## Finding Files

Search for files across all snapshots using `find -name` style patterns:

```bash
python kopia-find-files.py "*.py"              # Find all .py files (not .pyc)
python kopia-find-files.py "report*.pdf"       # Files starting with 'report'
python kopia-find-files.py "config.yaml"       # Exact filename match
python kopia-find-files.py "data_202[0-9].csv" # Character range: 2020-2029
```

Pattern syntax (same as `find -name`):
| Pattern | Matches |
|---------|---------|
| `*` | Any characters (zero or more) |
| `?` | Exactly one character |
| `[abc]` | One of: a, b, or c |
| `[a-z]` | One character in range a-z |

Options:
- `-n 50` — Search last 50 snapshots (default: 100)
- `--all` — Search all snapshots
- `--path` — Match against full path (like `find -path`) instead of filename
- `--repo name1,name2` — Search specific repositories
- `--mount` — Mount repository after search for browsing

## Common Options

All scripts support:
- `--log-level DEBUG|INFO|WARNING|ERROR` (default: WARNING)
- `--dry-run` (for backup script)

## Dependencies

- Python 3.8+
- PyYAML: `pip install pyyaml`
- python-dotenv (optional): `pip install python-dotenv`
- rclone (for OneDrive/Dropbox/etc sync): https://rclone.org/downloads/

## License

MIT License - see [LICENSE](LICENSE) for details.
