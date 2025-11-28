# Kopia Backup Scripts

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

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
| `kopia-start-backups.py` | Main backup script - creates snapshots, sets policies, also schedules in task-scheduler |
| `kopia-health-check.py` | Alert if no backups in 7 days (toast notification), also schedules in task-scheduler |
| `kopia-check-backups.py` | Shows backup status and recent snapshots |
| `kopia-find-files.py` | Search for files across all snapshots |

## Setup

1. Install [Kopia](https://kopia.io/docs/installation/)
2. Copy `kopia-configs.template.yaml` to `kopia-configs.yaml`
3. Edit `kopia-configs.yaml` with your repository paths and sources
4. Set your password (see below)
5. Run `python kopia-start-backups.py` as Administrator
6. Run `python kopia-health-check.py --register` to enable backup monitoring

Your `kopia-configs.yaml` is re-read each time the scheduled task runs, so config changes take effect automatically. You can also safely re-run steps 5 and 6 anytime to re-register the tasks (but not required). Uou will receive a Windows Toast Notification if there are backup failures or lack of backup activity. 

## Password Configuration

Passwords can be set in multiple ways (checked in this order):

### Option 1: In config file (simplest)
```yaml
# kopia-configs.yaml
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
**Note:** The variable name is based on the repository `name` in your `kopia-configs.yaml`, converted to uppercase with dashes replaced by underscores.
Example: `name: my-backup1` → `KOPIA_PASSWORD_MY_BACKUP1`

## Scheduled Backups

Run as Administrator to auto-register with Windows Task Scheduler:
```
python kopia-start-backups.py
```

This creates a task that runs every 15 minutes (configurable in kopia-configs.yaml).

## Health Check

Register the health check to alert if backups stop:
```
python kopia-health-check.py --register
```

This checks every 3 hours and shows a toast notification if no backups for 7 days (configurable in kopia-configs.yaml).

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

## License

MIT License - see [LICENSE](LICENSE) for details.
