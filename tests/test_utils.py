import pytest
import os
import sys
import importlib.util
from datetime import datetime

# Import kopia_utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import kopia_utils as utils

# Dynamically import scripts to test their parsing logic
def import_script(name):
    file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), name)
    spec = importlib.util.spec_from_file_location("module.name", file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

kopia_health = import_script("kopia-health-check.py")
kopia_find = import_script("kopia-find-files.py")

class TestUtils:
    def test_format_timestamp_utc(self):
        ts = "2025-11-22T05:04:01Z"
        formatted = utils.format_timestamp_utc(ts)
        assert formatted == "2025-11-22 05:04:01Z"

    def test_format_kopia_verbose_timestamp(self):
        # Test regex parsing
        ts = "2025-11-22 16:09:30.2325872 +1100 AEDT"
        clean = utils.format_kopia_verbose_timestamp(ts)
        assert clean == "2025-11-22 16:09:30 +1100"
        
        # Test fallback
        ts_simple = "2025-11-22 16:09:30 +1100"
        clean_simple = utils.format_kopia_verbose_timestamp(ts_simple)
        assert clean_simple == "2025-11-22 16:09:30 +1100"

    def test_get_password(self):
        runner = utils.KopiaRunner(skip_config=True)
        
        # Test explicit config
        config = {'name': 'test', 'repo_password': 'explicit-pass'}
        assert runner.get_password(config) == 'explicit-pass'
        
        # Test ENV var
        os.environ['KOPIA_PASSWORD_TEST_REPO'] = 'env-pass'
        config_env = {'name': 'test-repo'}
        assert runner.get_password(config_env) == 'env-pass'
        del os.environ['KOPIA_PASSWORD_TEST_REPO']

class TestParsing:
    def test_parse_diff_line_changed(self):
        line = "changed ./my/file.txt at 2025-11-22 16:09:30.2325872 +1100 AEDT (size 100 -> 200)"
        result = kopia_health.parse_diff_line(line)
        assert result is not None
        change_type, path, formatted = result
        assert change_type == 'changed'
        assert path == './my/file.txt'
        assert 'c 2025-11-22 16:09:30 +1100' in formatted
        assert '200' in formatted

    def test_parse_diff_line_added(self):
        line = "added file ./new/file.txt (1234 bytes)"
        result = kopia_health.parse_diff_line(line)
        assert result is not None
        change_type, path, formatted = result
        assert change_type == 'added'
        assert path == './new/file.txt'
        assert '+' in formatted
        assert '1234' in formatted

    def test_parse_ls_line(self):
        # -rw-rw-rw-   24155 2025-11-22 21:39:58 AEDT 749004d41187bf2322037fecbb5022af   kopia/file.py
        line = "-rw-rw-rw-   24155 2025-11-22 21:39:58 AEDT 749004d41187bf2322037fecbb5022af   kopia/file.py"
        result = kopia_find.parse_ls_line(line)
        assert result is not None
        size, modified_str, path = result
        assert size == 24155
        assert modified_str == "2025-11-22 21:39:58 AEDT"
        assert path == "kopia/file.py"


class TestSyncRunner:
    """Tests for KopiaSyncRunner sync command building."""

    def test_build_sync_command_rclone_has_debug_flag(self):
        """Verify --rclone-debug is added for rclone backends (kopia#2573 workaround)."""
        runner = utils.KopiaSyncRunner()
        config = {'type': 'rclone', 'remote-path': 'onedrive:test/path'}
        cmd, err = runner.build_sync_command(config, 'test.config', 'secret')

        assert err is None
        assert '--rclone-debug' in cmd
        assert 'rclone' in cmd
        assert '--remote-path=onedrive:test/path' in cmd

    def test_build_sync_command_rclone_sync_args(self):
        """Verify sync-args for rclone are passed via --rclone-args=<arg>."""
        runner = utils.KopiaSyncRunner()
        config = {
            'type': 'rclone',
            'remote-path': 'onedrive:test/path',
            'sync-args': ['--tpslimit=8', '--tpslimit-burst=8']
        }
        cmd, err = runner.build_sync_command(config, 'test.config', 'secret')

        assert err is None
        # Verify args are passed via --rclone-args=<arg> format
        assert '--rclone-args=--tpslimit=8' in cmd
        assert '--rclone-args=--tpslimit-burst=8' in cmd

    def test_build_sync_command_filesystem_no_rclone_debug(self):
        """Verify --rclone-debug is NOT added for non-rclone backends."""
        runner = utils.KopiaSyncRunner()
        config = {'type': 'filesystem', 'path': '/backup/path'}
        cmd, err = runner.build_sync_command(config, 'test.config', 'secret')

        assert err is None
        assert '--rclone-debug' not in cmd
        assert 'filesystem' in cmd

    def test_build_sync_command_filesystem_sync_args(self):
        """Verify sync-args for non-rclone backends go directly to kopia."""
        runner = utils.KopiaSyncRunner()
        config = {
            'type': 'filesystem',
            'path': '/backup/path',
            'sync-args': ['--parallel=4']
        }
        cmd, err = runner.build_sync_command(config, 'test.config', 'secret')

        assert err is None
        # Verify arg goes directly to kopia (not via --rclone-args)
        assert '--parallel=4' in cmd
        assert '--rclone-args' not in cmd

    def test_build_sync_command_has_delete_and_flat(self):
        """Verify cloud backends get --delete and --flat flags."""
        runner = utils.KopiaSyncRunner()
        config = {'type': 'rclone', 'remote-path': 'onedrive:test'}
        cmd, err = runner.build_sync_command(config, 'test.config', 'secret')

        assert err is None
        assert '--delete' in cmd
        assert '--flat' in cmd

    def test_is_sync_already_running_non_rclone(self):
        """Verify non-rclone backends are not checked for running syncs."""
        runner = utils.KopiaSyncRunner()
        config = {'type': 'filesystem', 'path': '/backup/path'}
        is_running, msg = runner.is_sync_already_running(config)

        assert is_running is False
        assert msg == ""

    def test_is_sync_already_running_no_process(self):
        """Verify returns False when no matching rclone process is running."""
        runner = utils.KopiaSyncRunner()
        # Use a unique path that won't match any running process
        config = {'type': 'rclone', 'remote-path': 'fake-remote-12345:nonexistent/path'}
        is_running, msg = runner.is_sync_already_running(config)

        assert is_running is False

    def test_normalize_remote_path(self):
        """Verify path normalization handles trailing slashes and quotes."""
        runner = utils.KopiaSyncRunner()

        # Trailing slashes stripped
        assert runner._normalize_remote_path('onedrive:path/') == runner._normalize_remote_path('onedrive:path')

        # Quotes stripped
        assert runner._normalize_remote_path('"onedrive:path"') == runner._normalize_remote_path('onedrive:path')
        assert runner._normalize_remote_path("'onedrive:path'") == runner._normalize_remote_path('onedrive:path')

        # On Windows, case is normalized (case-insensitive but case-preserving filesystem)
        import sys
        if sys.platform == "win32":
            assert runner._normalize_remote_path('OneDrive:Path') == runner._normalize_remote_path('onedrive:path')

    def test_path_in_cmdline_variations(self):
        """Verify _path_in_cmdline handles quoted and unquoted paths."""
        runner = utils.KopiaSyncRunner()
        path = runner._normalize_remote_path('onedrive:mybackups/kopia')

        # Unquoted path (case may differ on Windows due to case-preserving behavior)
        assert runner._path_in_cmdline('rclone serve webdav onedrive:mybackups/kopia --addr 127.0.0.1', path)
        assert runner._path_in_cmdline('rclone serve webdav OneDrive:MyBackups/Kopia --addr 127.0.0.1', path)

        # Path at end of line
        assert runner._path_in_cmdline('rclone serve webdav onedrive:mybackups/kopia', path)

        # Quoted path
        assert runner._path_in_cmdline('rclone serve webdav "onedrive:mybackups/kopia" --addr 127.0.0.1', path)

        # Different path should NOT match
        assert not runner._path_in_cmdline('rclone serve webdav onedrive:mybackups/other --addr', path)

        # CRITICAL: Path must NOT match if it's a PREFIX of the actual path!
        # e.g., 'onedrive:mybackups/kopia' must NOT match 'onedrive:mybackups/kopia/subdir'
        assert not runner._path_in_cmdline('rclone serve webdav onedrive:mybackups/kopia/subdir --addr', path)
        assert not runner._path_in_cmdline('rclone serve webdav onedrive:mybackups/kopia-other --addr', path)

        # And the reverse: subdir path should NOT match parent
        subdir_path = runner._normalize_remote_path('onedrive:mybackups/kopia/subdir')
        assert not runner._path_in_cmdline('rclone serve webdav onedrive:mybackups/kopia --addr', subdir_path)

        # Test with actual kopia-spawned rclone command line format
        real_cmdline = 'rclone -v serve webdav onedrive:mybackups/kopia/kopia-mysrc --addr 127.0.0.1:0 --rc --rc-addr 127.0.0.1:0'
        real_path = runner._normalize_remote_path('onedrive:mybackups/kopia/kopia-mysrc')
        assert runner._path_in_cmdline(real_cmdline, real_path)

        # Different repo should NOT match
        other_path = runner._normalize_remote_path('onedrive:mybackups/kopia/kopia-other')
        assert not runner._path_in_cmdline(real_cmdline, other_path)
