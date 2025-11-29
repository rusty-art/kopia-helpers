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
