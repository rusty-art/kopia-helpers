import pytest
import os
import sys
import shutil
import tempfile
import subprocess
from unittest.mock import MagicMock, patch

# Add parent directory to path so we can import kopia_utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import kopia_utils as utils

@pytest.fixture
def temp_repo():
    """Create a temporary directory for a Kopia repository."""
    temp_dir = tempfile.mkdtemp(prefix="kopia_test_repo_")
    repo_path = os.path.join(temp_dir, "repo")
    config_file = os.path.join(temp_dir, "repository.config")
    source_dir = os.path.join(temp_dir, "source")
    
    os.makedirs(repo_path)
    os.makedirs(source_dir)
    
    repo_config = {
        'name': 'test-repo',
        'repo_destination': repo_path,
        'repo_config': config_file,
        'repo_password': 'test-password',
        'sources': [source_dir]
    }
    
    yield {
        'dir': temp_dir,
        'repo_path': repo_path,
        'config_file': config_file,
        'source_dir': source_dir,
        'config': repo_config
    }
    
    # Cleanup
    shutil.rmtree(temp_dir)

@pytest.fixture(autouse=True)
def mock_subprocess(monkeypatch):
    """Mock subprocess.run to prevent accidental system changes (Task Scheduler, Toasts)."""
    original_run = subprocess.run
    
    def side_effect(args, **kwargs):
        cmd = args if isinstance(args, list) else args.split()
        cmd_str = " ".join(cmd)
        
        # Block Task Scheduler commands
        if "schtasks" in cmd_str:
            print(f"MOCKED: {cmd_str}")
            return subprocess.CompletedProcess(args, 0, stdout="SUCCESS", stderr="")
            
        # Block PowerShell Toast Notifications
        if "powershell" in cmd_str and "ToastNotification" in cmd_str:
            print(f"MOCKED TOAST: {cmd_str}")
            return subprocess.CompletedProcess(args, 0, stdout="SUCCESS", stderr="")
            
        # Allow Kopia commands (we want to test them against temp repos)
        if "kopia" in cmd_str or cmd[0].endswith("kopia.exe") or cmd[0] == "kopia":
            return original_run(args, **kwargs)
            
        return original_run(args, **kwargs)
        
    monkeypatch.setattr(subprocess, "run", side_effect)
