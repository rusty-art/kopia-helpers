import pytest
import os
import sys
import kopia_utils as utils

# Import scripts
import importlib.util
def import_script(name):
    file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), name)
    spec = importlib.util.spec_from_file_location("module.name", file_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

kopia_start = import_script("kopia-start-backups.py")
kopia_find = import_script("kopia-find-files.py")

class TestIntegration:
    def test_backup_lifecycle(self, temp_repo):
        """Test init, backup, and find flow."""
        runner = utils.KopiaRunner(skip_config=True)
        repo_config = temp_repo['config']
        source_dir = temp_repo['source_dir']
        
        # 1. Create a file in source
        test_file = os.path.join(source_dir, "test_doc.txt")
        with open(test_file, "w") as f:
            f.write("Hello Kopia")
            
        # 2. Run Backup (this should init repo and snapshot)
        # We use ensure_repository_connected to init
        connected = kopia_start.ensure_repository_connected(runner, repo_config)
        assert connected, "Failed to connect/init repository"
        
        # Run actual backup
        kopia_start.run_backup_job(runner, repo_config)
        
        # 3. Verify Snapshot exists
        success, stdout, _, _ = runner.run(
            ["snapshot", "list", "--json", "--config-file", repo_config['local_config_file_path']],
            repo_config=repo_config
        )
        assert success
        assert "test_doc.txt" in stdout or "source" in stdout
        
        # 4. Test Find Files
        matches = kopia_find.find_in_repo(runner, repo_config, "*.txt")
        assert len(matches) >= 1
        assert matches[0]['path'].endswith("test_doc.txt")
        assert matches[0]['size'] == 11  # "Hello Kopia" is 11 bytes

    def test_incremental_backup(self, temp_repo):
        runner = utils.KopiaRunner(skip_config=True)
        repo_config = temp_repo['config']
        source_dir = temp_repo['source_dir']
        
        # Init
        kopia_start.ensure_repository_connected(runner, repo_config)
        
        # 1. First backup
        file1 = os.path.join(source_dir, "file1.txt")
        with open(file1, "w") as f: f.write("v1")
        kopia_start.run_backup_job(runner, repo_config)
        
        # 2. Modify file
        with open(file1, "w") as f: f.write("v2-modified")
        kopia_start.run_backup_job(runner, repo_config)
        
        # 3. Find versions
        matches = kopia_find.find_in_repo(runner, repo_config, "file1.txt")
        assert len(matches) == 2
        sizes = sorted([m['size'] for m in matches])
        assert sizes == [2, 11]  # v1=2 bytes, v2=11 bytes
