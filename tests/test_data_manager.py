import unittest
from pathlib import Path
import tempfile
import yaml
from datatc.data_manager import DataDirectoryManager


class TestDataDirectoryManager(unittest.TestCase):

    def test_register_project_to_empty_file(self):
        project_hint = 'test_project'
        project_path = Path('test_path')

        expected_config = {'test_project': {'path': 'test_path'}}

        with tempfile.NamedTemporaryFile() as f:
            config_file_path = Path(f.name)
            DataDirectoryManager._register_project_to_file(project_hint, project_path, config_file_path)

            config = yaml.safe_load(open(f.name))
            self.assertEqual(config, expected_config)
