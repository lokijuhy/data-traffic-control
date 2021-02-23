import unittest
import pytest
from pathlib import Path
import tempfile
import yaml
import pandas as pd
import shutil
from datatc.data_directory import DataDirectoryManager, TestingDataDirectoryManager
from datatc.data_manager import DataManager


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


class TestDataManager(unittest.TestCase):

    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()

        # establish a dataframe
        self.raw_df = pd.DataFrame({'col_1': range(50), 'col_2': range(0, 100, 2)})

        # create a csv file
        self.raw_df.to_csv(Path(self.test_dir, 'test.csv'), index=False)

    def tearDown(self):
        # Remove the directory after the test
        shutil.rmtree(self.test_dir)

    def test_deprecation_warning(self):
        with pytest.deprecated_call():
            dm = DataManager(self.test_dir, data_dir_manager=TestingDataDirectoryManager)
            self.assertEqual(type(dm), DataManager)
