import unittest
import tempfile
import yaml
from datatc import DataManager


class TestDataManager(unittest.TestCase):

    def test_register_project_to_empty_file(self):
        project_hint = 'test_project'
        project_path = 'test_path'

        expected_config = {'test_project': {'path': 'test_path'}}

        with tempfile.NamedTemporaryFile() as f:
            config_file_path = f.name
            DataManager._register_project_to_file(project_hint, project_path, config_file_path)

            config = yaml.safe_load(open(f.name))
            self.assertEqual(config, expected_config)




# config_file_handle = StringIO()
#
# DataManager._register_project_to_file(project_hint, project_path, config_file_handle)
# config_file_handle.seek(0)
# content = config_file_handle.read()
# self.assertEqual(content, "Mary had a little lamb.\n")
