import unittest
from io import StringIO
import tempfile
from datatc.data_manager import DataManager


class TestDataManager(unittest.TestCase):

    def test_register_project_to_empty_file(self):
        project_hint = 'test_project'
        project_path = 'test_path'

        expected_file_contents = """
        discern:
            path: ~/switchdrive/Institution/discern
        """

        with tempfile.NamedTemporaryFile() as f:
            config_file_path = f.name
            DataManager._register_project_to_file(project_hint, project_path, config_file_path)

            f.seek(0)
            file_contents = f.read()
            self.assertEqual(file_contents, expected_file_contents)




# config_file_handle = StringIO()
#
# DataManager._register_project_to_file(project_hint, project_path, config_file_handle)
# config_file_handle.seek(0)
# content = config_file_handle.read()
# self.assertEqual(content, "Mary had a little lamb.\n")
