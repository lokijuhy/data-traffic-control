import unittest
import tempfile
import pandas as pd
import shutil
from datatc.data_interface import MagicDataInterface, TestingDataInterface


class TestDataInterface(unittest.TestCase):

    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()

        # establish a dataframe
        self.raw_df = pd.DataFrame({'col_1': range(50), 'col_2': range(0, 100, 2)})

    def tearDown(self):
        # Remove the directory after the test
        shutil.rmtree(self.test_dir)

    def test_construct_file_path_name_without_extension(self):
        file_name = 'file'
        file_dir_path = '/home'
        expected_result = '/home/file.test'
        self.assertEqual(TestingDataInterface.construct_file_path(file_name, file_dir_path), expected_result)

    def test_construct_file_path_name_with_extension(self):
        file_name = 'file.yaml'
        file_dir_path = '/home'
        expected_result = '/home/file.yaml'
        self.assertEqual(TestingDataInterface.construct_file_path(file_name, file_dir_path), expected_result)

    def test_data_interface_save(self):
        p = self.test_dir + 'test_save.csv'
        MagicDataInterface.save(self.raw_df, p, index=False)
        reloaded_data = pd.read_csv(p)
        pd.testing.assert_frame_equal(self.raw_df, reloaded_data)

    def test_data_interface_load(self):
        p = self.test_dir + 'test_load.csv'
        self.raw_df.to_csv(p, index=False)
        reloaded_data = MagicDataInterface.load(p)
        pd.testing.assert_frame_equal(self.raw_df, reloaded_data)
