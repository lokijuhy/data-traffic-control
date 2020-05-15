import unittest
from datatc.data_interface import TestingDataInterface


class TestDataInterface(unittest.TestCase):

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
