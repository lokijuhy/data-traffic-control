import unittest
import pandas as pd
from pathlib import Path
import shutil
import tempfile
from datatc.self_aware_data import SelfAwareData, SelfAwareDataInterface


class TestSelfAwareData(unittest.TestCase):

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

    @staticmethod
    def transform_func(input_df):
        df = input_df.copy()
        df['col_1'] = df['col_1'] * 2
        return df

    @staticmethod
    def transform_func_with_kwarg(input_df, factor):
        df = input_df.copy()
        df['col_1'] = df['col_1'] * factor
        return df

    def test_transform(self):
        raw_sad = SelfAwareData(self.raw_df)
        my_sad = raw_sad.transform(self.transform_func, enforce_clean_git=False)
        manually_transformed_df = self.transform_func(self.raw_df)
        pd.testing.assert_frame_equal(my_sad.data, manually_transformed_df)

    def test_transform_with_kwarg(self):
        raw_sad = SelfAwareData(self.raw_df)
        my_factor = 2
        my_sad = raw_sad.transform(self.transform_func_with_kwarg, enforce_clean_git=False, factor=my_factor)
        manually_transformed_df = self.transform_func_with_kwarg(self.raw_df, my_factor)
        pd.testing.assert_frame_equal(my_sad.data, manually_transformed_df)

    def test_save_and_load(self):
        raw_sad = SelfAwareData(self.raw_df)
        my_sad = raw_sad.transform(self.transform_func, enforce_clean_git=False)

        # TEST SAVE
        sad_file_path = my_sad.save(Path(self.test_dir, 'new_sad.csv'), index=False)

        # assert a new directory was created, and the path matches the return value
        self.assertTrue(Path(sad_file_path).exists())

        # assert the sad directory contains 3 files
        sad_dir_contents = [f for f in Path(sad_file_path).iterdir()]
        self.assertEqual(len(sad_dir_contents), 3)

        # assert the sad dir contains a csv, dill, and txt file
        contents_extensions = [f.suffix.replace('.', '') for f in sad_dir_contents]
        for ext in ['csv', 'dill', 'txt']:
            self.assertTrue(ext in contents_extensions)

        # TEST LOAD
        reloaded_sad = SelfAwareData.load(sad_file_path)

        # assert the reloaded data is same as what was saved
        pd.testing.assert_frame_equal(reloaded_sad.data, my_sad.data)

        # assert view_code runs without error
        reloaded_sad.view_code()

        # assert rerun generates the same data if given the same input
        rerun_df = reloaded_sad.rerun(self.raw_df)
        manually_transformed_df = self.transform_func(self.raw_df)
        pd.testing.assert_frame_equal(rerun_df, manually_transformed_df)

    def test_get_info(self):
        raw_sad = SelfAwareData(self.raw_df)
        my_sad = raw_sad.transform(self.transform_func, enforce_clean_git=False)
        sad_file_path = my_sad.save(Path(self.test_dir, 'new_sad.csv'), index=False)

        info = SelfAwareDataInterface.get_info(sad_file_path)
        expected_keys = {'timestamp', 'git_hash', 'tag', 'data_type'}
        expected_info = {
            'tag': 'new_sad',
            'data_type': 'csv',
        }
        self.assertTrue(set(info.keys()) == expected_keys)
        for key in expected_info:
            self.assertEqual(info[key], expected_info[key])