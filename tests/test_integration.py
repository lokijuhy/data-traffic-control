import unittest
import pandas as pd
from pathlib import Path
import shutil
import tempfile
from datatc import DataManager
from datatc.data_directory import DataDirectory, SelfAwareDataDirectory, DataFile
from datatc.data_interface import TestDataInterfaceManager
from datatc.self_aware_data import SelfAwareData


class TestIntegration(unittest.TestCase):

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

    def test_integration(self):
        dd = DataDirectory(self.test_dir)
        self.assertTrue('test.csv' in dd.contents)
        self.assertTrue(type(dd.contents['test.csv']) == DataFile)

        dd.ls()

        # TEST LOAD
        test_csv = dd['test.csv'].load()
        pd.testing.assert_frame_equal(test_csv, self.raw_df)

        # TEST SELECT
        test_csv_path = dd.select('test').path
        self.assertEqual(test_csv_path, Path(self.test_dir, 'test.csv').__str__())

        test_csv_path = dd.select('csv').path
        self.assertEqual(test_csv_path, Path(self.test_dir, 'test.csv').__str__())

        # TEST LATEST
        test_csv_path = dd.latest().path
        self.assertEqual(test_csv_path, Path(self.test_dir, 'test.csv').__str__())

        # TEST SAVE
        new_df = self.raw_df.copy()
        dd.save(new_df, 'new_df.csv')
        self.assertTrue('new_df.csv' in dd.contents)
        self.assertTrue(type(dd.contents['new_df.csv']) == DataFile)

        # TEST SAD
        def transform_func(input_df, factor):
            df = input_df.copy()
            df['col_1'] = df['col_1'] * factor
            return df

        my_factor = 2

        raw_sad = SelfAwareData(new_df)
        my_sad = raw_sad.transform(transform_func, enforce_clean_git=False, factor=my_factor)
        manually_transformed_df = transform_func(new_df, my_factor)
        pd.testing.assert_frame_equal(my_sad.data, manually_transformed_df)

        # TEST SAD SAVE
        dd.save(my_sad, 'new_sad.csv', index=False)
        self.assertTrue('new_sad.csv' in dd.contents)
        self.assertTrue(type(dd.contents['new_sad.csv']) == SelfAwareDataDirectory)

        # TEST SAD GET_INFO
        info = dd['new_sad.csv'].get_info()
        expected_keys = {'timestamp', 'git_hash', 'tag', 'data_type'}
        expected_info = {
            'tag': 'new_sad',
            'data_type': 'csv',
        }
        self.assertTrue(set(info.keys()) == expected_keys)
        for key in expected_info:
            self.assertEqual(info[key], expected_info[key])

        # TEST SAD LOAD
        reloaded_sad = dd['new_sad.csv'].load()
        pd.testing.assert_frame_equal(reloaded_sad.data, my_sad.data)

        reloaded_sad.view_code()
        rerun_df = reloaded_sad.rerun(new_df, factor=my_factor)
        pd.testing.assert_frame_equal(rerun_df, manually_transformed_df)
