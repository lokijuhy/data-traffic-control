import unittest
import os
import pandas as pd
from pathlib import Path
import shutil
import tempfile
from datatc.self_aware_data import SelfAwareData, SelfAwareDataInterface
from datatc.data_interface import DataInterfaceManager


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

        # assert the sad dir name starts with "sad_dir"
        self.assertEqual(sad_file_path.stem[0:9], 'sad_dir__')

        # assert the sad directory contains 3 files
        sad_dir_contents = [f for f in Path(sad_file_path).iterdir()]
        self.assertEqual(len(sad_dir_contents), 3)

        # assert the sad dir contains a csv, dill, and txt file
        contents_extensions = [f.suffix.replace('.', '') for f in sad_dir_contents]
        for ext in ['csv', 'dill', 'yaml']:
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
        my_sad = raw_sad.transform(self.transform_func, tag='new_sad', enforce_clean_git=False)
        transform_info = my_sad.get_info()

        expected_transform_info_keys = {'timestamp', 'git_hash', 'tag', 'kwargs', 'code'}
        expected_info = {
            'tag': 'new_sad',
            'kwargs': {},
        }
        self.assertEqual(len(transform_info), 1)  # 1 transform
        transform_step_0_info = transform_info[0]
        self.assertTrue(set(transform_step_0_info.keys()) == expected_transform_info_keys)
        for key in expected_info:
            self.assertEqual(transform_step_0_info[key], expected_info[key])

    def test_get_info_with_kwargs(self):
        raw_sad = SelfAwareData(self.raw_df)
        my_sad = raw_sad.transform(self.transform_func_with_kwarg, tag='new_sad', enforce_clean_git=False, factor=2)
        transform_info = my_sad.get_info()

        expected_transform_info_keys = {'timestamp', 'git_hash', 'tag', 'kwargs', 'code'}
        expected_info = {
            'tag': 'new_sad',
            'kwargs': {'factor': 2},
        }
        self.assertEqual(len(transform_info), 1)  # 1 transform
        transform_step_0_info = transform_info[0]
        self.assertTrue(set(transform_step_0_info.keys()) == expected_transform_info_keys)
        for key in expected_info:
            self.assertEqual(transform_step_0_info[key], expected_info[key])

    def test_get_info_with_two_transforms(self):
        raw_sad = SelfAwareData(self.raw_df)
        step_1_sad = raw_sad.transform(self.transform_func, tag='step_1', enforce_clean_git=False)
        step_2_sad = step_1_sad.transform(self.transform_func_with_kwarg, tag='step_2', enforce_clean_git=False, factor=2)
        transform_info = step_2_sad.get_info()

        expected_transform_info_keys = {'timestamp', 'git_hash', 'tag', 'kwargs', 'code'}
        expected_info = [
            {
                'tag': 'step_1',
                'kwargs': {},
            },
            {
                'tag': 'step_2',
                'kwargs': {'factor': 2},
            },
        ]
        self.assertEqual(len(transform_info), 2)  # 2 transforms
        for step in range(len(expected_info)):
            self.assertTrue(set(transform_info[step].keys()) == expected_transform_info_keys)
            for key in expected_info[step]:
                self.assertEqual(transform_info[step][key], expected_info[step][key])

    def test_SelfAwareDataInterface_get_info(self):
        raw_sad = SelfAwareData(self.raw_df)
        my_sad = raw_sad.transform(self.transform_func, tag='new_sad', enforce_clean_git=False)
        sad_file_path = my_sad.save(Path(self.test_dir, 'new_sad.csv'), index=False)

        info = SelfAwareDataInterface.get_info(sad_file_path)
        self.assertEqual(type(info), dict)
        expected_top_level_keys = {'interface_version', 'transform_steps'}
        self.assertTrue(set(info.keys()) == expected_top_level_keys)

        transform_info = info['transform_steps']

        expected_transform_info_keys = {'timestamp', 'git_hash', 'tag', 'kwargs', 'code'}
        expected_info = {
            'tag': 'new_sad',
            'kwargs': {},
        }
        self.assertEqual(len(transform_info), 1)  # 1 transform
        transform_step_0_info = transform_info[0]
        self.assertTrue(set(transform_step_0_info.keys()) == expected_transform_info_keys)
        for key in expected_info:
            self.assertEqual(transform_step_0_info[key], expected_info[key])

    def test_save_untransformed(self):
        raw_sad = SelfAwareData(self.raw_df)
        sad_file_path = raw_sad.save(Path(self.test_dir, 'raw_sad.csv'), index=False)

        # assert a new directory was created, and the path matches the return value
        self.assertTrue(Path(sad_file_path).exists())

        # assert the sad dir name starts with "sad_dir"
        self.assertEqual(sad_file_path.stem[0:9], 'sad_dir__')

        # assert the sad directory contains 3 files
        sad_dir_contents = [f for f in Path(sad_file_path).iterdir()]
        self.assertEqual(len(sad_dir_contents), 3)

        # assert the sad dir contains a csv, dill, and txt file
        contents_extensions = [f.suffix.replace('.', '') for f in sad_dir_contents]
        for ext in ['csv', 'dill', 'yaml']:
            self.assertTrue(ext in contents_extensions)

    # @staticmethod
    # def save_v0(data, code, parent_path):
    #     transform_dir_name = 'transformed_data_dir__2020-05-11_09-56-05__bd9921a__normalized_df'
    #     new_transform_dir_path = Path(parent_path, transform_dir_name)
    #     os.makedirs(new_transform_dir_path)
    #
    #     data_interface = DataInterfaceManager.select('csv')
    #     data_interface.save(data, 'data', new_transform_dir_path)
    #
    #     transformer_func = inspect.getsource(transformer_func)
    #     func_interface = DataInterfaceManager.select('dill')
    #     func_interface.save(transformer_func, 'func', new_transform_dir_path)
    #
    #     code_interface = DataInterfaceManager.select('dill')
    #     code_interface.save(code, 'code', new_transform_dir_path)

    def test_load_interface_version_0(self):
        raw_sad = SelfAwareData(self.raw_df)
        my_sad = raw_sad.transform(self.transform_func, tag='v0_sad', enforce_clean_git=False)
        sad_file_path = SelfAwareDataInterface.save(my_sad, parent_path=self.test_dir, file_name='new_sad.csv',
                                                    interface_version=0, index=False)
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
        reloaded_v0_sad = SelfAwareData.load(sad_file_path)

        # assert the reloaded data is same as what was saved
        pd.testing.assert_frame_equal(reloaded_v0_sad.data, my_sad.data)

        # assert view_code runs without error
        reloaded_v0_sad.view_code()

        # assert rerun generates the same data if given the same input
        rerun_df = reloaded_v0_sad.rerun(self.raw_df)
        manually_transformed_df = self.transform_func(self.raw_df)
        pd.testing.assert_frame_equal(rerun_df, manually_transformed_df)

        # test get_info
        transform_info = my_sad.get_info()
        print(transform_info)

        expected_transform_info_keys = {'timestamp', 'git_hash', 'tag', 'kwargs', 'code'}
        expected_info = {
            'tag': 'v0_sad',
            'kwargs': {},
        }
        self.assertEqual(len(transform_info), 1)  # 1 transform
        transform_step_0_info = transform_info[0]
        self.assertTrue(set(transform_step_0_info.keys()) == expected_transform_info_keys)
        for key in expected_info:
            self.assertEqual(transform_step_0_info[key], expected_info[key])
