import unittest
import os
import pandas as pd
from pathlib import Path
import shutil
import tempfile
from datatc.self_aware_data import SelfAwareData, SelfAwareDataInterface, LiveTransformStep, SourceFileTransformStep,\
    IntermediateFileTransformStep


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

        # assert there is 1 transform step and 1 intermediate file step
        transform_sequence = reloaded_sad.transform_sequence.sequence
        self.assertEqual(len(transform_sequence), 2)
        self.assertEqual(type(transform_sequence[0]), LiveTransformStep)
        self.assertEqual(type(transform_sequence[1]), IntermediateFileTransformStep)

        # assert print_steps runs without error
        reloaded_sad.print_steps()

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
        expected_top_level_keys = {'interface_version', 'timestamp', 'tag', 'data_type', 'transform_steps'}
        self.assertTrue(set(info.keys()) == expected_top_level_keys)
        expected_top_level_info = {
            'interface_version': 1,
            'tag': 'new_sad',
            'data_type': 'csv',
        }
        for key in expected_top_level_info:
            self.assertEqual(info[key], expected_top_level_info[key])

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

    def test_load_SAD_from_file(self):
        raw_data_path = Path(self.test_dir, 'raw_df.csv')
        self.raw_df.to_csv(raw_data_path, index=False)
        from_file_sad = SelfAwareData.load_from_file(raw_data_path)

        self.assertEqual(len(from_file_sad.transform_sequence.sequence), 1)
        self.assertTrue(type(from_file_sad.transform_sequence.sequence[0]), SourceFileTransformStep)

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

    def test_load_interface_version_0(self):
        raw_sad = SelfAwareData(self.raw_df)
        my_sad = raw_sad.transform(self.transform_func, 'unrecorded_tag', enforce_clean_git=False)
        sad_file_path = SelfAwareDataInterface.save(my_sad, parent_path=self.test_dir, file_name='v0_sad.csv',
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

        # assert there is 1 transform step and 1 intermediate file step
        transform_sequence = reloaded_v0_sad.transform_sequence.sequence
        self.assertEqual(len(transform_sequence), 2)
        self.assertEqual(type(transform_sequence[0]), LiveTransformStep)
        self.assertEqual(type(transform_sequence[1]), IntermediateFileTransformStep)

        # assert print_steps runs without error
        reloaded_v0_sad.print_steps()

        # assert rerun generates the same data if given the same input
        rerun_df = reloaded_v0_sad.rerun(self.raw_df)
        manually_transformed_df = self.transform_func(self.raw_df)
        pd.testing.assert_frame_equal(rerun_df, manually_transformed_df)

        # test get_info
        transform_info = reloaded_v0_sad.get_info()
        print(transform_info)

        expected_transform_info_keys = {'timestamp', 'git_hash', 'tag', 'kwargs', 'code'}
        expected_info = {
            'tag': 'v0_sad',
            'kwargs': {},
        }
        self.assertEqual(len(transform_info), 2)  # 1 transform step and 1 intermediate file step
        transform_step_0_info = transform_info[0]
        self.assertTrue(set(transform_step_0_info.keys()) == expected_transform_info_keys)
        for key in expected_info:
            self.assertEqual(transform_step_0_info[key], expected_info[key])
