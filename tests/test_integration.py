import unittest
import pandas as pd
from pathlib import Path
import shutil
import tempfile
from datatc.data_directory import DataDirectory, DataFile


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
