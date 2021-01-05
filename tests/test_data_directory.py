import unittest
from datatc.data_directory import DataDirectory, DataFile
from datatc.data_interface import TestDataInterfaceManager


class TestDataDirectory(unittest.TestCase):

    # === LS ===

    def test_ls_one_file(self):
        file1 = DataFile(path='file1.txt', contents={})
        data_dir = DataDirectory('top', contents={'file1.txt': file1})
        expected_result = {'top': ['file1.txt']}
        self.assertEqual(data_dir._build_ls_tree(), expected_result)

    def test_ls_one_file_full(self):
        file1 = DataFile(path='file1.txt', contents={})
        data_dir = DataDirectory('top', contents={'file1.txt': file1})
        expected_result = {'top': ['file1.txt']}
        self.assertEqual(data_dir._build_ls_tree(full=True), expected_result)

    def test_ls_two_same_type_files(self):
        file1 = DataFile(path='file1.txt', contents={})
        file2 = DataFile(path='file2.txt', contents={})
        data_dir = DataDirectory('top', contents={'file1.txt': file1, 'file2.txt': file2})
        expected_result = {'top': ['file1.txt', 'file2.txt']}
        self.assertEqual(data_dir._build_ls_tree(), expected_result)

    def test_ls_two_same_type_files_full(self):
        file1 = DataFile(path='file1.txt', contents={})
        file2 = DataFile(path='file2.txt', contents={})
        data_dir = DataDirectory('top', contents={'file1.txt': file1, 'file2.txt': file2})
        expected_result = {'top': ['file1.txt', 'file2.txt']}
        self.assertEqual(data_dir._build_ls_tree(full=True), expected_result)

    def test_ls_one_file_in_subdir(self):
        file1 = DataFile(path='file1.txt', contents={})
        subdir = DataDirectory(path='subdir', contents={'file1.txt': file1})
        data_dir = DataDirectory('top', contents={'subdir': subdir})
        expected_result = {'top': [
            {'subdir': [
                '1 txt items']
            }
        ]}
        self.assertEqual(data_dir._build_ls_tree(), expected_result)

    def test_ls_one_file_full_in_subdir(self):
        file1 = DataFile(path='file1.txt', contents={})
        subdir = DataDirectory(path='subdir', contents={'file1.txt': file1})
        data_dir = DataDirectory('top', contents={'subdir': subdir})
        expected_result = {'top': [
            {'subdir': [
                'file1.txt'
            ]}
        ]}
        self.assertEqual(data_dir._build_ls_tree(full=True), expected_result)

    def test_ls_two_same_type_files_in_subdir(self):
        file1 = DataFile(path='file1.txt', contents={})
        file2 = DataFile(path='file2.txt', contents={})
        subdir = DataDirectory(path='subdir', contents={'file1.txt': file1, 'file2.txt': file2})
        data_dir = DataDirectory('top', contents={'subdir': subdir})
        expected_result = {'top': [
            {'subdir': [
                '2 txt items'
            ]}
        ]}
        self.assertEqual(data_dir._build_ls_tree(), expected_result)

    def test_ls_two_same_type_files_full_in_subdir(self):
        file1 = DataFile(path='file1.txt', contents={})
        file2 = DataFile(path='file2.txt', contents={})
        subdir = DataDirectory(path='subdir', contents={'file1.txt': file1, 'file2.txt': file2})
        data_dir = DataDirectory('top', contents={'subdir': subdir})

        expected_result = {'top': [
            {'subdir': [
                'file1.txt',
                'file2.txt'
            ]}
        ]}

        self.assertEqual(data_dir._build_ls_tree(full=True), expected_result)

    def test_ls_empty_top_dir(self):
        data_dir = DataDirectory('empty_dir', contents={})
        expected_result = {'empty_dir': []}
        self.assertEqual(data_dir._build_ls_tree(), expected_result)

    def test_ls_empty_sub_dir(self):
        data_dir = DataDirectory('empty_dir', contents={})
        top_dir = DataDirectory('top_dir', contents={'empty_dir': data_dir})
        expected_result = {'top_dir': [
            {'empty_dir': []
             }
        ]}
        self.assertEqual(top_dir._build_ls_tree(), expected_result)

    # === SELECT ===
    def test_select_hint_one_exact_match_one_fuzzy_match(self):
        sql_file = DataFile(path='query.sql', contents={})
        sqlite_file = DataFile(path='db.sqlite', contents={})
        data_dir = DataDirectory('top', contents={'query.sql': sql_file, 'db.sqlite': sqlite_file})
        hint = 'sql'
        self.assertEqual(data_dir.select(hint), sql_file)

    def test_select_hint_fuzzy_match_two_files_raises_value_error(self):
        sql_file = DataFile(path='query.sql', contents={})
        sqlite_file = DataFile(path='db.sqlite', contents={})
        data_dir = DataDirectory('top', contents={'query.sql': sql_file, 'db.sqlite': sqlite_file})
        hint = 'sq'
        self.assertRaises(ValueError, data_dir.select, hint)

    # === LATEST ===

    def test_latest_year_month_day_comparison(self):
        older_file = DataFile(path='2020_01_01.txt', contents={})
        newer_file = DataFile(path='2020_02_01.txt', contents={})
        data_dir = DataDirectory('top', contents={'2020_01_01.txt': older_file, '2020_02_01.txt': newer_file})
        self.assertEqual(data_dir.latest(), newer_file)

    def test_latest_empty_dir_returns_none(self):
        data_dir = DataDirectory('top', contents={})
        self.assertEqual(data_dir.latest(), None)

    # === SAVE ===

    def test_save_adds_to_dir_contents(self):
        initial_directory_contents = {}
        file_name = 'file.test'
        expected_file_path = '$HOME/{}'.format(file_name)
        expected_directory_contents_after_save = {file_name: DataFile(expected_file_path)}

        data_directory = DataDirectory(path='$HOME', contents=initial_directory_contents,
                                       data_interface_manager=TestDataInterfaceManager)
        data_directory.save(42, file_name)

        # check that the contents keys (the file names) are the same
        self.assertEqual(data_directory.contents.keys(), expected_directory_contents_after_save.keys())
        # check that the contents values are type DataFiles
        for k in expected_directory_contents_after_save:
            self.assertEqual(type(data_directory.contents[k]), DataFile)
