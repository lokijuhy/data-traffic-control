import datetime
import glob
import inspect
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

from datatc.data_interface import DataInterfaceManager, DillDataInterface, TextDataInterface
from datatc.data_transformer import TransformedData
from datatc import git_utilities


DIRS_TO_IGNORE = ['__pycache__']


class DataDirectory:
    """Manages interacting with data at a specific data path."""

    def __init__(self, path, contents: Dict[str, 'DataDirectory'] = None):
        """
        Initialize a DataDirectory at a path. The contents of that DataDirectory are recursively characterized and the
        DataDirectory's data_type set. For testing purposes, the contents can also be set directly.

        Args:
            path: The file path to which the DataDirectory corresponds.
            contents: The files and subdirectories contained in the directory.
        """
        # TODO: check path
        self.path = path
        self.name = os.path.basename(self.path)
        if contents is None:
            self.contents = self._characterize_dir(self.path)
        else:
            self.contents = contents
        # determine_data_type has to be done _after_ characterize dir because it inspects the children
        self.data_type = self._determine_data_type()

    def __getitem__(self, key):
        return self.contents[key]

    def is_file(self):
        return False

    def _determine_data_type(self) -> str:
        dir_data_types = [self.contents[f].data_type for f in self.contents]
        unique_dir_data_types = list(set(dir_data_types))
        if len(unique_dir_data_types) == 0:
            return 'empty'
        elif len(unique_dir_data_types) > 1:
            return 'mixed'
        else:
            return unique_dir_data_types[0]

    def select(self, hint: str) -> 'DataDirectory':
        """Return the DataDirectory from self.contents that matches the hint.
        If more than one file matches the hint, then select the one that file whose type matches the hint exactly.
        Otherwise raise an error and display all matches.
        """
        matches = [self.contents[d] for d in self.contents if hint in self.contents[d].name]
        if len(matches) == 1:
            return matches[0]
        elif len(matches) == 0:
            raise FileNotFoundError("No match for hint '{}'".format(hint))
        elif len(matches) > 1:
            exact_matches = [self.contents[d] for d in self.contents if hint == self.contents[d].data_type]

            if len(exact_matches) == 1:
                return exact_matches[0]
            elif len(exact_matches) == 0:
                match_names = [m.name for m in matches]
                raise ValueError("More than one match found: [{}]".format(', '.join(match_names)))
            elif len(exact_matches) > 1:
                match_names = [m.name for m in exact_matches]
                raise ValueError("More than one match found: [{}]".format(', '.join(match_names)))

    def latest(self) -> 'DataDirectory':
        """Return the latest data file or directory, as determined alphabetically."""
        if len(self.contents) == 0:
            return None

        sorted_contents = sorted([d for d in self.contents])
        latest_content = sorted_contents[-1]
        return self.contents[latest_content]

    def save(self, data: Any, file_name: str, transformer_func: Callable = None, enforce_clean_git: bool = True
             ) -> None:

        if transformer_func is None:
            self.save_file(data, file_name)
        else:
            tag, data_file_type = os.path.splitext(file_name)
            self.transform_and_save(data, transformer_func, tag, data_file_type, enforce_clean_git)

    def save_file(self, data: Any, file_name: str) -> None:
        data_interface = DataInterfaceManager.select(file_name)
        data_interface.save(data, file_name, self.path)
        self.contents[file_name] = DataFile(Path(self.path, file_name))

    def transform_and_save(self, data: Any, transformer_func: Callable, tag: str, data_file_type: str,
                           enforce_clean_git=True) -> None:

        name, newTDD = TransformedDataDirectory.save(data, transformer_func, tag, data_file_type, parent_path=self.path,
                                                     enforce_clean_git=enforce_clean_git)
        self.contents[name] = newTDD
        return

    def load(self):
        raise NotImplementedError("Loading the entire contents of a directory has not yet been implemented!")

    @staticmethod
    def _characterize_dir(path) -> Dict[str, 'DataDirectory']:
        """
        Characterize the contents of the DataDirectory, creating new DataDirectories for subdirectories and DataFiles
        for files.

        Args:
            path: File path to characterize.

        Returns: A Dictionary of file/directory names (str) to DataDirectory/DataFile objects.

        """
        contents = {}
        glob_path = Path(path, '*')
        subpaths = glob.glob(glob_path.__str__())
        for p in subpaths:
            name = os.path.basename(p)
            if name in DIRS_TO_IGNORE:
                continue
            if os.path.isdir(p):
                if 'transformed_data_dir' in p:
                    contents[name] = TransformedDataDirectory(p)
                else:
                    contents[name] = DataDirectory(p)
            elif os.path.isfile(p):
                contents[name] = DataFile(p)
            else:
                print('WARNING: {} is neither a file nor a directory.'.format(p))
        return contents

    def ls(self, full=False) -> None:
        """
        Print the contents of the data directory. Defaults to printing all subdirectories, but not all files.

        Args:
            full: Whether to print all files.

        Returns: prints!

        """
        contents_ls_tree = self._build_ls_tree(full=full)
        self._print_ls_tree(contents_ls_tree)

    def _build_ls_tree(self, full: bool = False, top_dir: bool = True) -> Dict[str, List]:
        """
        Recursively navigate the data directory tree and build a dictionary summarizing its contents.
        All subdirectories are added to the ls_tree dictionary.
        Files are added to the ls_tree dictionary if any of the three conditions are true:
        - the `full` flag is used
        - the files sit next to other subdirectories
        - the initial directory being ls'ed contains only files, no subdirectories

        Args:
            full: flag to add all files to the ls_tree dict
            top_dir: whether the dir currently being inspected is the initial dir that the the user called `ls` on

        Returns: Dictionary describing the DataDirectory at the requested level of detail.
        """
        contents_ls_tree = []

        if len(self.contents) > 0:
            contains_subdirs = any([not self.contents[c].is_file() for c in self.contents])
            if contains_subdirs or full or (top_dir and not contains_subdirs):
                # build all directories first
                dirs = [self.contents[item] for item in self.contents if not self.contents[item].is_file()]
                dirs_sorted = sorted(dirs, key=lambda k: k.name)
                for d in dirs_sorted:
                    contents_ls_tree.append(d._build_ls_tree(full=full, top_dir=False))

                # ... then collect all files
                files = [self.contents[item] for item in self.contents if self.contents[item].is_file()]
                files_sorted = sorted(files, key=lambda k: k.name)
                for f in files_sorted:
                    contents_ls_tree.append(f.name)
            else:
                contents_ls_tree.append('{} {} items'.format(len(self.contents), self.data_type))

        return {self.name: contents_ls_tree}

    def _print_ls_tree(self, ls_tree: Dict[str, List], indent: int = 0) -> None:
        """
        Recursively print the ls_tree dictionary as created by `_build_ls_tree`.
        Args:
            ls_tree: Dict describing a DataDirectory contents.
            indent: indent level to print with at the current level of recursion.

        Returns: None. Prints!

        """
        if type(ls_tree) == str:
            print('{}{}'.format(' ' * 4 * indent, ls_tree))
        else:
            for key in ls_tree:
                contents = ls_tree[key]
                if len(contents) == 0:
                    print('{}{}'.format(' ' * 4 * indent, key))
                else:
                    print('{}{}/'.format(' ' * 4 * indent, key))
                    for item in contents:
                        self._print_ls_tree(item, indent+1)


class TransformedDataDirectory(DataDirectory):
    """Manages interacting with the file expression of TransformedData, which is:
    transformed_data_<date>_<git_hash>_<tag>/
        data.xxx
        func.dill
        code.txt
    """

    def __init__(self, path, contents=None):
        super().__init__(path, contents)

        self.data_file = self._identify_file_from_contents(self.contents, 'data', self.path)
        self.func_file = self._identify_file_from_contents(self.contents, 'func', self.path)
        self.code_file = self._identify_file_from_contents(self.contents, 'code', self.path)

        self.time_stamp, self.git_hash, self.tag = self._parse_transform_dir_name(self.name)

    @staticmethod
    def _identify_file_from_contents(contents: Dict[str, 'DataFile'], file_name: str, parent_dir: str) -> 'DataFile':
        options = [contents[file] for file in contents if file_name in file]
        if len(options) == 0:
            raise ValueError('No {} file found for TransformedDataDirectory {}'.format(file_name, parent_dir))
        elif len(options) > 1:
            raise ValueError('More than one {} file found for TransformedDataDirectory {}'.format(file_name, parent_dir)
                             )
        else:
            return options[0]

    def _determine_data_type(self):
        # This function can't use self.data_file because this function is called in super().__init__(), and
        # self.data_file is not set until self.__init__()
        data_type = self._identify_file_from_contents(self.contents, 'data', self.path).data_type
        return data_type

    @classmethod
    def transform_and_save(cls, data: Any, transformer_func: Callable, tag: str, data_file_type: str, parent_path: str,
                           enforce_clean_git=True) -> Tuple[str, 'TransformedDataDirectory']:
        # TODO: figure out how to move this to TransformedDataInterface
        """
        Save a transformed dataset.

        Args:
            data: Input data to transform.
            transformer_func: Transform function to apply to data.
            tag: Short description of the transform being applied.
            data_file_type: File type to save the data as
            parent_path: The parent path at which the new TransformedDataDirectory will be created
            enforce_clean_git: Whether to only allow the save to proceed if the working state of the git directory is
                clean.

        Returns: Tuple[new transform directory name, TransformedDataDirectory object], for adding to contents dict.

        """
        # TODO: need module 'datatc.git_utilities' has no attribute 'get_repo_path'
        # if enforce_clean_git:
        #     git_utilities.check_for_uncommitted_git_changes()

        transform_dir_name = cls._generate_name_for_transform_dir(tag)
        new_transform_dir_path = Path(parent_path, transform_dir_name)
        os.makedirs(new_transform_dir_path)

        data_interface = DataInterfaceManager.select(data_file_type)
        data = transformer_func(data)
        data_interface.save(data, 'data', new_transform_dir_path)

        DillDataInterface.save(transformer_func, 'func', new_transform_dir_path)

        transformer_func_code = inspect.getsource(transformer_func)
        TextDataInterface.save(transformer_func_code, 'code', new_transform_dir_path)

        new_TransformedDataDirectory = TransformedDataDirectory(new_transform_dir_path)

        print('created new file {}'.format(new_transform_dir_path))
        return transform_dir_name, new_TransformedDataDirectory

    def load(self, data_interface_hint=None) -> 'TransformedData':
        """
        Load a saved data transformer- the data and the function that generated it.

        Args:
            data_interface_hint:

        Returns: Tuple(data, transformer_func)

        """
        data = self.data_file.load(data_interface_hint=data_interface_hint)
        transformer_func = self.func_file.load()
        transformer_code = self.code_file.load()
        return TransformedData(data, transformer_func, transformer_code)

    @classmethod
    def _generate_name_for_transform_dir(cls, tag: str = None) -> str:
        repo_path = git_utilities.get_repo_path()
        git_hash = git_utilities.get_git_hash(repo_path)
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        delimiter_char = '__'
        file_name_components = ['transformed_data_dir', timestamp, git_hash]
        if tag is not None:
            file_name_components.append(tag)
        return delimiter_char.join(file_name_components)

    @classmethod
    def _parse_transform_dir_name(cls, name) -> Tuple[str, str, str]:
        delimiter_char = '__'
        file_name_components = name.split(delimiter_char)
        if len(file_name_components) == 3:
            denoter, timestamp, git_hash = file_name_components
            tag = ''
        elif len(file_name_components) == 4:
            denoter, timestamp, git_hash, tag = file_name_components
        else:
            raise ValueError('TransformedDataDirectory name could not be parsed: {}'.format(name))
        return timestamp, git_hash, tag

    def _build_ls_tree(self, full: bool = False, top_dir: bool = True) -> Dict[str, List]:
        ls_description = '{}.{}'.format(self.tag, self.data_type)
        return {ls_description: []}


class DataFile(DataDirectory):

    def __init__(self, path, contents=None):
        super().__init__(path, contents)

    def __getitem__(self, key):
        raise NotADirectoryError('This is a file!')

    def is_file(self):
        return True

    def _determine_data_type(self):
        root, ext = os.path.splitext(self.name)
        if ext != '':
            return ext.replace('.', '')
        else:
            return 'unknown'

    def load(self, data_interface_hint=None):
        if data_interface_hint is None:
            data_interface = DataInterfaceManager.select(self.data_type)
        else:
            data_interface = DataInterfaceManager.select(data_interface_hint)
        print('Loading {}'.format(self.path))
        return data_interface.load(self.path)
