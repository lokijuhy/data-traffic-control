import glob
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Union

from datatc.data_interface import DataInterfaceManager
from datatc.data_transformer import TransformedData, TransformedDataInterface


DIRS_TO_IGNORE = ['__pycache__']


class DataDirectory:
    """Manages saving, loading, and viewing data files within a specific data path."""

    def __init__(self, path, contents: Dict[str, 'DataDirectory'] = None, data_interface_manager=DataInterfaceManager):
        """
        Initialize a DataDirectory at a path. The contents of that DataDirectory are recursively characterized and the
        DataDirectory's data_type set. For testing purposes, the contents can also be set directly.

        Args:
            path: The file path to which the DataDirectory corresponds.
            contents: The files and subdirectories contained in the directory.
            data_interface_manager: DataInterfaceManager object to use to interface with files.
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
        self.data_interface_manager = data_interface_manager

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

    def select(self, hint: str) -> Union['DataDirectory', 'DataFile']:
        """Return the DataDirectory from self.contents that matches the hint.
        If more than one file matches the hint, then select the one that file whose type matches the hint exactly.
        Otherwise raise an error and display all matches.

        Args:
            hint: string to use to search for a file within the directory.

        Raises:
            FileNotFoundError: if no file can be found in the data directory that matches the hint.
            ValueError: if more than one file is found in the data directory that matches the hint.
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

    def latest(self) -> Union['DataDirectory', 'DataFile']:
        """Return the latest data file or directory, as determined alphabetically."""
        if len(self.contents) == 0:
            return None

        sorted_contents = sorted([d for d in self.contents])
        latest_content = sorted_contents[-1]
        return self.contents[latest_content]

    def save(self, data: Any, file_name: str, transformer_func: Callable = None, enforce_clean_git: bool = True,
             get_git_hash_from: Any = None, **kwargs) -> None:
        """
        Save a data object within the data directory.

        Args:
            data: data object to save.
            file_name: file name for the saved object, including file extension. The file extension is used to determine
                the file type and method for saving the data.
            transformer_func: Transformation function to run on the data object before saving. If provided,
                the result will be saved as a `TransformedDataDirectory`.
            enforce_clean_git: Whether to require a clean git state before proceeding with the save.
            get_git_hash_from: Package to get the git hash metadata from. Useful if the `transformer_func` is not
                defined within a git repo.
            **kwargs: Remaining args are passed to the data interface save function.

        """

        if transformer_func is None:
            self.save_file(data, file_name, **kwargs)
        else:
            self.transform_and_save(data, transformer_func, file_name, enforce_clean_git, get_git_hash_from, **kwargs)

    def save_file(self, data: Any, file_name: str, **kwargs) -> None:
        data_interface = self.data_interface_manager.select(file_name)
        saved_file_path = data_interface.save(data, file_name, self.path, **kwargs)
        self.contents[file_name] = DataFile(saved_file_path)

    def transform_and_save(self, data: Any, transformer_func: Callable, file_name: str, enforce_clean_git=True,
                           get_git_hash_from: Any = None, **kwargs) -> None:
        new_transform_dir_path = TransformedDataInterface.save(data, transformer_func, parent_path=self.path,
                                                               file_name=file_name, enforce_clean_git=enforce_clean_git,
                                                               get_git_hash_from=get_git_hash_from, **kwargs)
        base_name = os.path.basename(new_transform_dir_path)
        self.contents[base_name] = TransformedDataDirectory(new_transform_dir_path)
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
    """Subclass of `DataDirectory` that manages interacting with the file expression of TransformedData."""

    def __init__(self, path, contents=None):
        super().__init__(path, contents)

        # Overwrite the name (normally os.path.basename) with effective file name
        self.name = self._get_printable_filename()

    def _determine_data_type(self):
        return TransformedDataInterface.get_info(self.path)['data_type']

    def load(self, data_interface_hint: str = None, load_function: bool = True, **kwargs) -> 'TransformedData':
        """
        Load a saved data transformer- the data and the function that generated it.

        Args:
            data_interface_hint: file extension indicating the data interface to use to load the file.
            load_function: Whether to load the transformation function of the TransformedData object. Specify False if
                the current environment does not support the dependencies of the transformation function.
            **kwargs: Remaining args are passed to the data interface save function.
        """
        return TransformedDataInterface.load(self.path, data_interface_hint, load_function, **kwargs)

    def get_info(self) -> Dict[str, str]:
        """Get metadata about the `TransformedData` object."""
        return TransformedDataInterface.get_info(self.path)

    def _build_ls_tree(self, full: bool = False, top_dir: bool = True) -> Dict[str, List]:
        printable_filename = self._get_printable_filename()
        info = TransformedDataInterface.get_info(self.path)
        ls_description = '{}  ({}, {})'.format(printable_filename, info['timestamp'], info['git_hash'])
        return {ls_description: []}

    def _get_printable_filename(self) -> str:
        """Build the filename that should be printed to describe the TransformedDataDirectory.
         The filename is created based on the TransformedDataDirectory tag and file type."""
        info = TransformedDataInterface.get_info(self.path)
        effective_filename = '{}.{}'.format(info['tag'], info['data_type'])
        return effective_filename


class DataFile(DataDirectory):

    def __init__(self, path, contents=None):
        super().__init__(path, contents)

    def __getitem__(self, key):
        raise NotADirectoryError('This is a file!')

    def is_file(self) -> bool:
        return True

    def _determine_data_type(self):
        root, ext = os.path.splitext(self.name)
        if ext != '':
            return ext.replace('.', '')
        else:
            return 'unknown'

    def load(self, data_interface_hint=None, **kwargs) -> Any:
        """
        Load a data file.

        Args:
            data_interface_hint: file extension indicating the data interface to use to load the file.
            **kwargs: Remaining args are passed to the data interface save function.

        """
        if data_interface_hint is None:
            data_interface = self.data_interface_manager.select(self.data_type)
        else:
            data_interface = self.data_interface_manager.select(data_interface_hint)
        print('Loading {}'.format(self.path))
        return data_interface.load(self.path, **kwargs)
