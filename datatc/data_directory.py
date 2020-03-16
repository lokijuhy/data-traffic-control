import glob
import os
from pathlib import Path
from typing import Dict

from datatc.data_interface import DataInterfaceManager


class DataDirectory:
    """Manages interacting with data at a specific data path."""

    def __init__(self, path):
        # TODO: check path
        self.path = path
        self.name = os.path.basename(self.path)
        self.contents = self._characterize_dir(self.path)
        # determine_data_type has to be done _after_ characterize dir because it inspects the children
        self.data_type = self._determine_data_type()

    def __getitem__(self, key):
        return self.contents[key]

    def is_file(self):
        return False

    def _determine_data_type(self):
        dir_data_types = [self.contents[f].data_type for f in self.contents]
        unique_dir_data_types = list(set(dir_data_types))
        if len(unique_dir_data_types) == 0:
            return 'empty'
        elif len(unique_dir_data_types) > 1:
            return 'mixed'
        else:
            return unique_dir_data_types[0]

    def select(self, hint: str) -> 'DataDirectory':
        """Return the DataDirectory from self.contents that matches the hint."""
        matches = [self.contents[d] for d in self.contents if hint in self.contents[d].name]
        if len(matches) == 1:
            return matches[0]
        elif len(matches) == 0:
            raise FileNotFoundError("No match for hint '{}'".format(hint))
        elif len(matches) > 1:
            match_names = [m.name for m in matches]
            raise ValueError("More than one match found: [{}]".format(', '.join(match_names)))

    def load(self):
        raise NotImplementedError("I haven't gotten to this yet!")

    def _characterize_dir(self, path) -> Dict:
        contents = {}
        glob_path = Path(self.path, '*')
        subpaths = glob.glob(glob_path.__str__())
        for p in subpaths:
            name = os.path.basename(p)
            if '.' not in name:
                contents[name] = DataDirectory(p)
            else:
                contents[name] = DataFile(p)
        return contents

    def ls(self, full=False, indent: int = 0):
        print('{}{}/'.format(' '*4*indent, self.name))

        if len(self.contents) > 0:
            contains_subdirs = any([not self.contents[c].is_file() for c in self.contents])
            if contains_subdirs or full:
                # print all directories first
                dirs = [self.contents[item] for item in self.contents if not self.contents[item].is_file()]
                dirs_sorted = sorted(dirs, key=lambda k: k.name)
                # ... then print all files
                files = [self.contents[item] for item in self.contents if self.contents[item].is_file()]
                files_sorted = sorted(files, key=lambda k: k.name)
                contents_sorted = dirs_sorted + files_sorted

                for d in contents_sorted:
                    d.ls(full=full, indent=indent+1)

            else:
                print('{}{} {} items'.format(' '*4*(indent+1), len(self.contents), self.data_type))


class DataFile(DataDirectory):

    def __init__(self, path):
        super().__init__(path)

    def __getitem__(self, key):
        raise NotADirectoryError('This is a file!')

    def is_file(self):
        return True

    def _determine_data_type(self):
        file_extension = self.name.split('.')[1]
        return file_extension

    def _characterize_dir(self, path) -> Dict:
        """A file has no sub contents"""
        return {}

    def load(self):
        data_interface = DataInterfaceManager.select(self.data_type)
        print('Loading {}'.format(self.path))
        return data_interface.load(self.path)

    def ls(self, full=False, indent: int = 0):
        print('{}{}'.format(' '*4*indent, self.name))
