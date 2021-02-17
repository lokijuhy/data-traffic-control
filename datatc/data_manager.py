from datatc.data_directory import DataDirectory, DataDirectoryManager

CONFIG_FILE_NAME = '.data_map.yaml'


class DataManager:
    """
    Keep track of project data directories.

    """

    def __init__(self, path_hint: str):
        """
        Initialize a DataManager pointing at a project's data_path.

        Args:
            path_hint: the name of a project that has been previously registered to `DataManager`, or a path to a data
                directory.

        """
        self.data_path = DataDirectoryManager.load_project_path_from_hint(path_hint)
        self.data_directory = DataDirectory(self.data_path.__str__())
        raise DeprecationWarning('DataManager is deprecated. Please use `DataDirectory.load()` instead.')

    def reload(self):
        """Refresh the data directory contents that `DataManager` is aware of.
        Useful if you have created a new file on the file system without using `DataManager`, and now need `DataManager`
        to know about it. """
        self.data_directory = DataDirectory(self.data_path)

    def __getitem__(self, key):
        return self.data_directory[key]

    @classmethod
    def register_project(cls, project_hint: str, project_path: str) -> None:
        return DataDirectoryManager.register_project(project_hint, project_path)

    @classmethod
    def list_projects(cls) -> None:
        return DataDirectoryManager.list_projects()

    def ls(self, full: bool = False) -> None:
        """
        List the contents of the data directory.

        Args:
            full: If True, prints the full data directory contents. If false, prints only a summary of the file types
             contained in each directory (prints all subdirectories).

        """
        self.data_directory.ls(full=full)
