import git
from git import Repo
import inspect
import os
import subprocess
from typing import Callable


def get_git_repo_of_func(func: Callable) -> str:
    """
    Determine the directory of the git repo in which the function is defined.
    If the function is not defined in a file (in a notebook or interactive session), or is defined in a file outside
         of a git repo, returns None.

    Args:
        func: A function.

    Returns:
        If the function is defined in a file inside a git repo, returns the path to the git working tree directory.
        If the function is not defined in a file (in a notebook or interactive session), or is defined in a file outside
         of a git repo, returns None.

    """
    file_path = get_file_where_func_defined(func)
    if os.path.exists(file_path):
        if check_if_path_is_in_git_repo(file_path):
            repo = Repo(file_path, search_parent_directories=True)
            return repo.working_tree_dir
    return None


def get_file_where_func_defined(func: Callable) -> str:
    return os.path.abspath(inspect.getfile(func))


def check_if_path_is_in_git_repo(path: str) -> bool:
    try:
        _ = Repo(path, search_parent_directories=True)
    except git.exc.InvalidGitRepositoryError:
        return False
    return True


def get_git_hash_from_path(dir_path: str) -> str:
    """
    Get the short hash of latest git commit.
        path (str): Path to directory within a git repo. Does not need to be the top level repo directory.
    Returns:
        git_hash (str): Short hash of latest commit on the active branch of the git repo.
    """
    git_hash_raw = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD'], cwd=dir_path)
    git_hash = git_hash_raw.strip().decode("utf-8")
    return git_hash


def check_for_uncommitted_git_changes_at_path(repo_path: str) -> bool:
    """
    Check if there are uncommitted changes in the git repo, and raise an error if there are.
    Args:
        repo_path: str. Path to a directory or file within a Git repository. Does not need to be the top level repo dir.
    Returns: bool.
        False: no uncommitted changes found, Repo is valid.
        True: uncommitted changes found. Repo is not valid.
    """
    repo = Repo(repo_path, search_parent_directories=True)

    try:
        # get list of gitignore filenames and extensions as these wouldn't have been code synced over
        # and therefore would appears as if they were uncommitted changes
        with open(os.path.join(repo.working_tree_dir, '.gitignore'), 'r') as f:
            gitignore = [line.strip() for line in f.readlines() if not line.startswith('#') and line != '\n']
    except FileNotFoundError:
        gitignore = []

    gitignore_files = [item for item in gitignore if not item.startswith('*')]
    gitignore_ext = [item.strip('*') for item in gitignore if item.startswith('*')]

    # get list of changed files, but ignore ones in gitignore (either by filename match or extension match)
    changed_files = [item.a_path for item in repo.index.diff(None)
                     if os.path.basename(item.a_path) not in gitignore_files]
    changed_files = [item for item in changed_files
                     if not any([item.endswith(ext) for ext in gitignore_ext])]

    if len(changed_files) > 0:
        raise RuntimeError('There are uncommitted changes in files: {}'
                           '\nCommit them before proceeding. '.format(', '.join(changed_files)))

    return False
