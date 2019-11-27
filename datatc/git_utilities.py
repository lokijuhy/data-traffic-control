from git import Repo
import subprocess


def get_git_hash(path: str) -> str:
    """
    Get the short hash of latest git commit.
        path (str): Path to git repo.
    Returns:
        git_hash (str): Short hash of latest commit on the active branch of the git repo.
    """
    git_hash_raw = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD'],
                                           cwd=path)
    git_hash = git_hash_raw.strip().decode("utf-8")
    return git_hash


def check_for_uncommitted_git_changes_at_path(repo_path: str) -> bool:
    """
    Check if there are uncommitted changes in the git repo, and raise an error if there are.
    Args:
        repo_path: str. Path to the repo to check.
    Returns: bool. False: no uncommitted changes found, Repo is valid.
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
