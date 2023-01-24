from distutils.log import error
import pandas as pd
import os
from datetime import datetime
import multiprocessing
import tqdm
from functools import partial

from Repository import Repository
from utilities import load_api_dict

"""
Determine which ml stages were modified in each commit.
Creates a .csv file for each repository that contains a row for each commit, a column for each ml stages and the number of relevant files changed as cell values.
Requires that the repos have already been cloned and stored locally.
"""

#Specify where you stored your local copies of the subject repos
CLONED_REPO_DIR = '/mnt/volume1/mlexpmining/cloned_repos'
OUTPUT_DIR = "data/commit_stages"

N_PROCESSES = 32

# Storing information about skipped repos
skipped = multiprocessing.Value('i', 0)


def get_commit_stages(repo_name, functions_to_stages, stages_to_functions):
    """Get the results for one repository and save it. Assumes that repos are stored locally."""

    # Determine the full absolute filepath from the repo name
    repo_filepath = os.path.join(CLONED_REPO_DIR, repo_name)
    print(repo_filepath)
    if not os.path.exists(repo_filepath):
        return
    try:
        # Get results
        repo = Repository(local_dir=repo_filepath)
        results = repo.get_commit_stages(
            functions_to_stages, stages_to_functions)
        # Save results
        repo_name = repo_filepath.split('/')[-1]
        result_dir = os.path.join(OUTPUT_DIR, repo_name)
        results.to_csv(result_dir, index=False)
    except Exception as e:
        # Ignore errors
        print(e)
    # return results, result_dir


def single_run():
    """Get commit stages for all repos that are stored locally. Uses single process."""

    # repos = os.listdir(CLONED_REPO_DIR)
    stages_to_functions, functions_to_stages = load_api_dict()
    repos = os.listdir(CLONED_REPO_DIR)[:10]
    for i, repo_name in enumerate(repos):
        print(i)
        # repo_path = repo_dir.path
        results = get_commit_stages(repo_name, functions_to_stages,
                                    stages_to_functions.keys())
        print(results)


def parallel_run():
    """Get commit stages for all repos that are stored locally. Uses multiple processes."""

    start_time = datetime.now()
    # Load data
    stages_to_functions, functions_to_stages = load_api_dict()
    repos = os.listdir(CLONED_REPO_DIR)

    # Apply multiprocessing to obtain the results
    pool = multiprocessing.Pool(processes=N_PROCESSES)
    callback = partial(get_commit_stages, stages=stages_to_functions.keys(),
                       functions_to_stages=functions_to_stages)
    for _ in tqdm.tqdm(pool.imap_unordered(callback, repos), total=len(repos)):
        pass

    print(f"Skipped {skipped.value} out of {len(repos)} repositories.")
    end_time = datetime.now()
    print("Program execution time: ", end_time-start_time)


def get_commit_stages_remote(repo_name, functions_to_stages, stages_to_functions):
    """Does the dame as get_commit_stages if repos are stored as tar files on an sftp server"""

    try:
        repo = Repository(remote_compressed=repo_name)
        results = repo.get_commit_stages(
            functions_to_stages, stages_to_functions)
        result_dir = os.path.join(OUTPUT_DIR, repo_name)
        results.to_csv(result_dir, index=False)
    except Exception as e:
        print(e)


def remote_run():
    """Get the commit_stages for all repositories that are stored on an sftp server. Uses single process."""
    stages_to_functions, functions_to_stages = load_api_dict()
    repos = []
    with open('../remaining_repos.txt') as f:
        for line in f.readlines():
            repos.append(line.replace('\n', ''))

    for i, repo_name in enumerate(repos):
        print(i)
        get_commit_stages_remote(repo_name, functions_to_stages,
                                 stages_to_functions.keys())


def remote_run_parallel():
    """Get the commit_stages for all repositories that are stored on an sftp server. Uses multiple processes."""
    start_time = datetime.now()
    # Load data
    stages_to_functions, functions_to_stages = load_api_dict()
    repos = []
    with open('../remaining_repos.txt') as f:
        for line in f.readlines():
            repos.append(line.replace('\n', ''))
    repos = repos
    # Apply multiprocessing to obtain the results
    pool = multiprocessing.Pool(processes=N_PROCESSES)
    callback = partial(get_commit_stages_remote, stages_to_functions=stages_to_functions,
                       functions_to_stages=functions_to_stages)
    for _ in tqdm.tqdm(pool.imap_unordered(callback, repos), total=len(repos)):
        pass

    print(f"Skipped {skipped.value} out of {len(repos)} repositories.")
    end_time = datetime.now()
    print("Program execution time: ", end_time-start_time)


def get_commit_stages_clone(repo_url, functions_to_stages, stages_to_functions):
    """Get the commit stages of a given repository cloning the repo directly from GitHub"""
    try:
        repo = Repository(remote_url=repo_url)
        results = repo.get_commit_stages(
            functions_to_stages, stages_to_functions)
        repo_name = repo_url[19:].replace('/', '-')
        result_dir = os.path.join(OUTPUT_DIR, repo_name)
        results.to_csv(result_dir, index=False)
    except Exception as e:
        print(e)


def run_clone():
    """Get the commit_stages of all remaining repos using get_commit_stages_clone. Uses single process."""
    stages_to_functions, functions_to_stages = load_api_dict()
    with open('remaining_repo_urls', 'r') as f:
        repos = [i[:-2] for i in f.readlines()]

    for i, repo_url in enumerate(repos):
        print(i)
        get_commit_stages_clone(repo_url, functions_to_stages,
                                stages_to_functions.keys())


def run_clone_parallel():
    """Get the commit_stages of all remaining repos using get_commit_stages_clone. Uses multiple processes."""
    start_time = datetime.now()
    # Load data
    stages_to_functions, functions_to_stages = load_api_dict()
    with open('remaining_repo_urls', 'r') as f:
        repos = repos = [i[:-2] for i in f.readlines()]

    # Apply multiprocessing to obtain the results
    pool = multiprocessing.Pool(processes=N_PROCESSES)
    callback = partial(get_commit_stages_clone, stages_to_functions=stages_to_functions,
                       functions_to_stages=functions_to_stages)
    for _ in tqdm.tqdm(pool.imap_unordered(callback, repos), total=len(repos)):
        pass

    print(f"Skipped {skipped.value} out of {len(repos)} repositories.")
    end_time = datetime.now()
    print("Program execution time: ", end_time-start_time)


if __name__ == "__main__":
    parallel_run()
    # remote_run()
    # remote_run_parallel()
    # run_clone()
    # run_clone_parallel()
