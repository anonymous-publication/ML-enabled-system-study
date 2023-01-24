import os
import multiprocessing
import tqdm
from functools import partial
import pandas as pd
from datetime import datetime

from Repository import Repository
from utilities import load_api_dict

"""
Obtains the implemented ml stages for each repository and adds this information to the summary file.
"""

n_processes = 4

# Adapt these filepaths depending on the storage location of your interim results
input_filepath = '../5-attributes/tensorflow.csv'
output_filepath = '../5-attributes/tensorflow.csv'
local_path_main = '/mnt/volume1/mlexpmining/cloned_repos'

# Storing information about skipped repos
skipped = multiprocessing.Value('i', 0)


def get_stage_list(item, stages_to_functions, functions_to_stages):
    """
    Returns a list of ml stages implemented by a given repository
    Args:
        item:                   Tuple contnaining a row of the dataframe and an index. (Yielded by df.iterrows).
        stages_to_functions:    Dictionary mapping ml stages to functions
        functions_to_stages:    Inverse of stages_to_functions.
    Returns:
        pd.Series (Row of the dataframe with the list of ml stages added)
        None (If there was an error)
    """

    # Extract relevant information
    repo_line = item[1]
    repo_url = repo_line['html_url']
    # Create a suitable folder name to store the cloned repo
    repo_name = repo_url[19:].replace('/', '-')
    local_path = os.path.join(local_path_main, repo_name)

    stage_list = []
    try:
        repo = Repository(repo_url, local_path)
        # Create a list of files for each workflow stage
        ml_stages = repo.get_ml_stages(
            stages_to_functions, functions_to_stages)
        # Create a list of stages that are implemented in this project
        for k in ml_stages:
            # Check if there is at least one file corresponding to this stage
            if ml_stages[k]:
                stage_list.append(k)
    except ValueError as e:
        # In case that there was an error cloning or loading the repository
        print(e)
        with skipped.get_lock():
            skipped.value += 1
        return None

    repo_line['ml_stages'] = stage_list
    return repo_line


def parallel_run():
    """Apply get_stage_list to all repositories. Uses multiple processed."""

    start_time = datetime.now()
    # Load data
    data_input = pd.read_csv(input_filepath)
    stages_to_functions, functions_to_stages = load_api_dict()
    columns = data_input.columns.to_list()+['ml_stages']
    data_output = pd.DataFrame(columns=columns)

    # Apply multiprocessing to obtain the results
    pool = multiprocessing.Pool(processes=n_processes)
    callback = partial(get_stage_list, stages_to_functions=stages_to_functions,
                       functions_to_stages=functions_to_stages)
    for i in tqdm.tqdm(pool.imap_unordered(callback, data_input.iterrows()), total=len(data_input)):
        # results.append(i)
        data_output = data_output.append(i)

    # Save results
    print(f"Skipped {skipped.value} out of {len(data_input)} repositories.")
    end_time = datetime.now()
    print("Program execution time: ", end_time-start_time)
    print(data_output)
    data_output.to_csv(output_filepath, index=False)


def single_run():
    """Apply get_stage_list to all repositories. Uses single process."""

    # Load data
    data_input = pd.read_csv(input_filepath)[:5]
    stages_to_functions, functions_to_stages = load_api_dict()
    columns = data_input.columns.to_list()+['ml_stages']
    data_output = pd.DataFrame(columns=columns)
    skipped = 0
    for i, repo in data_input.iterrows():
        new_line = get_stage_list(
            repo, stages_to_functions, functions_to_stages)
        data_output = data_output.append(new_line)
        if new_line['ml_stages'] is None:
            skipped += 1

    # Save results
    print(data_output)
    print("Skipped {} of {} repositories.".format(skipped, len(data_output)))
    data_output.to_csv(output_filepath, index=False)


if __name__ == "__main__":
    parallel_run()
