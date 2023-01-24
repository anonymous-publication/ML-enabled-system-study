import numpy as np
import pandas as pd
import json
import os.path
import multiprocessing
import tqdm
from functools import partial
from datetime import datetime
import shutil

from Repository import Repository

"""
Filters out repositories that make no calls to scikit-learn/ tensorflow or cannot be cloned for analysis.
Note: Make sure that nbconvert is installed! Otherwise no error will be raised, all notebook files are just skipped.
"""

n_processes = 6
n_chunks = 10
local_path_main = '/mnt/volume1/mlexpmining/cloned_repos'
library = 'tensorflow'
input_filepath = f'../3-number_commits_filtered/{library}_dependents_14_02_2022.csv'
output_filepath = f'../4a-library_calls_filtered/{library}_14_02_2022.csv'

# Storing information about skipped and excluded repos
excluded = multiprocessing.Value('i', 0)
skipped = multiprocessing.Value('i', 0)


def process_chunks_parallel():
    """Splits the data in large chunks, calls parallel_run on them and saves the results for each chunk."""
    data_in = pd.read_csv(input_filepath)
    chunk_size = len(data_in)//n_chunks
    log_file = 'filtering_logs_{}.txt'.format(library)
    for i in range(n_chunks)[9:10]:
        start = i*chunk_size
        end = min(start+chunk_size, len(data_in))
        chunk = data_in[start:end]
        result = parallel_run(chunk)
        data_out = result[0]
        # Define the writing mode for the log file and the results .csv file.
        # For the first chunk: Create a new file. For subsequent chunks: append to that file
        mode = 'w' if i == 0 else 'a'
        data_out.to_csv(output_filepath, index=False,
                        mode=mode, header=(mode == 'w'))
        # Write metadata to log_file
        with open(log_file, mode) as f:
            f.write('Chunk number {} \n'.format(i))
            f.write("Total: {}\t".format(end-start))
            f.write("Skipped: {}\t".format(result[1]))
            f.write("Excluded: {}\t".format(result[2]))
            f.write("Remaining: {}\t".format(result[3]))
            f.write("Program execution time: {}\n".format(result[4]))


def repo_uses_library(item, library: str):
    """
    Determines wheter a given repository uses a given library.
    Args:
        repo_url:               String with the repository url
        stages_to_functions:    Name of the specified library, e.g. sklearn or tensorflow.
    Returns:                    True/False or None, if the repo cannot be cloned.
    """
    repo_line = item[1]
    repo_url = repo_line['html_url']
    repo_name = repo_url[19:].replace('/', '-')
    local_path = os.path.join(local_path_main, repo_name)
    # Try to create the repository for analysis
    try:
        # repo=Repository(repo_url,local_path)
        if os.path.exists(local_path):
            repo = Repository(repo_url, local_path)
        else:
            repo = Repository(repo_url)
        imports = repo.get_all_imports()
        # Check if there is at least 1 file that uses the specified library
        for i in imports:
            if i.startswith(library):
                repo_line['uses_library'] = True
                return repo_line
        with excluded.get_lock():
            excluded.value += 1
        repo_line['uses_library'] = False
        # shutil.rmtree(local_path)
        return repo_line

    except ValueError as e:
        # If the repository cannot be cloned return None
        with skipped.get_lock():
            skipped.value += 1
        print("An error occured. Skipping repository.")
        return None


def parallel_run(data_input=None):
    '''Apply repo_uses_library to all repos using multiple processes.'''

    start_time = datetime.now()

    # Loading the dictionary mapping ml workflow stages to sklearn modules
    # with open('workflow_stages_to_sklearn.json', 'r') as f:
    #     stages_to_sklearn = json.load(f)

    # Creating dictionaries mapping api calls to ml workflow stages and vice versa
    df = pd.read_csv('../4-api-dictionary/API-dictionary.csv',
                     usecols=[0, 1, 2, 3, 4, 5])
    stages_to_functions = {}
    functions_to_stages = {}
    for column_name in df:
        column = df[column_name].dropna()
        stages_to_functions[column_name] = list(column)
        for item in column:
            functions_to_stages[item] = column_name

    #  Loading the dependents of scikit-learn
    if data_input is None:
        data_input = pd.read_csv(input_filepath)
    columns = data_input.columns.to_list()+['uses_library']
    data_output = pd.DataFrame(columns=columns)

    # Execute the parallel filtering
    pool = multiprocessing.Pool(processes=n_processes)
    callback = partial(repo_uses_library, library=library)
    for i in tqdm.tqdm(pool.imap_unordered(callback, data_input.iterrows()), total=len(data_input)):
        data_output = data_output.append(i)

    # Print additional data for analysis
    print("Total:\t{}".format(len(data_input)))
    print("Skipped:\t{}".format(skipped.value))
    print("Excluded:\t{}".format(excluded.value))
    print("Remaining:\t{}".format(len(data_input)-skipped.value-excluded.value))
    end_time = datetime.now()
    execution_time = end_time-start_time
    print("Program execution time: ", execution_time)

    # Save results
    data_output = data_output[data_output['uses_library'] == True].drop(
        columns='uses_library')
    print(data_output)
    data_output.to_csv(output_filepath, index=False)


def single_run():
    '''Apply repo_uses_library to all repos using a single process.'''

    #  Loading the dependants of scikit-learn
    data_input = pd.read_csv(input_filepath)[:10]

    # Create dictionary for mapping of function calls to ml stages
    df = pd.read_csv('API-dictionary.csv', usecols=[0, 1, 2, 3, 4, 5])
    stages_to_functions = {}
    functions_to_stages = {}
    for column_name in df:
        column = df[column_name].dropna()
        stages_to_functions[column_name] = list(column)
        for item in column:
            functions_to_stages[item] = column_name

    skipped_repos = 0
    for i, row in data_input.iterrows():
        print(i)
        print(row.loc['full_name'])
        new_row = repo_uses_library((i, row), library)
        data_output = data_output.append(new_row)

    # Print additional data for analysis
    print("Total:\t{}".format(len(data_input)))
    print("Skipped:\t{}".format(skipped.value))
    print("Excluded:\t{}".format(excluded.value))
    print("Remaining:\t{}".format(len(data_input)-skipped.value-excluded.value))
    end_time = datetime.now()
    execution_time = end_time-start_time
    print("Program execution time: ", execution_time)

    # Save results
    data_output = data_output[data_output['uses_library'] == True].drop(
        columns='uses_library')
    print(data_output)


if __name__ == "__main__":
    parallel_run()
