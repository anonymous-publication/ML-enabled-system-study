import pandas as pd
import os
import multiprocessing
import tqdm
from functools import partial

from utilities.Repository import Repository

"""Add the number of ml files for each project to the summary file."""

CLONED_REPO_DIR = '/mnt/volume1/mlexpmining/cloned_repos'
summary_merged_path = '../results_test/13-local_path/merged.csv'
output_path = '../results_test/14-ml_files/tf_skl_merged.csv'

commit_stages_results = '../results_test/commit_stages'
N_PROCESSES = 4


def get_ml_files(item, stages_to_functions, functions_to_stages):
    '''Calculate the number of ml files for a given repo.
    Args:
        item:   tuple (i, row). Returned by pd.DataFrame.iterrows(). Row is the relevant line from the summary file.
        stages_to_functions, functions_to_stages:   Mapping function calls to ml stages and vice versa.
    Returns:    The row with the numeber of ml stages added.
    '''
    row = item[1]
    path = os.path.join(CLONED_REPO_DIR, row['local_folder'])
    repo = Repository(local_dir=path)
    ml_files = repo.get_ml_files(stages_to_functions, functions_to_stages)
    row['ml_files'] = len(ml_files)
    return row


def single_run():
    # Load data
    df = pd.read_csv(summary_merged_path).drop('Unnamed: 0', axis=1)[:100]
    results_list = []
    print(df)

    # Create dictionary for mapping of function calls to ml stages
    api_dict = pd.read_csv('API-dictionary.csv', usecols=[0, 1, 2, 3, 4, 5])
    stages_to_functions = {}
    functions_to_stages = {}
    for column_name in api_dict:
        column = api_dict[column_name].dropna()
        stages_to_functions[column_name] = list(column)
        for item in column:
            functions_to_stages[item] = column_name

    for i, row in df.iterrows():
        print(i)
        new_line = get_ml_files(row, stages_to_functions, functions_to_stages)
        results_list.append(new_line)
    results_df = pd.DataFrame(results_list)
    print(results_df)
    results_df.to_csv(output_path, index=False)


def parallel_run():
    '''Apply repo_uses_library to all repos using multiple processes.'''

    # Creating dictionaries mapping api calls to ml workflow stages and vice versa
    df = pd.read_csv('API-dictionary.csv', usecols=[0, 1, 2, 3, 4, 5])
    stages_to_functions = {}
    functions_to_stages = {}
    for column_name in df:
        column = df[column_name].dropna()
        stages_to_functions[column_name] = list(column)
        for item in column:
            functions_to_stages[item] = column_name

    #  Loading the dependents of scikit-learn
    data_input = pd.read_csv(summary_merged_path)

    # Execute the parallel filtering
    pool = multiprocessing.Pool(processes=N_PROCESSES)
    callback = partial(get_ml_files, stages_to_functions=stages_to_functions,
                       functions_to_stages=functions_to_stages)
    results_list = []
    for i in tqdm.tqdm(pool.imap_unordered(callback, data_input.iterrows()), total=len(data_input)):
        results_list.append(i)

    # Save results
    results_df = pd.DataFrame(results_list)
    print(results_df)
    results_df.to_csv(output_path, index=False)


if __name__ == '__main__':
    parallel_run()
    # single_run()
