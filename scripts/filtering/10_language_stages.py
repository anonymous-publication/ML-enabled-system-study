import pandas as pd
import os
import multiprocessing

"""
Adds names of local directories to the dataframe.
Filters out repos with empty ml_stages or not Python as primary language.
"""

library = 'tensorflow'
local_path_main = '/mnt/volume1/mlexpmining/cloned_repos'
input_path = f'../results_test/12-n_files/{library}.csv'
output_path = f'../results_test/13-local_path/{library}.csv'

not_stored = multiprocessing.Value('i', 0)
no_stages = multiprocessing.Value('i', 0)
wrong_language = multiprocessing.Value('i', 0)


def process_row(row):
    '''
    Args:
        row:    row of the pandas dataframe describing the repo.
    Returns:    The row with the local path added.
                None, if local_path not found, ml_stages empty, or language!=Python
    '''
    # Getting the local path
    repo_url = row['html_url']
    repo_name = repo_url[19:].replace('/', '-')
    local_path = os.path.join(local_path_main, repo_name)

    # Filtering
    if not os.path.exists(local_path):
        with not_stored.get_lock():
            not_stored.value += 1
        return None
    if (row['language'] != 'Python') and (row['language'] != 'Jupyter Notebook'):
        with wrong_language.get_lock():
            wrong_language.value += 1
        return None
    if row['ml_stages'] == '[]':
        with no_stages.get_lock():
            no_stages.value += 1
        return None

    row['local_folder'] = repo_name
    return row


def single_run():
    '''Apply process_row to all repos using a single process.'''

    #  Loading the dependants of scikit-learn
    data_input = pd.read_csv(input_path)
    output_list = []
    for i, row in data_input.iterrows():
        print(f'{i:5d}/{len(data_input)}')
        new_row = process_row(row)
        if new_row is not None:
            output_list.append(new_row)

    data_output = pd.DataFrame(output_list)
    print(data_output)
    print(f'Not stored:\t{not_stored.value}')
    print(f'Wrong language:\t{wrong_language.value}')
    print(f'No ml stages:\t{no_stages.value}')
    data_output.to_csv(output_path)


if __name__ == '__main__':
    single_run()
