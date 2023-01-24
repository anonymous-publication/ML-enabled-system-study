import pandas as pd
import os
import multiprocessing
import tqdm

"""Get the number of files for each repository and add results to the .csv file."""

# Adapt these filepaths depending on the storage location of your interim results
input_filepath = '../5-attributes/tensorflow.csv'
output_filepath = '../5-attributes/tensorflow.csv'
local_path_main = '/mnt/volume1/mlexpmining/cloned_repos'
n_processes = 4


def get_n_files(item):
    '''
    Get the number of files for one repo.
    Args:
        item:   Tuple (row_number, row). Returned by pd.DataFrame.iterrows()
    '''

    line = item[1]
    repo_url = line['html_url']
    repo_name = repo_url[19:].replace('/', '-')
    local_path = os.path.join(local_path_main, repo_name)
    if not os.path.exists(local_path):
        return None

    n_files = 0
    for root, dirs, files in os.walk(local_path):
        n_files += len(files)

    line['n_files'] = n_files

    return line


def single_run():
    '''Apply get_n_files to all repos from the .csv file. Uses a single process.'''

    # Load data
    data_input = pd.read_csv(input_filepath)[:10]
    columns = data_input.columns.to_list()+['n_files']
    data_output = pd.DataFrame(columns=columns)
    skipped = 0
    for i, repo in data_input.iterrows():
        new_line = get_n_files(repo)
        if new_line is None:
            skipped += 1
        else:
            data_output = data_output.append(new_line)

    # Save results
    print(data_output)
    print("Skipped {} of {} repositories.".format(skipped, len(data_output)))
    data_output.to_csv(output_filepath, index=False)


def parallel_run():
    '''Apply get_n_files to all repos from the .csv file. Uses multiple processes.'''

    # Load data
    data_input = pd.read_csv(input_filepath)
    columns = data_input.columns.to_list()+['n_files']
    data_output = pd.DataFrame(columns=columns)
    pool = multiprocessing.Pool(processes=n_processes)
    for i in tqdm.tqdm(pool.imap_unordered(get_n_files, data_input.iterrows()), total=len(data_input)):
        if i is not None:
            data_output = data_output.append(i)

    # Save results
    print(
        f'Skipped {len(data_input)-len(data_output)} of {len(data_input)} entries')
    print(data_output)
    data_output.to_csv(output_filepath, index=False)


if __name__ == "__main__":
    parallel_run()
