import pandas as pd
import os
import multiprocessing
import tqdm
from functools import partial
import copy

from utilities import retrieve_metric

"""Obtain the numnber of contributors/branches/pull requests/tags/releases for each repository using the GitHub REST api."""

# Adapt these filepaths for each attribute added
input_filepath = '../4a-library_calls_filtered/tensorflow.csv'
output_filepath = '../5-attributes/tensorflow.csv'

username = open("username.txt", "r").readline()[:-1]
token = open("token.txt", "r").readline()[:-1]

input_df = pd.read_csv(input_filepath)
columns = input_df.columns.to_list()+['n_issues']
output_df = pd.DataFrame(columns=columns)

for i, line in input_df.iterrows():
    repo = line.loc['full_name']
    print(repo)
    print(f'{i}/{len(input_df)}')

    n_issues = retrieve_metric(
        repo, (username, token), 'issues', command='state=all')
    if n_issues is not None:
        new_line = copy.deepcopy(line)
        new_line['n_issues'] = n_issues
        output_df = output_df.append(new_line, ignore_index=True)
        # output_df=pd.concat([new_line,output_df.loc[:]]).reset_index(drop=True)

print(output_df)
output_df.to_csv(output_filepath, index=False)
