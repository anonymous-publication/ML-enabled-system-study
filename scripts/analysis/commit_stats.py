import pandas as pd
import os
from datetime import datetime

"""Analyze the results from commit_stages"""

# To be updated with the final results
RESULTS_DIR = "../commit_stages"
stages = ['Acquisition', 'Preparation', 'Modeling',
          'Training', 'Evaluation', 'Prediction']


def change_rate(filepath):
    """
    Determine the proportion of commits affecting each ml stage
    Args:
        filepath:   .csv file containing the commit_stages info
    Returns:    Dictionaty with the proportion of commits affecting each stage and the time in days between first and last commit
    """

    df = pd.read_csv(filepath)
    n_commits = len(df)
    n_changes = {c: 0 for c in stages}
    for stage in stages:
        commits = (df.loc[:, stage])
        relevant_commits = commits[commits > 0]
        n_changes[stage] = len(relevant_commits)/n_commits

    n_changes['n_commits'] = n_commits
    start_date = datetime.strptime(
        (df.loc[:, 'time'].iloc[0]), '%Y-%m-%d %H:%M:%S%z')
    end_date = datetime.strptime(
        (df.loc[:, 'time'].iloc[-1]), '%Y-%m-%d %H:%M:%S%z')
    n_changes['time'] = (end_date-start_date).days

    return n_changes


def single_run():
    """Bin the data depending on the number of commits and calculate averge change rate for each froup and stage."""

    # Get change rate
    filepaths = os.listdir(RESULTS_DIR)
    results_list = []
    for f in filepaths:
        filepath = os.path.join(RESULTS_DIR, f)
        new_line = change_rate(filepath)
        results_list.append(new_line)
    result_df = pd.DataFrame(results_list)
    print(result_df)

    # Bin results
    result_df['bins'] = pd.cut(result_df['n_commits'], [
                               50, 70, 100, 150, 300, 1000000])
    # Print mean values
    print('Proportion of commits affecting each ml stage:')
    print(result_df.groupby(['bins']).mean())
    print('Number of repositories in each bin')
    print(result_df.groupby(['bins']).count())


if __name__ == "__main__":
    single_run()
