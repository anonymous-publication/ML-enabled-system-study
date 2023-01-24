import pandas as pd
"""
Combines a .csv file containing number of commits with a .csv file containing other repository information.
Outputs the combined dataframe to a .csv file with the name "library_dependents_appended" in the 3a-number_commits_queried folder.
Also filters out the repositories with less than commit_threshold commits and outputs to 3-number_commits_filtered folder.
"""
library = "scikit-learn"
commit_threshold = 50

repo_info_df = pd.read_csv(
    f"../2-forks_removed/{library}_dependents_14_02_2022.csv")
repo_info_df['n_commits'] = -1
commits_df = pd.read_csv(
    f"../3a-number_commits_queried/{library}_dependents.csv")

max_idx = len(commits_df)
for index, row in commits_df.iterrows():
    repo_info_df.loc[repo_info_df["full_name"] ==
                     row["full_name"], "n_commits"] = row["n_commits"]
    if index % 1000 == 0 and index != 0:
        print(f"{round(index*100/max_idx,2)} %")

repo_info_df.to_csv(
    f"../3a-number_commits_queried/{library}_dependents_appended.csv", index=False)
repo_info_df = repo_info_df[repo_info_df["n_commits"] >= commit_threshold]
repo_info_df.to_csv(
    f"../3-number_commits_filtered/{library}_dependents_14_02_2022.csv", index=False)
