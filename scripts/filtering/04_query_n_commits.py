from utilities import retrieve_n_commits
import pandas as pd
import shutil
"""
Finds number of commits for all the repositories stored in .csv input_file with a "full_name" column that contains
the repositories on the format owner_username/repository_name.

Stores the number of commits as .csv in output_file.

Args:
    input_file:     String. .csv file containing one repository per line with a "full_name" column on the format owner_username/repository_name.
    output_file:    String. Filename of the .csv output file. The code will output two columns: "full_name" and "n_commits".
    username:       GitHub username placed in username.txt in the root directory of the project.
    token:          GitHub token placed in token.txt in the root directory of the project.
    checkpoint:     Integer. How often to make a copy of the output_file (e.g. to continue from a backup if the code crashes).
"""
input_file = "data/2-forks_removed/scikit-learn_dependents_14_02_2022.csv"
output_file = "data/3a-number_commits_queried/scikit-learn_dependents.csv"
username = open("username.txt", "r").readline()
token = open("token.txt", "r").readline()
checkpoint = 4000

repos = pd.read_csv(input_file)["full_name"].values
num_repos = len(repos)
df = pd.read_csv(output_file)
already_requested_repos = set(df["full_name"])
total_repos_requested = len(already_requested_repos)
columns = ["full_name","n_commits"]
df = pd.DataFrame(columns = columns)    # initiate a new empty dataframe after already_requested_repos has been retrieved to save RAM

for i,repo in enumerate(repos):
    if repo in already_requested_repos:
        continue
    already_requested_repos.add(repo)

    n_commits = retrieve_n_commits(repo, (username, token))
    if n_commits is not None:
        df.loc[df.shape[0]] = [repo, n_commits]

    if total_repos_requested % checkpoint == 0 and total_repos_requested != 0:
        # checkpoint
        shutil.copyfile(output_file, f"{output_file.split('.')[0]}_{total_repos_requested}.csv")
        df.to_csv(output_file, mode='a', index=False, header=False)
        df = pd.DataFrame(columns = columns)    # wipe the df to save RAM

    total_repos_requested += 1
    print(f'Progress: {total_repos_requested}/{num_repos} ({round(total_repos_requested*100/num_repos,2)}%)')

# all requests have been made, save and terminate
shutil.copyfile(output_file, f"{output_file.split('.')[0]}_{total_repos_requested}.csv")
df.to_csv(output_file, mode='a', index=False, header=False)
