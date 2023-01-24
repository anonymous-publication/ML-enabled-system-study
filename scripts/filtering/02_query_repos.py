from utilities import get_list_from_file, query_repo, list_to_str
import pandas as pd
import shutil
"""
Queries all repositories stored in input_file on the format owner_username/repository_name, with one repository per line, e.g.:

tensorflow/tensorflow
scikit-learn/scikit-learn

Stores relevant information about them as .csv in output_file (see the columns variable to see what information is stored).

Args:
    input_file:     String. .txt file containing one repository per line on the format owner_username/repository_name.
    output_file:    String. Filename of the .csv output file. The code will store the information described by the columns variable down below.
    username:       GitHub username placed in username.txt in the root directory of the project.
    token:          GitHub token placed in token.txt in the root directory of the project.
    checkpoint:     Integer. How often to make a copy of the output_file (e.g. to continue from a backup if the code crashes).
"""
input_file = "data/1-dependents/scikit-learn_dependents_20_01_2022.txt"
output_file = "data/1a-dependents_queried/scikit-learn_dependents.csv"
username = open("username.txt", "r").readline()
token = open("token.txt", "r").readline()
checkpoint = 4000

repos = get_list_from_file(input_file)
num_repos = len(repos)
df = pd.read_csv(output_file)
already_requested_repos = set(df["full_name"])
total_repos_requested = len(already_requested_repos)
columns = ["full_name", "html_url", "fork", "forks_count", "stargazers_count", "is_template",
           "has_wiki", "organization", "language", "topics", "description", "created_at",
           "pushed_at", "updated_at", "size", "watchers_count", "has_issues", "has_projects",
           "has_downloads", "has_pages", "open_issues_count", "allow_forking", "network_count", "subscribers_count",
           "default_branch", "license_name", "license_key"]
# initiate a new empty dataframe after already_requested_repos has been retrieved to save RAM
df = pd.DataFrame(columns=columns)

for i, repo in enumerate(repos):
    if repo in already_requested_repos:
        continue
    already_requested_repos.add(repo)

    response_json = query_repo(repo, (username, token))
    if response_json is not None:
        to_add = []
        for col_name in columns:
            if col_name == "topics":
                # topics is a list, convert it to string separated by |
                to_add.append(list_to_str(response_json[col_name]))
            elif col_name == "organization":
                try:
                    to_add.append(response_json["organization"]["login"])
                except KeyError:
                    # does not belong to an organization
                    to_add.append("")
            elif col_name == "license_name":
                try:
                    to_add.append(response_json["license"]["name"])
                except TypeError:
                    # does not have a license name
                    to_add.append("")
            elif col_name == "license_key":
                try:
                    to_add.append(response_json["license"]["key"])
                except TypeError:
                    # does not have a license key
                    to_add.append("")
            else:
                to_add.append(response_json[col_name])
        df.loc[df.shape[0]] = to_add

    if total_repos_requested % checkpoint == 0 and total_repos_requested != 0:
        # checkpoint
        shutil.copyfile(
            output_file, f"{output_file.split('.')[0]}_{total_repos_requested}.csv")
        df.to_csv(output_file, mode='a', index=False, header=False)
        df = pd.DataFrame(columns=columns)    # wipe the df to save RAM

    total_repos_requested += 1
    print(
        f'Progress: {total_repos_requested}/{num_repos} ({round(total_repos_requested*100/num_repos,2)}%)')

# all requests have been made, save and terminate
shutil.copyfile(
    output_file, f"{output_file.split('.')[0]}_{total_repos_requested}.csv")
df.to_csv(output_file, mode='a', index=False, header=False)
