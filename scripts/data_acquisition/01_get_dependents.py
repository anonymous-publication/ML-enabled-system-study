import requests
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup
import re
import time

"""
Gets the total number of dependents for library_name and saves the list of dependents to a .txt file.
Args:
    library_name:   String. Library name on the format owner_username/repository_name, e.g. tensorflow/tensorflow
    output_dir:     String. Directory of where to save the .txt file, e.g. results_test/
    username:       GitHub username placed in username.txt in the root directory of the project.
    token:          GitHub token placed in token.txt in the root directory of the project.
"""
library_name = "tensorflow/tensorflow"
output_dir = "data/1-dependents/"
username = open("username.txt", "r").readline()
token = open("token.txt", "r").readline()

NXT_BTN_PATTERN = """(?<=dependents\?dependents_after=)[\s\S]*?(?=\")"""
BASE_URL = f"https://github.com/{library_name}/network/dependents"
DEPENDENTS_PREFIX = "?dependents_after="
next_dependents_page_url = BASE_URL

DEPENDENTS_PATTERN1 = """(?<=<a class="text-bold" data-hovercard-type="repository" data-hovercard-url=").*?(?<=hovercard" href="\/).*?(?=">)"""
DEPENDENTS_PATTERN2 = """(?<=href="\/).*"""
all_dependets = []


def get_num_dependents(library_name, username, token):
    """
    Get the total number of dependents for library_name.
    Args:
        library_name:   String. Library name on the format owner_username/repository_name, e.g. tensorflow/tensorflow
    Returns:        Integer number of dependents for library_name.
    """
    url = f"https://github.com/{library_name}/network/dependents"
    response = requests.get(url, auth=HTTPBasicAuth(username, token))
    soup_object = BeautifulSoup(response.content, "html.parser")
    dependents_div = soup_object.find(id="dependents")
    dependents_num_repos_class = dependents_div.find_all(
        "div", class_="table-list-header-toggle states float-left pl-0")
    num_dependents = re.findall(
        """(?<=<\/svg>)([\s\S]*)(?=Repositories)""", dependents_num_repos_class[0].prettify())
    num_dependents = int("".join(re.findall("\d", num_dependents[0])))
    return num_dependents


# This currently does not work, because GitHub changed something about the html laylout
num_dependents = get_num_dependents(library_name, username, token)
num_dependents_retrieved = 0
while True:
    response = requests.get(next_dependents_page_url,
                            auth=HTTPBasicAuth(username, token))
    if response.status_code == 429:
        # 429: too many requests, wait then continue
        retry_after = int(
            response.raw.headers._container['retry-after'][1]) + 10
        print(
            f"429 (too many requests): {next_dependents_page_url} retrying after {str(retry_after)}")
        time.sleep(retry_after)
        continue

    # page retrieved succesfully, proceed
    soup_object = BeautifulSoup(response.content, "html.parser")
    dependents_div = soup_object.find(id="dependents")

    # retrieve the dependents
    dependent_href_div_class = dependents_div.find_all(
        "div", class_="Box-row d-flex flex-items-center")
    dependent_repositories = list(map(lambda i: re.findall(
        DEPENDENTS_PATTERN1, i.prettify()), dependent_href_div_class))
    dependent_repositories = list(map(lambda i: re.findall(
        DEPENDENTS_PATTERN2, i[0]), dependent_repositories))
    num_dependents_retrieved += len(dependent_repositories)
    print(
        f'Progress: {num_dependents_retrieved}/{num_dependents} ({round(num_dependents_retrieved*100/num_dependents,2)}%)')
    all_dependets.extend(dependent_repositories)

    # retrieve the url for the next page
    button_div_class = dependents_div.find_all("div", class_="BtnGroup")
    next_url_matches = re.findall(
        NXT_BTN_PATTERN, button_div_class[0].prettify())
    if len(next_url_matches) == 0:
        # no next page with dependents, break the loop
        break
    next_dependents_page_url = BASE_URL + DEPENDENTS_PREFIX + \
        re.findall(NXT_BTN_PATTERN, button_div_class[0].prettify())[0]

# save all the dependents to a .txt file
output_filename_prefix = output_dir+library_name.split("/")[1]
textfile = open(f"{output_filename_prefix}_dependents.txt", "w")
for dependent in all_dependets:
    if dependent[0] != all_dependets[-1][0]:
        textfile.write(dependent[0] + "\n")
    else:
        textfile.write(dependent[0])
textfile.close()
