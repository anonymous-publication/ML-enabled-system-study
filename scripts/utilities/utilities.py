import requests
from requests.models import Response
from requests.auth import HTTPBasicAuth
import time
import pandas as pd
import numpy as np
from collections import Counter
import json
import os
import re
from treelib import Tree,Node

from utilities.Repository import Repository


def get_repos(search_params: dict, auth_params, n_repos=None):
    """
    Returns a list of repos based on given search criteria. Uses keyword search.
    Args:
        search_params:  Dictionary of paremeters for the request to the GitHub REST API.
        auth_params:    Tuple (username, token). Used for authentication to GitHub.
        n_repos:        Macimum number of repositories to be returnes. None means that all repositories that are found  will be returned.
    Returns:    List of dictionaries containing the metadata for the repositories found.
    """

    response = requests.get(
        "https://api.github.com/search/repositories", params=search_params, auth=auth_params)
    current_page = response.url
    n_results = response.json()['total_count']
    last_page = response.links['last']['url']
    results = []
    while(True):
        print(current_page)

        response = requests.get(current_page)

        # Stop if the required number of results has been obtained
        if n_repos is not None:
            page_num = int(current_page.split('&page=')[1])
            remaining = n_repos-(search_params['per_page']*page_num)
            if remaining <= 0:
                limit = search_params['per_page']+remaining
                for i in response.json()['items'][:limit]:
                    results.append({
                        'html_url': i['html_url'],
                        'stargazers_count': i['stargazers_count'],
                        'forks_count': i['forks_count'],
                        'topics': i['topics'],
                        'language': i['language']
                    })
                break

        # Append the repo urls to the results
        for i in response.json()['items']:
            results.append(i)

        # Get the next page if there is one, else end the loop
        if (current_page != last_page):
            current_page = response.links['next']['url']
            # Wait every 10th page because of request limit of the Git REST API
            if (current_page[-2:] == '10'):
                time.sleep(60)
        else:
            break

    return (results, n_results)


def get_avg_features(repos: list, auth_params, results: dict = {}):
    """
    Extract average data for a list of repositories.
    Args:
        repos:          List of dicts. Each containing relevant data for the repository to be analyzed.
        auth_params:    Tuple (username, token). Used for authentication to GitHub.
        results:        Dictionary of features that are already provided.
    Returns:            Dictionary with the results
    """

    n_commits = []
    n_forks = []
    n_stars = []
    topics = []
    languages = []
    for r in repos:
        n_forks.append(r['forks_count'])
        n_stars.append(r['stargazers_count'])
        topics = topics+r['topics']
        languages.append(r['language'])
        n_commits.append(get_n_commits(r['url'], auth_params))

    # Calculate mean values
    results['avg_commits'] = np.mean(n_commits)
    results['avg_forks'] = np.mean(n_forks)
    results['avg_stars'] = np.mean(n_stars)

    results['most_common_topics'] = str([i[0]
                                         for i in (Counter(topics).most_common(10))])
    results['most_common_languages'] = str([i[0]
                                            for i in (Counter(languages).most_common(3))])

    return results


def get_n_commits(repo_url, auth_params):
    """
    Determine the number of commits for a repository using the GitHub REST API.
    Args:
        repo_url:       Url of the repository to be analyzed.
        auth_params:    Tuple (username, token). Used for authentication to GitHub.
    """
    commits_url = repo_url+"/commits"
    commits_response = requests.get(
        commits_url, auth=auth_params, params={'per_page': 100})
    links = commits_response.links
    if links:
        # If there is more than one page of commits, sum up everything
        last_page_url = links['last']['url']
        n_pages = int(last_page_url.split('&page=')[1])
        last_page = requests.get(
            last_page_url, auth=auth_params, params={'per_page': 100})
        last_page_len = len(last_page.json())
        n_commits = n_pages*100+last_page_len
    else:
        # If there is only one page, the number of commits on this page suffices
        n_commits = len(commits_response.json())
    return n_commits


def get_list_from_file(file):
    with open(file) as f:
        rv = f.readlines()
    rv = list(map(lambda i: i.replace("\n", ""), rv))
    return rv


def combine_lists(lsts):
    # combines lists and removes duplicates
    if len(lsts) == 1:
        raise Exception("combine_lists: len(lsts) must be > 1")

    rv = set(lsts[0])
    for i in range(1, len(lsts)):
        rv = set.union(rv, lsts[i])
    return list(rv)


def list_to_txt_file(lst, file):
    textfile = open(file, "w")
    for element in lst:
        if element != lst[-1]:
            textfile.write(element + "\n")
        else:
            textfile.write(element)
    textfile.close()


def safe_api_query(url, auth_params, req_params=None):
    """
    Queries a GitHub API URL and handles the reponse status codes:
        200 (OK): returns the response object.
        429 (too many requets): waits before trying again.
        403 (forbidden): waits if rate limit exceeded, returns None if the repository has been blocked.
        404 (not found): returns None.
        451 (DMCA takedown): returns None.
        502 (bad gateway): sleeps for 5 minutes before retrying.
        409 (empty repo): returns None.
        Other code: Returns None and writes to "failed_repos.txt" with the status code and the url of repository
                    that triggered it to be handled later.
    Args:
        url:                String. GitHub API URL.
        auth_params:        Tuple (username, token). Used for authentication to GitHub.
        req_params:         Dict. Additional request parameters to make the request to the GitHub API with,
                            e.g.  {'per_page': 100}.
    Returns:                Response object if successful, None if repository cannot be found or has been blocked.
    """
    while True:
        try:
            if req_params is not None:
                response_object = requests.get(
                    url, auth=auth_params, params=req_params)
            else:
                response_object = requests.get(url, auth=auth_params)
        except requests.exceptions.ConnectionError:
            print("Connection dropped. Sleeping for 5 minutes before retrying.")
            time.sleep(60*5)
            continue
        if response_object.status_code == 200:
            return response_object
        elif response_object.status_code == 429:
            # too many requests
            retry_after = int(
                response_object.raw.headers._container['retry-after'][1]) + 10
            print(
                f"429 (too many requests): {url} retrying after {retry_after} s")
            time.sleep(retry_after)
        elif response_object.status_code == 403:
            # forbidden, usually because the rate limit has been exceeded
            if response_object.text.count("Repository access blocked"):
                # forbidden not because of rate limit
                return None
            reset_epoch = response_object.raw.headers._container['x-ratelimit-reset'][1]
            ratelimit_remaining = int(
                response_object.raw.headers._container['x-ratelimit-remaining'][1])
            current_epoch = int(time.time())
            to_wait = int(reset_epoch) - current_epoch + 10
            if ratelimit_remaining == 0:
                print(f"403 (forbidden): {url} retrying after {to_wait} s")
                time.sleep(to_wait)
        elif response_object.status_code == 404:
            # repo privatized or deleted
            return None
        elif response_object.status_code == 451:
            # repository disabled due to a DMCA takedown
            return None
        elif response_object.status_code == 502:
            # bad gateway, sleep for 5 minutes then try again
            print("502: Bad gateway. Sleeping for 5 minutes before retrying.")
            time.sleep(60*5)
        elif response_object.status_code == 409:
            # repository is empty
            return None
        else:
            with open("failed_repos.txt", 'a+') as f:
                f.write(f"{response_object.status_code},{url}\n")
            return None


def query_repo(repo, auth_params):
    """
    Queries the GitHub API for a repository and returns the information about it as a dict, otherwise None
    if the repository is blocked or cannot be found.
    Args:
        repo:               String. Library name on the format owner_username/repository_name, e.g. tensorflow/tensorflow.
        auth_params:        Tuple (username, token). Used for authentication to GitHub.
    Returns:                Dict if successful, None if repository cannot be found or has been blocked.
    """
    url = f"https://api.gNoneithub.com/repos/{repo}"
    response_object = safe_api_query(url, auth_params)
    if response_object is not None:
        return json.loads(response_object.content)
    else:
        return None


def retrieve_n_commits(repo, auth_params):
    """
    Determine the number of commits for a repository using the GitHub REST API.
    Args:
        repo_url:       String. Library name on the format owner_username/repository_name, e.g. tensorflow/tensorflow.
        auth_params:    Tuple (username, token). Used for authentication to GitHub.
    Returns:            Integer if successful, None if repository cannot be found or has been blocked.
    """
    url = f"https://api.github.com/repos/{repo}/commits?per_page=1"
    response_object = safe_api_query(url, auth_params)
    if response_object is not None:
        n_commits = re.findall("\d+$", response_object.links['last']['url'])
        n_commits = int(n_commits[0])
        return n_commits
    else:
        return None

def retrieve_metric(repo, auth_params,metric,command=None):
    """
    Determine the number of contributors/pull requests/branches/tags/releases for a repository using the GitHub REST API.
    Args:
        repo_url:       String. Library name on the format owner_username/repository_name, e.g. tensorflow/tensorflow.
        auth_params:    Tuple (username, token). Used for authentication to GitHub.
    Returns:            Integer if successful, None if repository cannot be found or has been blocked.
    """
    url = f"https://api.github.com/repos/{repo}/{metric}?per_page=1&{command}"
    response_object = safe_api_query(url, auth_params)
    if response_object is None:
        return None
    elif response_object.json()==[]:
        return 0
    else:
        try:
            metric = re.findall("\d+$", response_object.links['last']['url'])
            metric = int(metric[0])
            return metric
        except KeyError:
            #No links means only 1 page
            return 1


def list_to_str(lst):
    # uses | as separator
    rv = ""
    for element in lst:
        if element != lst[-1]:
            rv += (element + "|")
        else:
            rv += element
    return rv

def repo_has_ml_stages(repo_url,stages_to_functions:dict,functions_to_stages:dict):
    """
    Determines wheter a given repository uses a given library.
    Args:
        repo_url:               String with the repository url
        stages_to_functions:    Dicitonary mapping the stages of machine learning workflow to functions from ml libraries.
    Returns:                    True/False or None, if the repo cannot be cloned.
    """
    try:
        #Get mapping of ml stages to modules of the given library
        repo=Repository(repo_url)
        stages=repo.get_ml_stages(stages_to_functions, functions_to_stages)
        #Check if there is at least 1 file that uses the specified library
        for value in stages.values():
            if len(value) > 0:
                return True
        return False
    except ValueError as e:
        #If the repository cannot be cloned return None
        print("An error occured. Skipping repository.")
        return None



def load_tree(json_tree,tree=None, node=None):
    k, value = list(json_tree.items())[0]

    #Initialization
    if tree is None:
        tree=Tree()
        node=tree.create_node(k,'root').identifier

    for counter,value in enumerate(json_tree[k]['children']):
        if isinstance(json_tree[k]['children'][counter], str):
            tree.create_node(tag=value, parent=node)
        else:
            new_node=tree.create_node(tag=list(value)[0], parent=node)
            tree=load_tree(json_tree[k]['children'][counter],tree, new_node )

    return tree

def load_api_dict():
    """
    Loads the dictionary mapping api calls to ml workflow stages
    Returns: Tuple: stages_to_functions, funcitons_to_stages
    """
    df=pd.read_csv('API-dictionary.csv',usecols=[0,1,2,3,4,5])
    stages_to_functions={}
    functions_to_stages={}
    for column_name in df:
        column=df[column_name].dropna()
        stages_to_functions[column_name]=list(column)
        for item in column:
            functions_to_stages[item]=column_name

    return stages_to_functions,functions_to_stages

def mergeDictionary(dict_1, dict_2, sum_values = True):
   dict_3 = {**dict_1, **dict_2}
   for key, value in dict_3.items():
       if key in dict_1 and key in dict_2:
            if sum_values:
               dict_3[key] = value + dict_1[key]
            else:
                dict_3[key] = [dict_1[key], value]
   return dict_3

