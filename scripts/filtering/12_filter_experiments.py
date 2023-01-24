import pandas as pd
import os

"""
Filter out all projects that are not considered experiments, but libraries,
tutorials, software systems or just trash.
Exclusion criteria:
    - Significant number of source files that are not Python or Jupyter Notebook
    - More than 2 contributor
    - More than 100 commits
    - More than 5 stars
    - More than 5 watching
    - No readme file or readme file less than 300 characters
"""

input_filepath = '../results_test/15-n_scripts/tf_skl_merged.csv'
output_filepath = '../results_test/16-experiments_01.csv'
local_path_main = '/mnt/volume1/mlexpmining/cloned_repos'
folder_stats_dir = '/mnt/volume1/mlexpmining/folder_stats'
n_processes = 4

def get_readme_len(repo):
    """Returns the length of the readme file (in characters). 0 if there is no readme."""
    readme_path=os.path.join(local_path_main,repo['local_folder'],'README.md')
    try:
        file=open(readme_path)
        return len(file.read())
    except (FileNotFoundError,UnicodeDecodeError):
        return 0

def get_python_share(repo):
    """Returns the fraction of Python files compared to other source files."""
    folder_stats_path=os.path.join(folder_stats_dir,repo['local_folder']+'.csv')
    df=pd.read_csv(folder_stats_path)
    extension_count=df['extension'].value_counts()
    python_count=extension_count['py']
    if 'npynb'in extension_count:
        python_count+=extension_count['npynb']
    languages_list=['java','c','cpp','cgi','pl','cs','h','php','sh','swift','vb','js','R','M','scala','html','css']
    other_languages_count=0
    for l in languages_list:
        if l in extension_count:
            other_languages_count+=extension_count[l]

    return python_count/(python_count+other_languages_count)

def has_keywords(repo,keywords):
    """Checks if given keywords occur in readme of the Repository."""
    readme_path=os.path.join(local_path_main,repo['local_folder'],'README.md')
    try:
        file=open(readme_path)
        readme_string=file.read().lower()
        for k in keywords:
            if k in readme_string:
                return True
    except (FileNotFoundError,UnicodeDecodeError):
        return False

def single_run():
    data_input=pd.read_csv(input_filepath)
    output_list=[]
    skipped = 0
    excluded_language=0
    excluded_contributors=0
    excluded_commits=0
    excluded_readme=0
    excluded_forks=0
    excluded_releases=0
    excluded_keywords=0
    excluded_tags=0
    remaining=0

    for i, repo in data_input.iterrows():
        print(f'{i}/{len(data_input)}')
        #Obtaining required information
        repo['python_share']=get_python_share(repo)
        repo['readme_len']=get_readme_len(repo)

        #Filtering
        excluded=False
        if repo['forks_count']>0:
            excluded_forks+=1
            excluded=True
        if repo['n_releases']>0:
            excluded_releases+=1
            excluded=True
        if repo['n_tags']>0:
            excluded_tags+=1
            excluded=True
        if has_keywords(repo,['tutorial','assignment','course','lecture','curriculum','university']):
            excluded_keywords+=1
            excluded=True
        if repo['python_share']<0.9:
            excluded_language+=1
            excluded=True
        # if repo['n_contributors']>1:
        #     excluded_contributors+=1
        #     excluded=True
        # if repo['n_commits']>100:
        #     excluded_commits+=1
        #     excluded=True
        if repo['readme_len']<300:
            excluded_readme+=1
            excluded=True
        if (not excluded):
            remaining+=1
            output_list.append(repo)

    #Printing results
    print(f"Excluded by language:\t\t{excluded_language}")
    print(f"Excluded by contributors:\t{excluded_contributors}")
    print(f"Excluded by commits:\t\t{excluded_commits}")
    print(f"Excluded by readme:\t\t{excluded_readme}")
    print(f"Excluded by forks:\t\t{excluded_forks}")
    print(f"Excluded by n_releases:\t\t{excluded_releases}")
    print(f"Excluded by n_tags:\t\t{excluded_tags}")
    print(f"Excluded by keywords:\t\t{excluded_keywords}")
    print(f"Remaining:\t\t\t{remaining}")

    #Saving results
    data_output=pd.DataFrame(output_list)
    data_output.to_csv(output_filepath,index=False)

if __name__=="__main__":
    single_run()
