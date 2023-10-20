import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import itertools
# from utilities.Repository import Repository
# from utilities.utilities import load_api_dict
import multiprocessing
import tqdm

""""Analze which and how many ml development phases occur in one file"""

input_filepath = '../data/5-attributes/tf_skl_merged.csv'
output_filepath = '../data/results/stages_per_file.csv'
fig_filepath = '../data/results/stages_per_file.png'
#This is where the cloned repos are stored
local_path_main = '/mnt/volume1/mlexpmining/cloned_repos'
N_PROCESSES = 4
FIGSIZE=(12,6)
WSPACE=0.5
plt.rcParams.update({'font.size': 16})


def get_stage_count(repo):
    """Determine how many files incorporate multiple ml stages"""

    stage_count={
        0:0,
        1:0,
        2:0,
        3:0,
        4:0,
        5:0,
        6:0
    }
    Repo=Repository(local_dir=local_path_main+'/'+repo['local_folder'])
    stages_to_functions, functions_to_stages = load_api_dict()
    stages_to_files=Repo.get_ml_stages(stages_to_functions,functions_to_stages)
    #Create a list of files implementing an ml stage
    file_list=[]
    #Convert the dictionary into a list of files
    for v in stages_to_files.values():
        file_list+=v
    #Count occurences of all file names in the list to get the number of ml stages implemented by the file
    for file in set(file_list):
        n_stages=file_list.count(file)
        stage_count[n_stages]+=1
    return stage_count

def get_stage_combination(item):
    repo=item[1]
    Repo=Repository(local_dir=local_path_main+'/'+repo['local_folder'])
    stages_to_functions, functions_to_stages = load_api_dict()
    stages_to_files=Repo.get_ml_stages(stages_to_functions,functions_to_stages)
    #Invert the dictionary to get the stages implemented in each file
    file_list=set(itertools.chain.from_iterable(stages_to_files.values()))
    files_to_stages={i:[] for i in file_list}
    for k, v in stages_to_files.items():
        for i in v:
            files_to_stages[i].append(k)
    #Create a list of all stage combinations
    combinations_list=[]
    for k,v in files_to_stages.items():
        combinations_list.append(' '.join(v))
    combinations_set=set(combinations_list)
    #Count the occurences of each combination
    combinations_count={}
    for c in combinations_set:
        combinations_count[c]=combinations_list.count(c)

    return combinations_count

def single_run():
    '''Apply gest_stage_count to all repos from the .csv file. Uses a single process.'''

    # Load data
    data_input = pd.read_csv(input_filepath)
    skipped = 0
    results_list=[]
    print(data_input)
    for i, repo in data_input.iterrows():
        stage_count=get_stage_count(repo)
        stage_count[0]=repo['n_scripts']-repo['ml_files']
        results_list.append(stage_count)

    results_df=pd.DataFrame.from_records(results_list)
    # Save results
    mean=results_df.mean()
    print(results_df)
    print(mean)
    #mean_share=[i/np.sum(mean)for i in mean]
    mean.to_csv(output_filepath,index=False)

    # print("Skipped {} of {} repositories.".format(skipped, len(data_output)))
    # data_output.to_csv(output_filepath, index=False)

def plot_count():
    data=pd.read_csv(output_filepath[3:])
    print(data)
    fig=plt.figure(figsize=FIGSIZE)
    plt.bar(data.iloc[:,0],data.iloc[:,1],width=0.5, color='grey', edgecolor='black')
    plt.xlabel('Number of stages')
    plt.ylabel('Average number of files')
    plt.show()
    # fig.savefig('fig/stages_per_file.pdf',bbox_inches='tight',pad_inches=0)

def count_combinations():
    '''Analyze which combinations of ml stages frequently occur together'''
    # Load data
    data_input = pd.read_csv(input_filepath)[:100]
    skipped = 0
    results_list=[]
    print(data_input)
    for i, repo in data_input.iterrows():
        print(f'{i}/{len(data_input)}')
        results_list.append(get_stage_combination(repo))

    results_df=pd.DataFrame(results_list).fillna(0).sum().astype(int)
    print(results_df)
    results_df.to_csv('data/results/file_stage_combination.csv',index=True)

def count_combinations_parallel():
    # Load data
    data_input = pd.read_csv(input_filepath)
    skipped = 0
    results_list=[]
    print(data_input)

    #Apply multiprocessing to get the results
    pool = multiprocessing.Pool(processes=N_PROCESSES)
    results_list = []
    for i in tqdm.tqdm(pool.imap_unordered(get_stage_combination, data_input.iterrows()), total=len(data_input)):
        results_list.append(i)

    #Save the reuslts
    results_df=pd.DataFrame(results_list).fillna(0).sum().astype(int)
    print(results_df)
    results_df.to_csv('data/results/file_stage_combination.csv',index=True)

def analyze_combinations():
    data=pd.read_csv('data/results/file_stage_combination.csv',names=['stages','count'])
    data=data.sort_values(by='count',ascending=False)
    print(data[:20])
    # print(data.loc[data['count']>=100])

if __name__ == "__main__":
    # single_run()
    #count_combinations_parallel()
    analyze_combinations()
    plot_count()