import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import math

"""Analyze the share of ml files per project"""

results_path='../results_test/15-n_scripts/tf_skl_merged.csv'

def analyze_ml_files(bin_by,bins):
    #Load data
    data=pd.read_csv(results_path)

    n_files=data.loc[:,'n_scripts'].to_numpy()
    ml_files=data.loc[:,'ml_files'].to_numpy()
    ml_share=ml_files/n_files
    data['ml_share']=ml_share
    print(f'Average share of ml files over all repositories: {ml_share.mean()}')

    #Bin results
    data['bins']=pd.cut(data[bin_by], bins)
    groups=data.groupby(['bins'])
    #Print mean values
    print('Share of ml related files:')
    print(groups.mean())
    print('Number of repositories in each bin')
    print(groups.count())

    mean_ml_share=groups.median().loc[:,'ml_share'].to_numpy()
    print(mean_ml_share)
    plt.bar(np.arange(len(bins)-1),mean_ml_share)
    plt.xticks(np.arange(len(bins)-1),[f'{bins[i]}, {bins[i+1]}' for i in range(len(bins)-1)])
    plt.xlabel(bin_by)
    plt.ylabel('Average share of ml files')
    plt.show()

def make_scatterplot(x_param):
    #Load data
    data=pd.read_csv(results_path)

    n_files=data.loc[:,'n_scripts'].to_numpy()
    ml_files=data.loc[:,'ml_files'].to_numpy()
    ml_share=ml_files/n_files
    data['ml_share']=ml_share
    print(f'Average share of ml files over all repositories: {ml_share.mean()}')

    #Sort data
    data=data.sort_values(by=x_param)
    # plt.scatter(data.loc[:,x_param],data.loc[:,'ml_share'])

    #Bin data
    data['bins']=pd.qcut(data[x_param], 40)
    groups=data.groupby(['bins'])
    #Print mean values
    print('Share of ml related files:')
    print(groups.mean())
    print('Number of repositories in each bin')
    print(groups.count())

    plt.scatter(groups.median().loc[:,x_param],groups.median().loc[:,'ml_share'])

    #Cosmetics
    plt.xlabel(x_param)
    plt.ylabel('Average share of ml files')
    plt.xscale('linear')
    plt.show()

if __name__=='__main__':
    # analyze_ml_files('n_commits',[50,70,100,150,300,1000000])
    # analyze_ml_files('n_files', [50,70,100,150,300,1000000])
    # analyze_ml_files('n_contributors', [0,1,2,3,4,5,10,20,100])
    # analyze_ml_files('n_branches',[0,1,2,4,10,10000])
    # analyze_ml_files('stargazers_count',[0,1,4,20,10000])
    # analyze_ml_files('size',[0,500,2500,7500,15000,30000,50000,90000,180000,1000000000000])
    make_scatterplot('n_scripts')