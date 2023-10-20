import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter
import pandas as pd

FIGSIZE=(12,6)
WSPACE=0.5
plt.rcParams.update({'font.size': 16})

def project_types():
    data={
        'Experiment'	:41,
        'Education'	:31,
        'Tutorial'	:15,
        'System'	:14,
        'Research'	:12,
        'Library'	:10,
        'Toolset'	:8
    }

    fig=plt.figure(figsize=FIGSIZE)
    container=plt.bar(data.keys(),data.values(),width=0.5, color='grey', edgecolor='black')
    plt.ylabel('Number of projects')
    plt.xlabel('Project type')
    plt.bar_label(container,[f'{i}%' for i in data.values()])
    plt.show()
    # fig.savefig('paper/fig/project_cat.pdf',bbox_inches='tight',pad_inches=0)

def boxplots():
    data=pd.read_csv('data/6-ml_stages.csv')

    commits=data.loc[:,'n_commits'].to_numpy()
    contributors=data.loc[:,'n_contributors'].to_numpy()
    branches=data.loc[:,'n_branches'].to_numpy()
    stars=data.loc[:,'stargazers_count'].to_numpy()
    source_files=data.loc[:,'n_scripts'].to_numpy()
    ml_files=data.loc[:,'ml_files'].to_numpy()

    print('Min')
    print(np.min(source_files))
    print(np.min(ml_files))
    print(np.min(commits))
    print(np.min(contributors))
    print(np.min(branches))
    print(np.min(stars))

    print('Max')
    print(np.max(source_files))
    print(np.max(ml_files))
    print(np.max(commits))
    print(np.max(contributors))
    print(np.max(branches))
    print(np.max(stars))

    print("Average")
    print(np.average(source_files))
    print(np.average(ml_files))
    print(np.average(commits))
    print(np.average(contributors))
    print(np.average(branches))
    print(np.average(stars))

    print("Median")
    print(np.median(source_files))
    print(np.median(ml_files))
    print(np.median(commits))
    print(np.median(contributors))
    print(np.median(branches))
    print(np.median(stars))

    fig,ax=plt.subplots(1,6,figsize=FIGSIZE)
    plt.subplots_adjust(wspace=WSPACE)
    ax[0].boxplot(source_files,showfliers=True)
    ax[1].boxplot(ml_files,showfliers=True)
    ax[2].boxplot(commits,showfliers=True)
    ax[3].boxplot(contributors,showfliers=True)
    ax[4].boxplot(branches,showfliers=True)
    ax[5].boxplot(stars,showfliers=True)

    ax[0].set_xlabel("# Source files")
    ax[1].set_xlabel("# ML files")
    ax[2].set_xlabel("# Commits")
    ax[3].set_xlabel("# Contributors")
    ax[4].set_xlabel("# Branches")
    ax[5].set_xlabel("# Stars")

    for axis in ax:
        axis.set_yscale('log')
        axis.set_xticks([])
        # axis.yaxis.set_major_formatter(ScalarFormatter())

    plt.show()
    # fig.savefig('fig/statistics.pdf',bbox_inches='tight',pad_inches=0)

if __name__=="__main__":
    project_types()
    boxplots()