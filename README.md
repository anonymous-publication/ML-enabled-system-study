This repo contains the appendix/instruments for our paper, "Identifying Experimentation in ML/DL Systems on GitHub: An Empirical Study and Mining Heuristics".

The contents of each folders are listed below:

data: Contains the data ordered by filtering/ processing steps
- **1-dependents**: Names of GitHub projects that are dependent on SciKit-Learn and TensorFlow libraries.
- **1a-dependents_queried**: Query results of GitHub repositories dependent on SciKit-Learn and TensorFlow libraries.
- **2-forks_removed**: List of GitHub projects left after removing forks.
- **3a-number_commits_queried**: List of projects with their number of commits.
- **3-number_commits_filtered**: List of projects with commits count >= 50.
- **4-api-dictionary**: API dictionary mapping libraries calls to ML development phases.
- **4a-library_calls_filtered**: List of projects with relevant library calls.
- **5-attributes**: List of projects along with the following attributes: number of contributors, branches, pull requests, tags, number of releases, issues, files, and their ML development phases.
- **6-experiments**: Results after applying the different heuristics to filter out experiments
- **9a-dev-phases**: Count of ML development phases and their combinations found in the subjects.
- **9b-dev_phases**: Projects evolution indicating development phases affected per commit.

scripts: Python scripts used in this study.
  - **utilities**: Collection of functions required for the scripts
  - **data_acquisition**: Scripts required to obtain the data from GitHub
  - **filtering**: Scripts for filtering the data obtained from GitHub
  - **data_processing**: Generating further information required for analysis
  - **analysis**: Used to generate the results presented in the paper based on filtering and data_processing


The results from the paper can be reproduced by running the scripts in the following order:
  - Obtaining the data from GitHub: Execute scripts/data_acquisition/01_get_dependants.py
  - Filtering the data: Execute the Python scripts in scripts/filtering in the given order, except for the last one (12_filter_experiments.py)
  - Additional processing required for results in RO2: Run scripts/data_processing/commit_stages.py
  - Analysis:
    - For RO1 (fig. 3&4): scripts/analysis/ml_stages.py
    - For RO2 (fig. 5): scripts/analysis/commit_stats
  - Results for RO3: Run scripts/filtering/12_filter_experiments.py
