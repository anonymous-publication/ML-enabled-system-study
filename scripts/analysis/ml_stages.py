import pandas as pd
import ast
import pprint


ANALYSIS_DIR = "results_test/analysis_out"
RESULTS_DIR="results_test/15-n_scripts/tf_skl_merged.csv"

def merge_tf_sk_projects():
    sk_df = pd.read_csv("results_test/12-n_files/scikit_learn.csv")
    print(sk_df.shape)
    tf_df = pd.read_csv("results_test/12-n_files/tensorflow.csv")
    print(tf_df.shape)

    merged_df = pd.concat([sk_df, tf_df])
    merged_df = merged_df.drop_duplicates(ignore_index=True)
    print(merged_df.shape)
    merged_df.to_csv(f"{ANALYSIS_DIR}/sk_tf_merged.csv", index=False)


def get_stats():
    #data_df = pd.read_csv(f"{ANALYSIS_DIR}/sk_tf_merged.csv")
    data_df=pd.read_csv(RESULTS_DIR)

    print(f"No of commits: {data_df['n_commits'].describe()}\n")
    print(f"No of contributors: {data_df['n_contributors'].describe()}\n")
    print(f"No of branches: {data_df['n_branches'].describe()}\n")
    print(f"No of requests: {data_df['n_pull_requests'].describe()}\n")
    print(f"No of tags: {data_df['n_tags'].describe()}\n")
    print(f"No of releases: {data_df['n_releases'].describe()}\n")
    print(f"No of issues: {data_df['n_issues'].describe()}\n")
    print(f"No of files: {data_df['n_files'].describe()}\n")


def wf_stage_analysis():
    #data_df = pd.read_csv(f"{ANALYSIS_DIR}/sk_tf_merged.csv")
    data_df=pd.read_csv(RESULTS_DIR)
    stages_count_map = {}
    for index, row in data_df.iterrows():
        stages = row["ml_stages"]
        stages = ast.literal_eval(stages)
        for stage in stages:
            stages_count_map[stage] = stages_count_map.get(stage, 0) + 1

    stages_count_df = pd.DataFrame.from_dict(
        stages_count_map,
        orient="index",
        columns=["count"]
    )
    stages_count_df["stage"] = stages_count_df.index
    stages_count_df = stages_count_df[["stage", "count"]]
    stages_count_df = stages_count_df.sort_values(by=["count"], ascending=False)
    stages_count_df.to_csv(f"{ANALYSIS_DIR}/stage_freq.csv", index=False)


def wf_stage_combination_analysis():
    #data_df = pd.read_csv(f"{ANALYSIS_DIR}/sk_tf_merged.csv")
    data_df=pd.read_csv(RESULTS_DIR)
    stages_count_map = {}
    for index, row in data_df.iterrows():
        stages = row["ml_stages"]
        stages_count_map[stages] = stages_count_map.get(stages, 0) + 1

    stages_count_df = pd.DataFrame.from_dict(
        stages_count_map,
        orient="index",
        columns=["count"],
    )
    stages_count_df["stages"] = stages_count_df.index
    stages_count_df = stages_count_df[["stages", "count"]]
    stages_count_df = stages_count_df.sort_values(by=["count"], ascending=False)
    stages_count_df.to_csv(f"{ANALYSIS_DIR}/stage_combination.csv", index=False)


if __name__ == "__main__":
    #merge_tf_sk_projects()
    #get_stats()
    #wf_stage_analysis()
    wf_stage_combination_analysis()
