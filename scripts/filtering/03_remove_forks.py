import pandas as pd

# only keeps the repositories that are not forks and writes them to a new file
input_file = "data/1a-dependents_queried/tensorflow_dependents.csv"
output_file = "data/2-forks_removed/tensorflow_dependents_14_02_2022.csv"
df = pd.read_csv(input_file)
df = df[df["fork"] == False]
df.to_csv(output_file, index=False)
