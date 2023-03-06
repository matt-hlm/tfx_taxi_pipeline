import pandas as pd
import numpy as np

AGE = 'age'

filepath = r"raw/raw_data.csv"
data = pd.read_csv(filepath)

# create probability distribution for output
age_distribution = np.random.dirichlet((4, 5, 2, 1, 3, 3, 3), len(data))

output_columns = pd.DataFrame(age_distribution)
output_columns.columns = ['Cash', 'Credit Card', 'Dispute', 'No Charge', 'Pcard', 'Unknown', 'Prcard']

data = pd.concat([data, output_columns], axis=1)

data.to_csv(r"data/data.csv", index=False, index_label=False)