import numpy as np
import pandas as pd
import os

train_paths = os.listdir('data/train_data')
test_paths = os.listdir('data/test_data')

ROOT_TRAIN = 'data/train_data/'
ROOT_TEST = 'data/test_data/'


#new train_data
for key, path in enumerate(train_paths):
    tmp = pd.read_parquet(os.path.join(ROOT_TRAIN, path)).groupby('id').mean()
    tmp.to_parquet(f'data/new_train/new_train_{key}.pq')
    del tmp


# new test_data
for key, path in enumerate(test_paths):
    tmp = pd.read_parquet(os.path.join(ROOT_TEST, path)).groupby('id').mean()
    tmp.to_parquet(f'data/new_test/new_test_{key}.pq')
    del tmp