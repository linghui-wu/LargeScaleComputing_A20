# Numpy
import numpy as np 
x = np.ones((1000, 1000))
print(x + x.T - x.mean(axis=0))

import dask.array as da 
x = da.ones((1000, 1000))
print(x + x.T - x.mean(axis=0))

# Pandas
import pandas as pd 
df = pd.read_csv("file.csv")
df.groupby("x").y.mean()

import dask.dataframe as dd
df = dd.read_csv("s3://*.csv")
df.groupby("x").y.mean()

# Scikit-Learn
from scikit_learn.linear_model import LogisticRegression
lr = LogisticRegression()
lr.fit(data, labels)

from dask_ml.linear_model import LogisticRegression
lr = LogisticRegression()
lr.fit(data, labels)

# Create a 1D dask array that splits into 5 chunks
x = da.ones(15, chunks=(5, ))
# Actually computes the object and returns the result
print(x.compute())

# More complicated tasks
x_sum = x.sum()
print(x_sum.compute())

x = da.ones((15, 15), chunks=(5, 5))
x_sum = x.sum(axis=0)
print((x + x.T).compute())
