results = []
for x in A:
    for y in B:
        if x < y:
            results.append(f(x, y))
        else:
            results.append(g(x, y))

import dask
results = []
results = []
for x in A:
    for y in B:
        if x < y:
            results.append(dask.delayed(f(x, y)))
        else:
            results.append(dask.delayed(g(x, y)))
Results = dask.compute(results)

# dask.delayed decorators
@dask.delayed
def inc(x):
    return x + 1

data = [i + 1 for i in range(4)]

output = []
output = [inc(x) for x in data]
dask.compute(output)