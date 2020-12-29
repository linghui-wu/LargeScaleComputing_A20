# Querying Python interpreter's memory usage
import psutil, os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from math import sqrt
from dask import delayed
from dask.array import da
import dask.dataframe as dd
import time
import dask.bag as db
import time
import h5py  # Import module for reading HDF5 files
import glob
import json

def memory_footprint():
	"""Returns memory (in MB) being used by Python process."""
	mem = psutil.Process(os.getpid()).memory_info().rss
	return (mem / 1024 ** 2)

before = memory_footprint()
N = (1024 ** 2) // 8  # Number of floats that fill 1 MB
x = np.random.rand(50 * N) # Random array filling 50 MB
after = memory_footprint()
print(before, after)

before = memory_footprint()
x ** 2  # Computes but doesn't bind result to a variable
after = memory_footprint()
print(before, after)

# Querying array memory usage
print(x.nbytes)  # Memory footprint in bytes (B)
print(x.nbytes // (1024 ** 2))  # Memory footprint in megabytes (MB)

# Querying DataFrame memory usage
df = pd.DataFrame(x)
print(df.memory_usage(index=False))
print(df.memory_usage(index=False) // (1024 ** 2))

# Print the size in MB of the celsius array
print(celsius.nbytes / (1024 ** 2))
# Call memory_footprint(): before
before = memory_footprint()
# Convert celsius by multiplying by 9/5 and adding 32: fahrenheit
fahrenheit = celsius * 9 / 5 + 32
# Call memory_footprint(): after
after = memory_footprint()
# Print the difference between after and before
print(after - before)

# Think about data in chunks
# Using pd.read_csv() with chunksize
filename = 'NYC_taxi_2013_01.csv'
for chunk in pd.read_csv(filename, chunksize=50000):
	print('type: %s shape %s' % (type(chunk), chunk,shape))
# Examining a chunk
print(chunk.shape)
print(chunk.info())
# Filter a chunk
is_long_trip = (chunk.trip_time_in_secs > 1200)
chunk.loc[is_long_trip].shape
# Chunking & filtering together
def filter_is_long_trip(data):
	"""Return DataFrame filtering trips longer than 20 minutes."""
	is_long_trip = (data.trip_time_in_secs > 1200)
	return data.loc[is_long_trip]
chunks = []
for chunk in pd.read_csv(filename, chunksize=1000):
	chunks.append(filter_is_long_trip(chunk))
chunks = [filter_is_long_trip(chunk) for chunk in pd.read_csv(filename, chunksize=1000)]
# Using pd.concat()
print(len(chunks))
lengths = [len(chunks) for chunk in chunks]
print(lengths[-5:])  # Each has ~100 rows
long_trips_df = pd.concat(chunks)
print(long_trips_df.shape)
# Plotting the filtered results
long_trips_df.plot.scatter(x='trip_time_in_secs', y='trip_distance');
plt.xlabel('Trip duration [seconds]');
plt.ylabel('Trip distance [miles]');
plt.title('NYC Taxi rides over 20 minutes (2013-01-01 to 2013-01-04)');
plt.show();

# Create empty list: dfs
dfs = []

# Loop over 'WDI.csv'
for chunk in pd.read_csv('WDI.csv', chunksize=1000):
    # Create the first Series
    is_urban = chunk['Indicator Name']=='Urban population (% of total)'
    # Create the second Series
    is_AUS = chunk['Country Code']=='AUS'
    # Create the filtered chunk: filtered
    filtered = chunk.loc[is_urban & is_AUS]
    # Append the filtered chunk to the list dfs
    dfs.append(filtered)

# Print length of list dfs
print(len(dfs))
# Apply pd.concat to dfs: df
df = pd.concat(dfs)
# Print length of DataFrame df
print(len(df))
# Call df.plot.line with x='Year' and y='value'
df.plot.line(x='Year', y='value')
plt.ylabel('% Urban population')
# Call plt.show()
plt.show()

# Manageing Data with Generators
# Filtering & summing with generators
chunks = (filter_is_long_trip(chunk) for chunk in pd.read_csv(filename, chunksize=1000))
distances = (chunk['trip_distance'].sum() for chunk in chunks)
print(sum(distances))
print(distances)
print(next(distances))

# Reading many files
template = 'yellow_tripdata_2015-{:02d}.csv'
filenames = (template.format(k) for k in range(1, 13))  # Generator
for fname in filenames:
	print(fname) # Examine contents

# Examing a sample DataFrame
df = pd.read_csv('yellow_tripdata_2015-12.csv', parse_dates=[1, 2])
df.info()  # Columns deleted from output
# Examing a sample DataFrame
def count_long_trips(df):
	df['duration'] = (df.tpep_dropoff_datetime - df.tpep_pickup_datetime).dt.seconds
	is_long_trip = df.duration > 1200
	result_dict = {'n_long': [sum(is_long_trip)],
				'n_total': [len(df)]}
	return pd.DataFrame(result_dict)
filenames = [template.format(k) for k in range(1, 13)]  # List comprehension
dataframes = (pd.read_csv(fname, parse_dates=[1, 2]) for fname in filenames)  # Generator
totals = (count_long_trips(df) for df in dataframes)  # Generator
annual_totals = sum(totals)  # Consumes generators
# Computing the fraction of long trips
print(annual_totals)
fraction = annual_totals['n_long'] / annual_totals['n_total']
print(fraction)

# Define function with single input called df: pct_delayed
def pct_delayed(df):
    # Compute number of delayed flights: n_delayed
    n_delayed = (df['DEP_DELAY'] > 0).sum()
    # Return percentage of delayed flights
    return n_delayed  * 100 / len(df)

# Define the generator: dataframes
dataframes = (pd.read_csv(file) for file in filenames)
# Create the list comprehension: monthly_delayed
monthly_delayed = [pct_delayed(df) for df in dataframes]
# Create the plot
x = range(1,13)
plt.plot(x, monthly_delayed, marker='o', linewidth=0)
plt.ylabel('% Delayed')
plt.xlabel('Month - 2016')
plt.xlim((1,12))
plt.ylim((0,100))
plt.show()

# Aggregating with delayed Functions
template = 'yellow_tripdata_2015-{:02d}.csv'
filenames = [template.format(k) for k in range(1, 13)]
@delayed
def count_long_trips(df):
    df['duration'] = (df.tpep_dropoff_datetime - df.tpep_pickup_datetime).dt.seconds
    is_long_trip = df.duration > 1200
    result_dict = {'n_long': [sum(is_long_trip)], 'n_total': [len(df)]}
    return pd.DataFrame(result_dict)
@delayed
def read_file(fname):
    return pd.read_csv(fname, parse_dates=[1, 2])
# Computing fraction of long trips using `delayed` functions
totals = [count_long_trips(read_file(fname)) for fname infilenames]
annual_totals = sum(totals)
annual_totals = annual_totals.compute()
fraction = annual_totals['n_long'] / annual_totals['n_total']
print(fraction)

# Define count_flights
@delayed
def count_flights(df):
    return len(df)
# Define count_delayed
@delayed
def count_delayed(df):
    return (df['DEP_DELAY']>0).sum()
# Define pct_delayed
@delayed
def pct_delayed(n_delayed, n_flights):
    return 100 * sum(n_delayed) / sum(n_flights)

# Loop over the provided filenames list and call read_one: df
for file in filenames:
    df = read_one(file)
    # Append to n_delayed and n_flights
    n_delayed.append(count_delayed(df))
    n_flights.append(count_flights(df))
# Call pct_delayed with n_delayed and n_flights: result
result = pct_delayed(n_delayed, n_flights)
# Print the output of result.compute()
print(result.compute())

# Timing array computations
with h5py.File('dist.hdf5', 'r') as dset:
    dist = dest['dist'][:]
dist_dask8 = da.from_array(dist, chunks=dist.shape[0] // 8)
# Execute the commands separated by semicolons within a single IPython cell 
# to ensure immediate interactive execution
t_start = time.time(); \
mean8 = dist_dask8.mean().compute(); \
t_end = time.time()

# Call da.from_array():  energy_dask
energy_dask = da.from_array(energy, chunks=energy.shape[0]//4)
# Print energy_dask.chunks
print(energy_dask.chunks)
# Print Dask array average and then NumPy array average
print(energy_dask.mean().compute())
print(energy.mean())

# Call da.from_array() with arr: energy_dask8
energy_dask8 = da.from_array(energy, chunks=len(energy) / 8)
# Print the time to compute standard deviation
t_start = time.time()
std_8 = energy_dask8.std().compute()
t_end = time.time()
print((t_end - t_start) * 1.0e3)

# Delaying Computation with Dask
def f(z): return sqrt(z + 4)
def g(y): return y - 3
def h(x): return x ** 2

x = 4
print(f(g(h(x))))  # Equal
y = h(x)
z = g(y)
w = f(z)
print(w)

y = delayed(h)(x)
z = delayed(g)(y)
w = delayed(f)(z)
print(w, type(w))  # a dask Delayed objects
print(w.compute())  # COmputation occurs now


# Cisualizeing a task graph
w.visualize()

# Renaming decorate functions
f = delayed(f)
g = delayed(g)
h = delayed(h)
w = f(g(h(4)))
print(w) # a dask Delayed object
print(w.compute()) # Computation occurs now

# Using decorator @-notation
def f(x): return sqrt(x + 4)
f = delayed(f)
@delayed # Equivalent to definition in above 2 cells
def f(x): return sqrt(x + 4)

# Deferring Computation with Loops
@delayed
def increment(x): return (x + 1)
@delayed
def double(x): return 2 * x
@delayed
def add(x, y): return (x + y)

data = [1, 2, 3, 4, 5]
output = []
for x in data:
    a = increment(x)
    b = double(x)
    c = add(a, b)
    print(a, type(a), b, type(b), c, type(c))
    output.append(c)
total = sum(output)
print(total)
print(output)
print(total.visualize())

# Chunking Arrays in Dask
a = np.random.rand(10000)
print(a.shape, a.dtype)
print(a.sum())
print(a.mean())

# Working with Dask arrays
a_dask = da.from_array(a, chunks=len(a) // 4)
print(a_dask.chunks)
# Aggregating in chunks
n_chunks = 4
chunk_size = len(a) // n_chunks
result = 0  # Accumulate sum
for k in range(n_chunks):
    offset = k * chunk_size  # Track offset
    a_chunk = a[offset:offset + chunk_size]  # Slice chunk
    result += a_chunk.sum()
print(result)

# Aggregating with Dask arrays
a_dask = da.from_array(a, chunks=len(a) // n_chunks)
result = a_dask.sum()
print(result)
print(result.compute())
result.visualize(rankdir='LR')

# Timing array computations
with h5py.File('dist.hdf5', 'r') as dset:
    dist = dest['dist'][:]
dist_dask8 = da.from_array(dist, chunks=dist.shape[0] // 8)
# Execute the commands separated by semicolons within a single IPython cell 
# to ensure immediate interactive execution
t_start = time.time(); \
mean8 = dist_dask8.mean().compute(); \
t_end = time.time()

# Computing with Multidimensional Arrays
time_series = np.loadtxt('max_temps.csv', dtype=np.int64)
print(time_series.shape)
print(time_series.ndim)
print(time_series)
table = time_series.reshape((3, 7)) # Reshaped row-wise
print(table)  # Display the result
table = time_series.reshape((3, 7), order='F')  # Column-wise: correct

# Indexing in multiple dimensions
print(table)  # Display the result
table[0, 4]  # Value from week 0, day 4
table[1, 2:5]  # Values from Week1, Days 2, 3, & 4
table[0::2, ::3]  # Values form Weeks 0&2, Days 0, 3, & 6
table[0]  # Equivalent to table [0, :]

# Aggregating multidimensional arrays
print(table)
table.mean()  # mean of *every* entry in the table
daily_means = table.mean(axis=0)  # Average for days
print(daily_means)  # Mean computed of rows (for each day)
weekly_means = table.mean(axis=1)
print(weekly_means)  # Mean computed of columns (for each week)
table.mean(axis=(0, 1))  # Mean of rows, then columns
print(table - daily_means)  # This works!
print(table - weekly_means)  # This does not work!

print(table.shape)  # (3, 7)
print(daily_means.shape)  # (7, )
print(weekly_means.shape)  # (3, )
result = table - weekly_means.reshape((3, 1))  # This works now!

data = np.loadtxt('', usecols=(1, 2, 3, 4), dtype=np.int64)
data.shape, type(date)

data_dask = da.from_array(data, chunks=(366, 2))
result = data_dask.std(axis=0)  # Standard deviation dow columns
result.compute()

# Reshape load_recent to three dimensions: load_recent_3d
load_recent_3d = load_recent.reshape((3,365,96))
# Reshape load_2001 to three dimensions: load_2001_3d
load_2001_3d = load_2001.reshape((1,365,96))
# Subtract the load in 2001 from the load in 2013 - 2015: diff_3d
diff_3d = load_recent_3d - load_2001_3d
# Print the difference each year on March 2 at noon
print(diff_3d[:, 61, 48])
# Print mean value of load_recent_3d
print(load_recent_3d.mean())
# Print maximum of load_recent_3d across 2nd & 3rd dimensions
print(load_recent_3d.max(axis=(1,2)))
# Compute sum along last dimension of load_recent_3d: daily_consumption
daily_consumption = load_recent_3d.sum(axis=-1)
# Print mean of 62nd row of daily_consumption
print(daily_consumption[:, 61].mean())

# Analyzing Weather Data
# Open HDF5 File Object
data_store = h5py.File('tmax.2008.hdf5')
for key in data_store.keys():  # Iterate over keys
	print(key)  # tmax
# Extracting Dask array form HDF5
data = data_store['tmax']  # Bind to data for introspection
print(type(data))
print(data.shape) # 3D array: (2D for each month)
data_dask = da.from_array(data, chunks=(1, 444, 922))
# Aggregating while ignoring NaNs
data_dask.min()  # Yielfs unevaluated Dask Array
data_dask.min().compute()  # Force computation
da.nanmin(data_dask).compute()  # Ignoring nans
print(da.nanmin(data_dask).compute(), da.nan.min(data_dask).compute())

# Producing a visualization of data_dask
fig, panels = plt.subplots(nrows=4, ncols=3)
for month, panel in zip(range(N_months), panels.flattern()):
	im = panel.imshow(data_dask[month, :, :],
		origin='lower',
		vmin=lo, vmax=hi)  # Ensures a common map scale
	panel.set_title('2008-{:02d}'.format(month + 1))
	panel.axis('off')

plt.suptitle('Monthly averages (max. daily temperature [C])')
plt.colorbar(im, ax=panels.ravel().tolist());  # Common colorbar
plt.show()
# Stacking arrays
a = np.ones(3); b = 2 * a; c = 3 * a
print(a, '\n', b, '\n', c)
print(np.stack([a, b]))  # Makes 2D array of shape (2, 3)
print(np.stack([a, b]), axis=0)  # Same as above
print(np.stack([a, b]), axis=1)  # Makes 2D array of shape (3, 2)

X = np.stack([a, b]); \
Y = np.stack([b, c]); \
Z = np.stack([c, a])
print(X, Y, Z, sep='\n')
print(np.stack([X, Y, Z]))  # Makes 3D array of shape (3, 2, 3)

# Makes 3D array of shape (2, 3, 3)
np.stack([X, Y, Z], axis=1)

# List comprehension to read each file: dsets
dsets = [h5py.File(f)['/tmax'] for f in filenames]
# List comprehension to make dask arrays: monthly
monthly = [da.from_array(d, chunks=(1, 444, 922)) for d in dsets]
# Stack with the list of dask arrays: by_year
by_year = da.stack(monthly, axis=0)
# Print the shape of the stacked arrays
print(by_year.shape)
# Read the climatology data: climatology
dset = h5py.File('tmax.climate.hdf5')
climatology = da.from_array(dset['/tmax'], chunks=(1,444,922))
# Reshape the climatology data to be compatible with months
climatology = climatology.reshape((1, 12, 444, 922))
# Compute the difference: diff
diff = (by_year - climatology) * 9 / 5
# Compute the average over last two axes: avg
avg = da.nanmean(diff, axis=(-1,-2)).compute()
# Plot the slices [:,0], [:,7], and [:11] against the x values
x = range(2008,2012)
f, ax = plt.subplots()
ax.plot(x,avg[:,0], label='Jan')
ax.plot(x,avg[:,7], label='Aug')
ax.plot(x,avg[:,11], label='Dec')
ax.axhline(0, color='red')
ax.set_xlabel('Year')
ax.set_ylabel('Difference (degrees Fahrenheit)')
ax.legend(loc=0)
plt.show()

# Using Dask DataFrames
# Reading multiple CSV files
transactions = dd.read_csv('*.csv')
print(transactions.head(), transactions.tail())
# Building delayed piplines
is_wendy = (transactions['names'] == 'Wendy')
wendy_amounts = transactions.loc[is_wendy, 'amount']
print(wendy_amounts)
wendy_dfff = wendy_amounts.sum()
wendy_dfff.visualize(rankdir='LR')


# Read from 'WDI.csv': df
df = dd.read_csv('WDI.csv')
# Boolean series where 'Indicator Code' is 'EN.ATM.PM25.MC.ZS': toxins
toxins = df['Indicator Code'] == 'EN.ATM.PM25.MC.ZS'
# Boolean series where 'Region' is 'East Asia & Pacific': region
region = df['Region'] == 'East Asia & Pacific'
# Filter the DataFrame using toxins & region: filtered
filtered = df.loc[toxins & region]
# Grouby filtered by the 'Year' column: yearly
filtered.groupby('Year')
# Calculate the mean of yearly: yearly_mean
yearly_mean = filtered.groupby('Year').mean()
# Call .compute() to perform the computation: result
result = yearly_mean.compute()
# Plot the 'value' column with .plot.line()
result['value'].plot.line()
plt.ylabel('% pop exposed')
plt.show()

# Timing DataFrame Operations
t_start = time.time()
df = pd.read_csv('yellow_tripdata_2015-01.csv')
t_end = time.time()
print('pd.read_csv(): {} s'.format(t_end - t_start))  # time [s]
t_start = time.time()
m = df['trip_distance'].mean()
t_end = time.time()
print('.mean(): {} ms'.format(t_end - t_start) * 1000)  # time [ms]

# Timing I/0 & computation: Dask
t_start = time.time(); 
df = pd.read_csv('yellow_tripdata_2015-*.csv'); 
t_end = time.time(); 
print('dd.read_csv: {} ms'.format((t_end - t_start) * 1000))  # time [ms]
t_start = time.time()
m = df['trip_distance'].mean()
t_end = time.time()
print('.mean(): {} ms'.format(t_end - t_start) * 1000)  # time [ms]
t_start = time.time();
result = m.compute()
t_end = time.time()
print('.comput(): {} min'.format((t_end - t_start) / 60)  # time [min]

# Define a function with df as input: by_region
def by_region(df):
    # Create the toxins array
    toxins = df['Indicator Code'] == 'EN.ATM.PM25.MC.ZS'
    # Create the y2015 array
    y2015 = df['Year'] == 2015
    # Filter the DataFrame and group by the 'Region' column
    regions = df.loc[toxins & y2015].groupby('Region')
    # Return the mean of the 'value' column of regions
    return regions['value'].mean()
# Call time.time()
t0 = time.time()
# Read 'WDI.csv' into df
df = pd.read_csv('WDI.csv')
# Group df by region: result
result = by_region(df)
# Call time.time()
t1 = time.time()
# Print the execution time
print((t1-t0)*1000)

# Time the execution of just by_region with Pandas and print in milliseconds
df = pd.read_csv('WDI.csv')
t0 = time.time()
result = by_region(df)
t1 = time.time()
print((t1-t0)*1000)

# Time the execution of dd.read_csv and by_region together with 'WDI.csv' and print in milliseconds
t0 = time.time()
df = dd.read_csv('WDI.csv')
result = by_region(df)
t1 = time.time()
print((t1-t0)*1000)

# Analysing NYV Taxi Rides
df = pd.read_csv('yellow_tripdata_2015-01.csv')
print(df.shape, df.columns)
df['payment_type'].value_counts()

# Read all .csv files: df
df = dd.read_csv('taxi/*.csv', assume_missing=True)
# Make column 'tip_fraction'
df['tip_fraction'] = df['tip_amount'] / (df['total_amount'] - df['tip_amount'])
# Convert 'tpep_dropoff_datetime' column to datetime objects
df['tpep_dropoff_datetime'] = dd.to_datetime(df['tpep_dropoff_datetime'])
# Construct column 'hour'
df['hour'] = df['tpep_dropoff_datetime'].dt.hour

# Filter rows where payment_type == 1: credit
credit = df.loc[df['payment_type'] == 1]
# Group by 'hour' column: hourly
hourly = credit.groupby('hour')
# Aggregate mean 'tip_fraction' and print its data type
result = hourly['tip_fraction'].mean()
print(type(result))
# Perform the computation
tip_frac = result.compute()
# Print the type of tip_frac
print(type(tip_frac))
# Generate a line plot using .plot.line()
tip_frac.plot.line()
plt.ylabel('Tip fraction')
plt.show()

# Building Dask Bags & Globbing 
# Sequences to bags
nested_containers = [[0, 1, 2, 3], {}, [6.5, 3.14], 'Python', {'version': 3}, '']
the_bag = db.from_sequence(nested_containers)
print(the_bag.count())
print(the_bag.any(), the_bag.all())
# Reading text files
zen = db.read_text('zen')
taken = zen.take(1)
print(taken, type(taken), zen.take(3))

# Glob expressions
df = dd.read_csv('taxi/*.csv', assume_missing=True)
txt_files = glob.glob('*.txt')
print(txt_files)
print(glob.glob('b*.txt'))
print(glob.glob('b?.txt'))
print(glob.glob('?0[1-6].txt'))
print(glob.glob('??[1-6].txt'))

# Glob filenames matching 'sotu/*.txt' and sort
filenames = glob.glob('sotu/*.txt')
filenames = sorted(filenames)
# Load filenames as Dask bag with db.read_text(): speeches
speeches = db.read_text(filenames)
# Print number of speeches with .count()
print(speeches.count().compute())

# Call .take(1): one_element
one_element = speeches.take(1)
# Extract first element of one_element: first_speech
first_speech = one_element[0]
# Print type of first_speech and first 60 characters
print(type(first_speech))
print(first_speech[:60])

# Functional Approaches using Dask Bags
def squared(x):
    return x ** 2
squares = list(map(squared, [1, 2, 3, 4, 5, 6]))

def is_even(x):
    return x % 2 == 0
evens = filter(is_even, [1, 2, 3, 4, 5, 6]) 

numbers = db.from_sequence([1, 2, 3, 4, 5, 6])
squares = numbers.map(squared)
result = squares.compute()  # Must fit in memory
print(result)
# Using dask.bag.filter
numbers = db.from_sequence([1, 2, 3, 4, 5, 6])
evens = numbers.filter(is_even)
evens.compute()
even_squares = numbers.map(squared).filter(is_even)
even_squares.compute()

# Using .str & string methods
uppercase = zen.str.upper()
uppercase.take(1)

def my_upper(string):
    return string.upper()
my_uppercase = zen.map(my_upper)
my_uppercase.take(1)

def load(k):
    template = 'yellow_tripdata_2015-{:02d}.csv'
    return pd.read_csv(template.format(k))
def average(df):
    return df['total_amount'].mean()
def total(df):
    return df['total_amount'].sum()
data = db.from_sequence(range(1, 13)).map(load)
print(data)
totals = data.map(total)
totals.compute()
averages = data.map(average)
averages.compute()

t_sum, t_min, t_max = totals.sum(), totals.min(), totals.max()
t_mean, t_std = totals.mean(), totals.std()
stats = [t_sum, t_min, t_max, t_mean, t_std]
[s.compute() for s in stats]
dask.compute(t_sum, t_min, t_max, t_mean, t_std)

# Call .str.split(' ') from speeches and assign it to by_word
by_word = speeches.str.split(' ')
# Map the len function over by_word and compute its mean
n_words = by_word.map(len)
avg_words = n_words.mean()
# Print the type of avg_words and value of avg_words.compute()
print(type(avg_words))
print(avg_words.compute())

# Call .str.split(' ') from speeches and assign it to by_word
by_word = speeches.str.split(' ')
# Map the len function over by_word and compute its mean
n_words = by_word.map(len)
avg_words = n_words.mean()
# Print the type of avg_words and value of avg_words.compute()
print(type(avg_words))
print(avg_words.compute())

# Analyzing Congressional Legislation
with open('items.json') as f:
    items = json.load(f)
print(type(items))
print(items[0], items[1]['content']['b'])
items = db.read_text('items-by=line.json')
items.take(1)
dict_items = items.map(json.loads)  # converts strings into other data
dict_items.take(2)  # Note: tuple containing dicts
print(dict_items.take(2))
print(dict_items.take(2)[1]['content'])  # Chained indexing
print(dict_items.take(1)(0)['name'])  # Chained indexing

# Plucking values
contents = dict_items.pluck('content')
names = dict_items.pluck('names')
print(contents)
print(names)

contents.compute()
names.compute()

# Call db.read_text with congress/bills*.json: bills_text
bills_text = db.read_text('congress/bills*.json')
# Map the json.loads function over all elements: bills_dicts
bills_dicts = bills_text.map(json.loads)
# Extract the first element with .take(1) and index to the first position: first_bill
first_bill = bills_dicts.take(1)[0]
# Print the keys of first_bill
print(first_bill.keys())

# Filter the bills: overridden
overridden = bills_dicts.filter(veto_override)
# Print the number of bills retained
print(overridden.count().compute())
# Get the value of the 'title' key
titles = overridden.pluck('title')
# Compute and print the titles
print(titles.compute())

# Preparing Flight Dealy Data
# Reading/cleaning in a function
@delayed
def pipeline(filename, account_name):
    df = pd.read_csv(filename)
    df['account_name'] = account_name
    return df

for account in ['Bob', 'Alice', 'Dave']:
    fname = 'accounts/{}.csv'.format(account)
    delayed_dfs.append(pipeline(fname, account))
dask_df = dd.from_delayed(delayed_dfs)
dask_df['amount'].mean().compute()

df = pd.read_csv('flightdelayes-2016-1.csv')
print(df.columns)
print(df['WEATHER_DELAY'].tail())

# Replacing values
new_series = series.replace(6, np.nan)

# Define @delayed-function read_flights
@delayed
def read_flights(filename):
    # Read in the DataFrame: df
    df = pd.read_csv(filename, parse_dates=['FL_DATE'])
    # Replace 0s in df['WEATHER_DELAY'] with np.nan
    df['WEATHER_DELAY'] = df['WEATHER_DELAY'].replace(0, np.nan)
    # Return df
    return df

# Loop over filenames with index filename
for filename in filenames:
    # Apply read_flights to filename; append to dataframes
    dataframes.append(read_flights(filename))
# Compute flight delays: flight_delays
flight_delays = dd.from_delayed(dataframes)
# Print average of 'WEATHER_DELAY' column of flight_delays
print(flight_delays['WEATHER_DELAY'].mean().compute())

# Preparing Weather Data
df = pd.read_csv('DEN.csv', parse_dates=True, index_col='Date')
print(df.columns)

print(df.loc['March 2016', ['PrecipitationIn', 'Events']]).tail()
print(df['PrecipitationIn'][0], type(df['PrecipitationIn'][0]))

df[['PrecipitationIn', 'Events']].info()
new_series = pd.to_numeric(series, errors='coerce')

# Define @delayed-function read_weather with input filename
delayed
def read_weather(filename):
    # Read in filename: df
    df = pd.read_csv(filename, parse_dates=['Date'])
    # Clean 'PrecipitationIn'
    df['PrecipitationIn'] = pd.to_numeric\
    (df['PrecipitationIn'], errors='coerce')
    # Create the 'Airport' column
    df['Airport'] = filename.split('.')[0]
    # Return df
    return df

# Loop over filenames with filename
for filename in filenames:
    # Invoke read_weather on filename; append result to weather_dfs
    weather_dfs.append(read_weather(filename))
# Call dd.from_delayed() with weather_dfs: weather
weather = dd.from_delayed(weather_dfs)
# Print result of weather.nlargest(1, 'Max TemperatureF')
print(weather.nlargest(1, 'Max TemperatureF').compute())

# Make cleaned Boolean Series from weather['Events']: is_snowy
is_snowy = weather['Events'].str.contains('Snow').fillna(False)
# Create filtered DataFrame with weather.loc & is_snowy: got_snow
got_snow = weather.loc[is_snowy]
# Groupby 'Airport' column; select 'PrecipitationIn'; aggregate sum(): result
result = got_snow.groupby('Airport')['PrecipitationIn'].sum()
# Compute & print the value of result
print(result.compute())

# Merging & Persisting DataFrames
left_df.merge(left_df, left_on=['cat_left'], right_on=['cat_right'], how='inner')

df = dd.read_csv('flightdelayes-2016-*.csv')
print(df.WEATHER_DELAY.mean().compute())
print(df.WEATHER_DELAY.std().compute())
print(df.WEATHER_DELAY.count().compute())
persisted_df = df.persist()
print(persisted_df.WEATHER_DELAY.mean().compute())
print(persisted_df.WEATHER_DELAY.std().compute())
print(persisted_df.WEATHER_DELAY.count().compute())

# Print time in milliseconds to compute percent_delayed on weather_delays
t_start = time.time()
print(percent_delayed(weather_delays).compute())
t_end = time.time()
print((t_end-t_start)*1000)
# Call weather_delays.persist(): persisted_weather_delays
persisted_weather_delays = weather_delays.persist()
# Print time in milliseconds to compute percent_delayed on persisted_weather_delays
t_start = time.time()
print(percent_delayed(persisted_weather_delays).compute())
t_end = time.time()
print((t_end-t_start)*1000)
# Group persisted_weather_delays by 'Events': by_event
by_event = persisted_weather_delays.groupby('Events')
# Count 'by_event['WEATHER_DELAY'] column & divide by total number of delayed flights
pct_delayed = by_event['WEATHER_DELAY'].count() / persisted_weather_delays['WEATHER_DELAY'].count() * 100
# Compute & print five largest values of pct_delayed
print(pct_delayed.nlargest(5).compute())
# Calculate mean of by_event['WEATHER_DELAY'] column & return the 5 largest entries: avg_delay_time
avg_delay_time = by_event['WEATHER_DELAY'].mean().nlargest(5)
# Compute & print avg_delay_time
print(avg_delay_time.compute())

