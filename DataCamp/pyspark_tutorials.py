import numpy as np 
import pandas as pd
import pyspark

# Verify SparkContext
print(sc)

# Print Spark version
print(sc.version)

# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark
my_spark = SparkSession.builder.getOrCreate()

# Print my_spark
print(my_spark)

# Print the tables in the catalog
print(spark.catalog.listTables())

# Don't change this query
query = "FROM flights SELECT * LIMIT 10"

# Get the first 10 rows of flights
flights10 = spark.sql(query)

# Show the results
flights10.show()

# Don't change this query
query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"

# Run the query
flight_counts = spark.sql(query)

# Convert the results to a pandas DataFrame
pd_counts = flight_counts.toPandas()

# Print the head of pd_counts
print(pd_counts.head())

# Create pd_temp
pd_temp = pd.DataFrame(np.random.random(10))

# Create spark_temp from pd_temp
spark_temp = spark.createDataFrame(pd_temp)

# Examine the tables in the catalog
print(spark.catalog.listTables())

# Add spark_temp to the catalog
spark_temp.createOrReplaceTempView("temp")

# Examine the tables in the catalog again
print(spark.catalog.listTables())

# Don't change this file path
file_path = "/usr/local/share/datasets/airports.csv"

# Read in the airports data
airports = spark.read.csv(file_path, header=True)

# Show the data
airports.show()

# Create the DataFrame flights
flights = spark.table("flights")

# Show the head
flights.show()

# Add duration_hrs
flights = flights.withColumn("duration_hrs", flights.air_time / 60)

# Filter flights by passing a string
long_flights1 = flights.filter("distance > 1000")

# Filter flights by passing a column of boolean values
long_flights2 = flights.filter(flights.distance > 1000)

# Print the data to check they're equal
long_flights1.show()
long_flights2.show()

# Select the first set of columns
selected1 = flights.select("tailnum", "origin", "dest", "flights")

# Select the second set of columns
temp = flights.select(flights.origin, flights.dest, flights.carrier)

# Define first filter
filterA = flights.origin == "SEA"

# Define second filter
filterB = flights.dest == "PDX"

# Filter the data, first by filterA then by filterB
selected2 = temp.filter(filterA).filter(filterB)

# Define avg_speed
avg_speed = (flights.distance/(flights.air_time/60)).alias("avg_speed")

# Select the correct columns
speed1 = flights.select("origin", "dest", "tailnum", avg_speed)

# Create the same table using a SQL expression
speed2 = flights.selectExpr("origin", "dest", "tailnum", "distance/(air_time/60) as avg_speed")

# Find the shortest flight from PDX in terms of distance
flights.filter(flights.origin == "PDX").groupBy().min("distance").show()

# Find the longest flight from SEA in terms of air time
flights.filter(flights.origin == "SEA").groupBy().max("air_time").show()

# Average duration of Delta flights
flights.filter(flights.carrier == "DL") \
        .filter(flights.origin == "SEA") \
        .groupBy().avg("air_time").show()

# Total hours in the air
flights.withColumn("duration_hrs", flights.air_time/60). \
		groupBy().sum("duration_hrs").show()

# Group by tailnum
by_plane = flights.groupBy("tailnum")

# Number of flights each plane made
by_plane.count().show()

# Group by origin
by_origin = flights.groupBy("origin")

# Average duration of flights from PDX and SEA
by_origin.avg("air_time").show()

# Import pyspark.sql.functions as F
import pyspark.sql.functions as F

# Group by month and dest
by_month_dest = flights.groupBy("month", "dest")

# Average departure delay by month and destination
by_month_dest.avg("dep_delay").show()

# Standard deviation of departure delay
by_month_dest.agg(F.stddev("dep_delay")).show()

# Examine the data
print(airports.show())

# Rename the faa column
airports = airports.withColumnRenamed("faa", "dest")

# Join the DataFrames
flights_with_airports = flights.join(airports, on="dest", how="leftouter")

# Examine the new DataFrame
print(flights_with_airports.show())

# Rename year column
planes = planes.withColumnRenamed("year", "plane_year")

# Join the DataFrames
model_data = flights.join(planes, on="tailnum", how="leftouter")

# Cast the columns to integers
model_data = model_data.withColumn("arr_delay", model_data.arr_delay.cast("integer"))
model_data = model_data.withColumn("air_time", model_data.air_time.cast("integer"))
model_data = model_data.withColumn("month", model_data.month.cast("integer"))
model_data = model_data.withColumn("plane_year", model_data.plane_year.cast("integer"))

# Create the column plane_age
model_data = model_data.withColumn("plane_age", model_data.year - model_data.plane_year)

# Create is_late
model_data = model_data.withColumn("is_late", model_data.arr_delay > 0)

# Convert to an integer
model_data = model_data.withColumn("label", model_data.is_late.cast("integer"))

# Remove missing values
model_data = model_data.filter("arr_delay is not NULL and dep_delay is not NULL and air_time is not NULL and plane_year is not NULL")

# Create a StringIndexer
carr_indexer = StringIndexer(inputCol="carrier", outputCol="carrier_index")

# Create a OneHotEncoder
carr_encoder = OneHotEncoder(inputCol="carrier_index", outputCol="carrier_fact")

# Create a StringIndexer
dest_indexer = StringIndexer(inputCol="dest", outputCol="dest_index")

# Create a OneHotEncoder
dest_encoder = OneHotEncoder(inputCol="dest_index", outputCol="dest_fact")

# Make a VectorAssembler
vec_assembler = VectorAssembler(inputCols=["month", "air_time", "carrier_fact", "dest_fact", "plane_age"], outputCol="features")

# Import Pipeline
from pyspark.ml import Pipeline

# Make the pipeline
flights_pipe = Pipeline(stages=[dest_indexer, dest_encoder, carr_indexer, carr_encoder, vec_assembler])

# Fit and transform the data
piped_data = flights_pipe.fit(model_data).transform(model_data)

# Split the data into training and test sets
training, test = piped_data.randomSplit([.6, .4])

# Import LogisticRegression
from pyspark.ml.classification import LogisticRegression

# Create a LogisticRegression Estimator
lr = LogisticRegression()

# Import the evaluation submodule
import pyspark.ml.evaluation as evals

# Create a BinaryClassificationEvaluator
evaluator = evals.BinaryClassificationEvaluator(metricName="areaUnderROC")

# Import the tuning submodule
import pyspark.ml.tuning as tune

# Create the parameter grid
grid = tune.ParamGridBuilder()

# Add the hyperparameter
grid = grid.addGrid(lr.regParam, np.arange(0, .1, .01))
grid = grid.addGrid(lr.elasticNetParam, [0, 1])

# Build the grid
grid = grid.build()

# Create the CrossValidator
cv = tune.CrossValidator(estimator=lr,
               estimatorParamMaps=grid,
               evaluator=evaluator
               )

# Call lr.fit()
best_lr = lr.fit(training)

# Print best_lr
print(best_lr)

# Use the model to predict the test set
test_results = best_lr.transform(test)

# Evaluate the predictions
print(evaluator.evaluate(test_results))


# Spark URL
# Remore Cluster using Spark URL - spark://<IP address | DNS name>:<port>
# e.g. spark://13.59.151.161.7077
# Local Cluster
# local - only 1 core
# local[4] - 4 cores
# local[*] - all available cores

# Import the PySpark module
from pyspark.sql import SparkSession

# Create SparkSession object
spark = SparkSession.builder \
                    .master('local[*]') \
                    .appName('test') \
                    .getOrCreate()

# What version of Spark?
# (Might be different to what you saw in the presentation!)
print(spark.version)

# Terminate the cluster
spark.stop()

# Loading Data from CSV
# `csv` method treats all columns as string by default
cars = spark.read.csv('cars.csv', header=True, 
                    inferSchema=True, nullValue='NA') 
cars.show(5)
cars.printSchema()
cars.dtype
# Specify column types
schema = StructType([
    StructField("maker", StringType()),
    StructField("model", StringType()),
    StructField("origin" StringType()),
    StructField("type", StringType()),
    StructField("cyl", IntegerType()),
    StructField("size", DoubleType()),
    StructField("weight", IntegerType()),
    StructField("length", DoubleType()),
    StructField("rpm", IntegerType()),
    StructField("consumption", DoubleType())
])

cars = spark.read.csv("cars.csv", header=True, 
                    schema=schema, nullValue='NA')

# Read data from CSV file
flights = spark.read.csv('flights.csv',
                         sep=',',
                         header=True   ,
                         inferSchema=True,
                         nullValue='NA')

# Get number of records
print("The data contain %d records." % flights.count())

# View the first five records
flights.show(5)

# Check column data types
print(flights.dtypes)

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Specify column names and types
schema = StructType([
    StructField("id", IntegerType()),
    StructField("text", StringType()),
    StructField("label", IntegerType())
])

# Load data from a delimited file
sms = spark.read.csv('sms.csv', sep=';', header=False, schema=schema)

# Print schema of DataFrame
sms.printSchema()

# Either drop the columns you don't want
cars = cars.drop('maker', 'model')
# Or select the columns you want to retain
cars = cars.select('origin', 'type', 'cyl', 'size', 'weight', 
                    'length', 'rpm', 'consumption')

# Filetering out missing data
# How many missing values?
cars.filter('cyl is NULL').count()
# Drop records with missing values in the `cylinder` column
cars = cars.filter('cyl is NOT NULL')
# Drop records with missing values in any column
cars = cars.dropna()

# Mutating columns
from pyspark.sql.functions import round
# Create a new `mass` column
cars = cars.withColumn('mass', round(cars.weight / 2.205, 0))
# Convert length to metres
cars = cars.withColumn('length', round(cars.length * 0.0254, 3))

# Indexing categorical data
from pyspark.ml.feature import StringIndexer 
indexer = StringIndexer(inputCol='type', outputCol='type_idx')
# Assign index values to strings
indexer = indexer.fit(cars)
# Create column with index values
cars = indexer.transform(cars)
# By default, the index value is assigned according to the descending relative  ferquency of each of the string values.

# Use `stringOrderType` to change the order
# Index country of origin
# USA -> 0
# non-USA -> 1
cars = StringIndexer(
    inputCol='origin', outputCol='label'
).fit(cars).transform(cars)

# Assembling columns
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=['cyl', 'size'], 
                            outputCol='features')
assembler.transform(cars)

# Remove the 'flight' column
flights_drop_column = flights.drop('flight')

# Number of records with missing 'delay' values
flights_drop_column.filter('delay IS NULL').count()

# Remove records with missing 'delay' values
flights_valid_delay = flights_drop_column.filter('delay IS NOT NULL')

# Remove records with missing values in any column and get the number of remaining rows
flights_none_missing = flights_valid_delay.dropna()
print(flights_none_missing.count())

# Import the required function
from pyspark.sql.functions import round

# Convert 'mile' to 'km' and drop 'mile' column
flights_km = flights.withColumn('km', round(flights.mile * 1.60934, 0)) \
                    .drop('mile')

# Create 'label' column indicating whether flight delayed (1) or not (0)
flights_km = flights_km.withColumn('label', (flights_km.delay >= 15).cast('integer'))

# Check first five records
flights_km.show(5)

from pyspark.ml.feature import StringIndexer

# Create an indexer
indexer = StringIndexer(inputCol='carrier', outputCol='carrier_idx')

# Indexer identifies categories in the data
indexer_model = indexer.fit(flights)

# Indexer creates a new column with numeric index values
flights_indexed = indexer_model.transform(flights)

# Repeat the process for the other categorical feature
flights_indexed = StringIndexer(inputCol='org', outputCol='org_idx').fit(flights_indexed).transform(flights_indexed)

# Import the necessary class
from pyspark.ml.feature import VectorAssembler

# Create an assembler object
assembler = VectorAssembler(inputCols=[
    'mon', 'dom', 'dow',
    'carrier_idx', 
    'org_idx',
    'km',
    'depart',
    'duration'
], outputCol='features')

# Consolidate predictor columns
flights_assembled = assembler.transform(flights)

# Check the resulting column
flights_assembled.select('features', 'delay').show(5, truncate=False)

# Split train/test
# Specify a seed for reproducibility
cars_train, cars_test = cars.randomSplit([0.8, 0.2], seed=23)
print([car_train.count(), cars_test.count()])

# Build a Decision Tree model
from pyspark.ml.classification import DecisionTreeClassifier
# Create a Decision Tree classifier
tree = DecisionTreeClassifier()
# Learn from the training data
tree_model = tree.fit(cars_train)
# Evaluating
prediction = tree_model.transform(cars_test)
# Confusion matrix: a table describes performance of a model on testing data
prediction.groupBy('label', 'prediction').count().show()
# Accuracy = (TN + TP) / (TN + TP + FN + FP) - proportion of correct predictions

# Split into training and testing sets in a 80:20 ratio
flights_train, flights_test = flights.randomSplit([0.8, 0.2], seed=17)

# Check that training set has around 80% of records
training_ratio = flights_train.count() / flights_test.count()
print(training_ratio)

# Import the Decision Tree Classifier class
from pyspark.ml.classification import DecisionTreeClassifier

# Create a classifier object and fit to the training data
tree = DecisionTreeClassifier()
tree_model = tree.fit(flights_train)

# Create predictions for the testing data and take a look at the predictions
prediction = tree_model.transform(flights_test)
prediction.select('label', 'prediction', 'probability').show(5, False)

# Create a confusion matrix
prediction.groupBy('label', 'prediction').count().show()

# Calculate the elements of the confusion matrix
TN = prediction.filter('prediction = 0 AND label = prediction').count()
TP = prediction.filter('prediction = 1 AND label = prediction').count()
FN = prediction.filter('prediction = 0 AND label = 1').count()
FP = prediction.filter('prediction = 1 AND label = 0').count()

# Accuracy measures the proportion of correct predictions
accuracy = (TN + TP) / (TN + TP + FN + FP)
print(accuracy)

# Logistic Regreesion
# Build a Logistic Regression model
from pyspark.ml.classification import LogisticRegression
# Create a Logistic Regression classifier
logistic = LogisticRegression()
# Learn from the training data
logistic = logistic.fit(cars_train)
# Prediction
prediction = logistic.transform(cars_test)
# Precision (positive)
TP / (TP + FP)
# Recall (positive)
TP / (TP + FN)
# Weighted metrics
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator()
evaluator.evaluate(prediction, {evaluator.metricName: 'weightedPrecision'})
# Other metrics include: weightedRecall, accuracy, f1 (the harmonic mean of the precision and the recall)

# ROC = Receiver Operating Characteristic
# TP verse FP
# threshold = 0 (top right)
# threshold = 1 (bottom left)
# AUC = Area Under the Curve
# ideally AUC = 1

# Import the logistic regression class
from pyspark.ml.classification import LogisticRegression

# Create a classifier object and train on training data
logistic = LogisticRegression().fit(flights_train)

# Create predictions for the testing data and show confusion matrix
prediction = logistic.transform(flights_test)
prediction.groupBy('label', 'prediction').count().show()

from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator

# Calculate precision and recall
precision = TP / (TP + FP)
recall = TP / (TP + FN)
print('precision = {:.2f}\nrecall    = {:.2f}'.format(precision, recall))

# Find weighted precision
multi_evaluator = MulticlassClassificationEvaluator()
weighted_precision = multi_evaluator.evaluate(prediction, {multi_evaluator.metricName: "weightedPrecision"})

# Find AUC
binary_evaluator = BinaryClassificationEvaluator()
auc = binary_evaluator.evaluate(prediction, {binary_evaluator.metricName: "areaUnderROC"})

# Turning Text into Tables
# term-document text
# A selection of children's books
books.show(truncate=False)

# Removing punctuation
from pyspark.sql.functions import regexp_replace
# Regular expression (REGEX) to match commas and hyphens
REGEX = '[,\\-]'
books = books.withColumn('text', regexp_replace(books.text, REGEX, ' '))

# Text to tokens
from pyspark.ml.feature import Tokenizer
books = Tokenizer(inputCol='text', outputCol='tokens').transform(books)

# Remove stop words
from pyspark.ml.feature import StopWordsRemover
stopwords = StopWordsRemover()
# Take a look at the list of stop words
stopwords.getStopWords()
# Spevify the input and output column names
stopwords = stopwords.setInputCol('tokens').setOutputCol('words')
books = stopwords.transform(books)

# Feature hashing
from pyspark.ml.feature import HashingTF
hasher = HashingTF(inputCol='words', outputCol='hash', numFeatures=32)
books = hasher.transoform(books)

# Dealing with common words
from pyspark.ml.feature import IDF 
books = IDF(inputCol='hash', outputCol='features').fit(books).transform(books)

# Import the necessary functions
from pyspark.sql.functions import regexp_replace
from pyspark.ml.feature import Tokenizer

# Remove punctuation (REGEX provided) and numbers
wrangled = sms.withColumn('text', regexp_replace(sms.text, '[_():;,.!?\\-]', ' '))
wrangled = wrangled.withColumn('text', regexp_replace(wrangled.text, '[0-9]', ' '))

# Merge multiple spaces
wrangled = wrangled.withColumn('text', regexp_replace(wrangled.text, ' +', ' '))

# Split the text into words
wrangled = Tokenizer(inputCol='text', outputCol='words').transform(wrangled)

wrangled.show(4, truncate=False)

from pyspark.ml.feature import StopWordsRemover, HashingTF, IDF

# Remove stop words.
wrangled = StopWordsRemover(inputCol='words', outputCol='terms').transform(sms)

# Apply the hashing trick
wrangled = HashingTF(inputCol='terms', outputCol='hash', numFeatures=1024).transform(wrangled)

# Convert hashed symbols to TF-IDF
tf_idf = IDF(inputCol='hash', outputCol='features').fit(wrangled).transform(wrangled)
      
tf_idf.select('terms', 'features').show(4, truncate=False)

# Split the data into training and testing sets
sms_train, sms_test = sms.randomSplit([0.8, 0.2], seed=13)

# Fit a Logistic Regression model to the training data
logistic = LogisticRegression(regParam=0.2).fit(sms_train)

# Make predictions on the testing data
prediction = logistic.transform(sms_test)

# Create a confusion matrix, comparing predictions to known labels
prediction.groupBy('label', 'prediction').count().show()

# One-hot encoding
from pyspark.ml.feature import OneHotEncoderEstimator
onehot = OneHotEncoderEstimator(inputCols=['type_idx'], outputCol=['type_dummy'])
# Fit the encoder to the data
onehot = onehot.fit(cars)
# How many category levels?
print(onehot.categorySizes)

cars = onehot.transform(cars)
cars.select('type', 'type_idx', 'type_dummy').distinct().sort('type_idx').show()

# Dense verse sparse
from spark.mllib.linalg import DenseVector, SparseVector
DenseVector([1, 0, 0, 0, 0, 7, 0, 0])
SparseVector(8, [0, 5], [1, 7])

# Import the one hot encoder class
from pyspark.ml.feature import OneHotEncoderEstimator

# Create an instance of the one hot encoder
onehot = OneHotEncoderEstimator(inputCols=['org_idx'], outputCols=['org_dummy'])

# Apply the one hot encoder to the flights data
onehot = onehot.fit(flights)
flights_onehot = onehot.transform(flights)

# Check the results
flights_onehot.select('org', 'org_idx', 'org_dummy').distinct().sort('org_idx').show()

# Regression
from pyspark.ml.regression import LinearRegression
regression = LinearRegression(labelCol='consumption')

regression = regression.fit(cars_train)
predictions = regression.transform(cars_test)

# Calculate RMSE
from pyspark.ml.evaluation import RegressionEvaluator
# Find RMSE
RegressionEvaluator(labelCol='consumption').evaluate(prediction)
# Other metrics: mae, r2, mse
# Examine intercept
print(regression.intercept)
# Examine Coefficients
print(regression.coefficients)

from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Create a regression object and train on training data
regression = LinearRegression(labelCol='duration').fit(flights_train)

# Create predictions for the testing data and take a look at the predictions
predictions = regression.transform(flights_test)
predictions.select('duration', 'prediction').show(5, False)

# Calculate the RMSE
RegressionEvaluator(labelCol='duration').evaluate(predictions)

# Intercept (average minutes on ground)
inter = regression.intercept
print(inter)

# Coefficients
coefs = regression.coefficients
print(coefs)

# Average minutes per km
minutes_per_km = regression.coefficients[0]
print(minutes_per_km)

# Average speed in km per hour
avg_speed = 60 / minutes_per_km
print(avg_speed)

# Average speed in km per hour
avg_speed_hour = 60 / regression.coefficients[0]
print(avg_speed_hour)

# Average minutes on ground at OGG
inter = regression.intercept
print(inter)

# Average minutes on ground at JFK
avg_ground_jfk = inter + regression.coefficients[3]
print(avg_ground_jfk)

# Average minutes on ground at LGA
avg_ground_lga = inter + regression.regression.coefficients[4]
print(avg_ground_lga)

# RPM buckcket
from pyspark.ml.feature import Bucketizer
bucketizer = Bucketizer(split=[3500, 4500, 6000, 6500], 
    inputCol='rpm', outpuCol='rpm_bin')
# Apply bucket to rpm column
cars = bucketizer.transform(cars)
# ROM buckets
bucketed.select('rpm', 'rpm_bin').show(5)
cars.groupBy('rpm_bin').count().show()

# Engineering density
cars = cars.withColumn('density_line', cars.mass / cars.length)  # Linear density
cars = cars.withColumn('density_quad', cars.mass / cars.length ** 2)  # Area density
cars = cars.withColumn('density_cube', cars.mass / cars.length ** 3)  # Volume density

from pyspark.ml.feature import Bucketizer, OneHotEncoderEstimator

# Create buckets at 3 hour intervals through the day
buckets = Bucketizer(splits=[3 * x for x in range(9)], inputCol='depart', outputCol='depart_bucket')

# Bucket the departure times
bucketed = buckets.transform(flights)
bucketed.select('depart', 'depart_bucket').show(5)

# Create a one-hot encoder
onehot = OneHotEncoderEstimator(inputCols=['depart_bucket'], outputCols=['depart_dummy'])

# One-hot encode the bucketed departure times
flights_onehot = onehot.fit(bucketed).transform(bucketed)
flights_onehot.select('depart', 'depart_bucket', 'depart_dummy').show(5)

# Find the RMSE on testing data
from pyspark.ml.evaluation import RegressionEvaluator
RegressionEvaluator(labelCol='duration', metricName='rmse').evaluate(predictions)

# Average minutes on ground at OGG for flights departing between 21:00 and 24:00
avg_eve_ogg = regression.intercept
print(avg_eve_ogg)

# Average minutes on ground at OGG for flights departing between 00:00 and 03:00
avg_night_ogg = regression.intercept + regression.coefficients[8]
print(avg_night_ogg)

# Average minutes on ground at JFK for flights departing between 00:00 and 03:00
avg_night_jfk = regression.intercept + regression.coefficients[3] + regression.coefficients[8]
print(avg_night_jfk)

# Regularization
assembler = VectorAssembler(inputcOLS=[
    'mass', 'cyl', 'type_dummy', 'density_line', 'density_quad', 'density_cube'
    ], outputCol='features')
cars = assembler.transform(cars)

# Fir a standard Linear Regression model to the training data
regression = LinearRegression(labelCol='consumption').fit(cars_train)
# Examine the coefficients
print(regression.coefficients)
# Cars: Ridge regression
# ? = 0.1 | ? = 0 -> Ridge
ridge = LinearRegression(labelCol='consumption', elasticNetParam=0, regParam=0.1)
ridge.fit(cars_train)
# Cars: Lasso regression
# ? = 0.1 | ? = 1 -> Lasso
lasso = LinearRegression(labelCol='consumption', elasticNetParam=1, regParam=0.1)
lasso.fit(cars_train)

from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Fit linear regression model to training data
regression = regression = LinearRegression(labelCol='duration').fit(flights_train)

# Make predictions on testing data
predictions = regression.transform(flights_test)

# Calculate the RMSE on testing data
rmse = RegressionEvaluator(labelCol='duration', metricName='rmse').evaluate(predictions)
print("The test RMSE is", rmse)

# Look at the model coefficients
coeffs = regression.coefficients
print(coeffs)

from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Fit Lasso model (α = 1) to training data
regression = LinearRegression(labelCol='duration', regParam=1, elasticNetParam=1).fit(flights_train)

# Calculate the RMSE on testing data
rmse = RegressionEvaluator(labelCol='duration', metricName='rmse').evaluate(regression.transform(flights_test))
print("The test RMSE is", rmse)

# Look at the model coefficients
coeffs = regression.coefficients
print(coeffs)

# Number of zero coefficients
zero_coeff = sum([beta == 0 for beta in regression.coefficients])
print("Number of coefficients equal to 0:", zero_coeff)

# Pipeline
# Cars model: Steps
indexer = StringIndexer(inputCol='type', outputCol='type_idx')
onehot = OneHotEncoderEstimator(inputCols=['type_idx'], outputCols=['type_dummy'])
assemble = VectorAssembler(inputCols=['mass', 'cyl', 'type_dummy'], outputCol='features')
regression = LinearRegression(labelCol='consumption')
# Cars model: Applying steps
# Training data
indexer = indexer.fit(cars_train)
cars_train = indexer.transform(cars_train)
onthot = onthot.fit(cars_train)
cars_train = onehot.transform(cars_train)
cars_train  =assemble.transform(cars_train)
# Fit model to training data
regression = regression.fit(cars_train)
# Testing data
cars_test = indexer.transform(cars_test)
cars_test = onthot.transform(cars_test)
cars_test = assemble.transform(cars_test)
# Make predictions on testing data
predictions = regression.transform(cars_test)
# Cars model: Pipeline
from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[indexer, onehot, assemble, regression])
pipeline = pipenline.fit(cars_train)
predictions = pipeline.transform(cars_test)
# Cars model: stages
# The LinearRegression object (fourth stage -> index 3)
print(pipeline.stages[3].intercept)
print(pipeline.stages[3].coefficients)

# Convert categorical strings to index values
indexer = StringIndexer(inputCol='org', outputCol='org_idx')

# One-hot encode index values
onehot = OneHotEncoderEstimator(
    inputCols=['org_idx', 'dow'],
    outputCols=['org_dummy', 'dow_dummy']
)

# Assemble predictors into a single column
assembler = VectorAssembler(inputCols=['km', 'org_dummy', 'dow_dummy'], outputCol='features')

# A linear regression object
regression = LinearRegression(labelCol='duration')

# Import class for creating a pipeline
from pyspark.ml import Pipeline

# Construct a pipeline
pipeline = Pipeline(stages=[indexer, onehot, assembler, regression])

# Train the pipeline on the training data
pipeline = pipeline.fit(flights_train)

# Make predictions on the testing data
predictions = pipline.transform(flights_test)

from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF

# Break text into tokens at non-word characters
tokenizer = Tokenizer(inputCol='text', outputCol='words')

# Remove stop words
remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol='terms')

# Apply the hashing trick and transform to TF-IDF
hasher = HashingTF(inputCol=remover.getOutputCol(), outputCol="hash")
idf = IDF(inputCol=hasher.getOutputCol(), outputCol="features")

# Create a logistic regression object and add everything to a pipeline
logistic = LogisticRegression()
pipeline = Pipeline(stages=[tokenizer, remover, hasher, idf, logistic])

# Grid and cross-validation
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
# A grid of parameter values (empty for the moment)
params = ParamGridBuilder().build()
# The cross-validation object
cv = CrossValidator(esrimator=regression,
    estimatorParamMaps=params,
    evaluator=evaluator,
    numFolds=10,
    seed=13)
# Apply cross-validation to the training data
cv = cv.fit(cars_train)
# What's the average RMSE across the folds
print(cv.avgMetrics)
# Make predictions on the original testing data
evaluator.evaluate(cv.transform(cars_test))

# Create an empty parameter grid
params = ParamGridBuilder().build()

# Create objects for building and evaluating a regression model
regression = LinearRegression(labelCol='duration')
evaluator = RegressionEvaluator(labelCol='duration')

# Create a cross validator
cv = CrossValidator(estimator=regression, estimatorParamMaps=params, evaluator=evaluator, numFolds=5)

# Train and test model on multiple folds of the training data
cv = cv.fit(flights_train)

# NOTE: Since cross-valdiation builds multiple models, the fit() method can take a little while to complete.

# Create an indexer for the org field
indexer = StringIndexer(inputCol='org', outputCol='org_idx')

# Create an one-hot encoder for the indexed org field
onehot = OneHotEncoderEstimator(inputCols=['org_idx'], outputCols=['org_dummy'])

# Assemble the km and one-hot encoded fields
assembler = VectorAssembler(inputCols=['km', 'org_dummy'], outputCol='features')

# Create a pipeline and cross-validator.
pipeline = Pipeline(stages=[indexer, onehot, assembler, regression])
cv = CrossValidator(estimator=pipeline,
          estimatorParamMaps=params,
          evaluator=evaluator)

# Grid Search
# Linear regression with an intercept. Fit to training data.
regression = LinearRegression(labelCol='consumption', fitIntercept=True)
regression = regression.fit(cars_train)
evaluator.evaluate(regression.transform(cars_test))
# Linear regression without an intercept. Fit to training data.
regression = LinearRegression(labelCol='consumption', fitIntercept=False)
regression = regression.fit(cars_train)
evaluator.evaluate(regression.transform(cars_test))

from pyspark.ml.tuning import ParamGridBuilder
# Create a parameter grid builder
params = ParamGridBuilder()
# Add grid points
params = params.addGrid(regression.fitIntercept, [True, False])
# Construct the grid
params = params.build()

# How many models?
print('Number of models to be tested: ', len(params))

# Create a cross-validator and fit to the training data
cv = CrossValidator(estimator=regression,
    estimatorParamMaps=params,
    evaluator=evaluator)
cv = cv.setNumFolds(10).setSeed(13).fit(cars_train)
# What's the cross-validated RMSE for each model
print(cv.avgMetrics)
# Access the best model
print(cv.bestModel)
# Or just use the cross-validator object
predictions = cv.transform(cars_test)
# Retrieve the best parameter
print(cv.bestModel.explainParam('fitIntercept'))
# A more complicate grid
params = ParamGridBuilder(). \
addGrid(regression.fitIntercept, [True, False]). \
addGrid(regression.regParam, [0.001, 0.01, 0.1, 1, 10]). \
addGrid(regression.elasticNetParam, [0, 0.25, 0.5, 0.75, 1]). \
build()
# How many models noe?
print('Number of models to be tested: ', len(params))

# Create parameter grid
params = ParamGridBuilder()

# Add grids for two parameters
params = params.addGrid(regression.regParam, [0.01, 0.1, 1.0, 10.0])\
               .addGrid(regression.elasticNetParam, [0.0, 0.5, 1.0])

# Build the parameter grid
params = params.build()
print('Number of models to be tested: ', len(params))

# Create cross-validator
cv = CrossValidator(estimator=pipeline, estimatorParamMaps=params, evaluator=evaluator, numFolds=5)

# Get the best model from cross validation
best_model = cv.bestModel

# Look at the stages in the best model
print(best_model.stages)

# Get the parameters for the LinearRegression object in the best model
best_model.stages[3].extractParamMap()

# Generate predictions on testing data using the best model then calculate RMSE
predictions = best_model.transform(flights_test)
evaluator.evaluate(predictions)

# Create parameter grid
params = ParamGridBuilder()

# Add grid for hashing trick parameters
params = params.addGrid(hasher.numFeatures, (1024, 4096, 16384))\
               .addGrid(hasher.binary, (True, False))

# Add grid for logistic regression parameters
params = params.addGrid(logistic.regParam, (0.01, 0.1, 1.0, 10.0))\
               .addGrid(logistic.elasticNetParam, (0.0, 0.5, 1.0))

# Build parameter grid
params = params.build()

# Ensemble
# Create a forest of trees
from pyspark.ml.classification import RandomForestClassifier
forest = RandomForestClassifier(numTrees=5)
# Fit to the training data
forest.fit(cars_train)
# How to access trees within forest
print(forest.trees)
# Feature importance
print(forest.featureImportance)

# Create a Gradient-Boosted Tree Classifier
from pyspark.ml.classification import GBTClassifier
gbt = GBTClassifier(maxIter=10)
# Fit to the training data
gbt = gbt.fit(cars_train)

# Import the classes required
from pyspark.ml.classification import DecisionTreeClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Create model objects and train on training data
tree = DecisionTreeClassifier().fit(flights_train)
gbt = GBTClassifier().fit(flights_train)

# Compare AUC on testing data
evaluator = BinaryClassificationEvaluator()
evaluator.evaluate(tree.transform(flights_test))
evaluator.evaluate(gbt.transform(flights_test))

# Find the number of trees and the relative importance of features
print(gbt.trees)
print(gbt.featureImportances)

# Create a random forest classifier
forest = RandomForestClassifier()

# Create a parameter grid
params = ParamGridBuilder() \
        .addGrid(forest.featureSubsetStrategy, ['all', 'onethird', 'sqrt', 'log2']) \
        .addGrid(forest.maxDepth, [2, 5, 10]) \
        .build()

# Create a binary classification evaluator
evaluator = BinaryClassificationEvaluator()

# Create a cross-validator
cv = CrossValidator(estimator=forest, estimatorParamMaps=params, 
                    evaluator=evaluator, numFolds=5)

# Average AUC for each parameter combination in grid
avg_auc = cv.avgMetrics

# Average AUC for the best model
best_model_auc =  max(avg_auc)

# What's the optimal parameter value?
opt_max_depth = cv.bestModel.explainParam('maxDepth')
opt_feat_substrat = cv.bestModel.explainParam('featureSubsetStrategy')

# AUC for best model on testing data
best_auc = evaluator.evaluate(cv.transform(flights_test))


















