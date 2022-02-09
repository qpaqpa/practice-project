from pyspark import SparkContext
sc = SparkContext.getOrCreate()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('lorentzyeung-logistic-regression-churn').getOrCreate()
from pyspark.ml.classification import LogisticRegression
data = spark.read.csv("Sales_old.csv", header=True, inferSchema=True)
data.show(5, truncate=True)
data.printSchema()