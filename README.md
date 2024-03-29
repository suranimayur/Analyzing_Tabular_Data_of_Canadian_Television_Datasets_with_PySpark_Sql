# Analyzing_Tabular_Data_of_Canadian_Television_Datasets_with_PySpark_Sql
SparkCanTV: Exploring Canadian Television Datasets with PySpark

Dataset link: https://open.canada.ca/data/en/dataset/800106c1-0b08-401e-8be2-ac45d62e662e

**Project Summary:**

The project aims to provide a comprehensive guide for analyzing tabular data using PySpark's SQL module. Covering various aspects from basic data ingestion to advanced data manipulation and analysis techniques, the project serves as a valuable resource for data engineers and analysts working with large-scale datasets. 

Key areas of focus include reading delimited data, understanding PySpark's dataframe representation, exploring and processing tabular data, and summarizing data frames for quick insights. Through practical examples and code snippets, users can learn to effectively utilize PySpark for their data analysis needs.

# Creating our SparkSession object to start using PySpark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F 

spark = SparkSession.builder.getOrCreate()

# Reading delimited data into a PySpark dataframe
my_grocery_list = [
    ["Banana", 2, 1.74],
    ["Apple", 4, 2.04],
    ["Carrot", 1, 1.09],
    ["Cake", 1, 10.99],
]

my_grocery_list_df = spark.createDataFrame(my_grocery_list, ['item', 'Quantity', 'Price'])

my_grocery_list_df.printSchema()
print(my_grocery_list_df.show())

# PySpark for analyzing and processing tabular data
DIRECTORY = '/home/surani/python_projects/DataAnalysisWithPythonAndPySpark/Data/broadcast_logs/BroadcastLogs_2018_Q3_M8_sample.CSV'

logs = spark.read.csv(
    DIRECTORY,
    header=True,
    inferSchema=True,
    sep='|',
    timestampFormat="yyyy-MM-dd",
)

logs.printSchema()
logs.show(5)
logs.describe().show()

# Selecting columns
logs.select("BroadcastLogID", "LogServiceID", "LogDate").show(5, False)

# Creating new columns
logs = logs.withColumn(
    "Duration_seconds",
    (F.col("Duration").substr(1, 2).cast("int") * 60 * 60
     + F.col("Duration").substr(4, 2).cast("int") * 60
     + F.col("Duration").substr(7, 2).cast("int"))
)

# Renaming columns
logs = logs.withColumnRenamed("Duration_seconds", "duration_seconds")

# Dropping columns
logs = logs.drop("BroadcastLogID", "SequenceNo")

# Grouping and aggregating data
logs.groupBy("LogServiceID").agg(F.sum("Duration_seconds").alias("total_duration")).show()

# Joining data frames
logs_identifier = spark.read.csv(
    '/home/surani/python_projects/DataAnalysisWithPythonAndPySpark/Data/broadcast_logs/ReferenceTables/LogIdentifier.csv',
    sep="|",
    header=True,
    inferSchema=True,
)

logs_and_channel = logs.join(
    logs_identifier,
    on='LogServiceID',
    how='inner'
)

# Filling null values
logs_filled = logs.fillna(0)

# Final analysis
logs_filled.groupBy("LogServiceID").agg(F.avg("Duration_seconds").alias("avg_duration")).show()




**Project Closure Report:**

Throughout the project, we've covered essential concepts and techniques for analyzing tabular data using PySpark. We began by understanding PySpark's dataframe representation and demonstrated how to read delimited data into a PySpark dataframe. We then explored various methods for manipulating and selecting columns, renaming them, and creating new ones. Additionally, we delved into techniques for summarizing data frames, including descriptive statistics and summary functions.

The project also covered advanced topics such as joining and grouping data frames, which are fundamental for performing insightful analysis on relational data. We explored different types of joins and demonstrated how to handle null values effectively using dropna() and fillna() methods.

In conclusion, this project equips users with the necessary skills and knowledge to leverage PySpark for efficient tabular data analysis. By following the examples and guidelines provided, users can enhance their data processing workflows and derive meaningful insights from their datasets.

