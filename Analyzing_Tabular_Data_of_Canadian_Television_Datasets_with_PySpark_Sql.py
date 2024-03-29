'''
Analyzing Tabular Data with pyspark.sql 


'''
'''
We will focus on below areas

- Reading delimited data into a Pyspark dataframe
- Understanding how PySpark represents tabular data in dataframe
- Ingesting and exploring tabular or relational data
- Selecting, manipulating,renaming and deleting columns in a data frame
- Summarizing data frames for quick exploration


'''

## Creating our SparkSession object to start using PySpark

from pyspark.sql import SparkSession
import pyspark.sql.functions as F 

spark = SparkSession.builder.getOrCreate()

## Reading delimited data into a Pyspark dataframe

'''
Let’s take  very healthy grocery list as an example, and load it into PySpark. To
make things simple, we’ll encode our grocery list into a list of lists. PySpark has multi-
ple ways to import tabular data, but the two most popular are the list of lists and the
pandas data frame.

'''

## Creating a data frame out of grocery list


my_grocery_list = [
["Banana", 2, 1.74],
["Apple", 4, 2.04],
["Carrot", 1, 1.09],
["Cake", 1, 10.99],
]

my_grocery_list = spark.createDataFrame(my_grocery_list,['item','Quantity',"Price"]
)

my_grocery_list.printSchema()

print(my_grocery_list.show())

## PySpark for analyzing and processing tabular data

'''
What are the channels with the greatest and least proportion of commercials?

Before moving to the next section, make sure you have the following. With the exception of the 
large BroadcastLogs file, the rest is in the repository:

 data/BroadcastLogs_2018_Q3_M8.CSV (either download from the website or use the sample from the repo)
 data/broadcast_logs/ReferenceTables
 data/broadcast_logs/data_dictionary.doc

The CSV file format stems from a simple idea: we use text, separated in two-
dimensional records (rows and columns), that are separated by two types of delimit-
ers. Those delimiters are characters, but they serve a special purpose when applied
in the context of a CSV file:

 The first one is a row delimiter. The row delimiter splits the file into logical records. 
There is one and only one record between delimiters.68

 The second one is a field delimiter. Each record is made up of an identical number of fields, 
and the field delimiter tells where one field starts and ends.
'''

## A first pass at the SparkReader specialized for CSV files

## Reading our broadcasting information

import os


DIRECTORY = '/home/surani/python_projects/DataAnalysisWithPythonAndPySpark/Data/broadcast_logs/BroadcastLogs_2018_Q3_M8_sample.CSV'

logs = spark.read.csv(
    DIRECTORY,
    header=True,
    inferSchema=True,
    sep='|',
    timestampFormat="yyyy-MM-dd",
)

## The spark.read.csv function, with every parameter explicitly laid out

logs.printSchema()

logs.show(5)

logs.describe().show()

## Selecting five rows of the first three columns of our data frame

logs.select("BroadcastLogID","LogServiceID","LogDate").show(5,False)

## Four ways to select columns in PySpark, all equivalent in terms of results

# Using the string to column conversion

logs.select("BroadcastLogID","LogServiceID","LogDate")

logs.select(["BroadcastLogID","LogServiceID","LogDate"])

# Passing the column object explicitly

logs.select(
    F.col("BroadcastLogID"),F.col("LogServiceID"),F.col("LogDate"),
)

logs.select(
    *[F.col("BroadcastLogID"),F.col("LogServiceID"),F.col("LogDate")]
)


## Peeking at the data frame in chunks of three columns

import numpy as np

column_split = np.array_split(np.array(logs.columns),len(logs.columns) // 3)

print(column_split)


for x in column_split:
    logs.select(*x).show(100,False)

## Getting rid of columns using the drop() method

logs = logs.drop("BroadcastLogID","SequenceNo")

logs.printSchema()

logs.show(5)


## Getting rid of columns, select style

logs = logs.select(
    *[x for x in logs.columns if x not in ["BroadCastLogID","SequenceNo"]]
)

logs.count()

## Creating what’s not there: New columns with withColumn()

#### Selecting and displaying the Duration column

logs.select(F.col('Duration')).show(5)


print(logs.select(F.col("Duration")).dtypes)

print(logs.select(F.col("Duration")).show())
## Extracting the hours, minutes, and seconds from the Duration column

logs.select(
    F.col("Duration"),
    F.col("Duration").substr(1,2).cast("int").alias("dur_hours"),
    F.col("Duration").substr(4,2).cast('int').alias("dur_minutes"),
    F.col("Duration").substr(7,2).cast('int').alias("dur_seconds"),
).distinct().show()

## Creating a duration in second field from the Duration column

logs.select(
    F.col("Duration"),
    (
        F.col("Duration").substr(1,2).cast("int")*60*60
        + 
        F.col("Duration").substr(4,2).cast("int")*60
        +
        F.col("Duration").substr(7,2).cast("int")
    ).alias("Duration_seconds")
).distinct().show(10)

## Creating a new column with withColumn()

logs = logs.withColumn(
    "Duration_seconds",
    (
        F.col("Duration").substr(1,2).cast("int")*60*60
        + 
        F.col("Duration").substr(4,2).cast("int")*60
        +
        F.col("Duration").substr(7,2).cast("int")
    ),
    
)

logs.printSchema()

## Renaming one column at a type, the withColumnRenamed() way

logs = logs.withColumnRenamed("Duration_seconds","duration_seconds")

logs.printSchema()

## Renaming one column at a type, the withColumnRenamed() way

# Batch lowercasing using the toDF() method

logs.toDF(*[x.lower() for x in logs.columns]).printSchema()

logs.select(sorted(logs.columns)).printSchema()

## Diagnosing a data frame with describe() and summary()


for i in logs.columns:
    logs.describe(i).show()

## Summarizing everything in one fell swoop

for i in logs.columns:
    logs.select(i).summary().show()
    
'''
5. Data frame gymnastics: Joining and grouping

 Joining two data frames together
 Selecting the right type of join for your use case
 Grouping data and understanding the GroupedData transitional object
 Breaking the GroupedData with an aggregation method
 Filling null values in your data frame

'''

## Exploring our first link table: log_identifier

logs_identifier = spark.read.csv(
    '/home/surani/python_projects/DataAnalysisWithPythonAndPySpark/Data/broadcast_logs/ReferenceTables/LogIdentifier.csv',
    sep ="|",
    header=True,
    inferSchema=True,
)

logs_identifier.printSchema()

logs_identifier = logs_identifier.where(F.col("PrimaryFG")== 1)

print(logs_identifier.count())

logs_identifier.show(5)

'''
We have two data frames, logs and log_identifier, each containing a set of columns. We are ready to start joining!
The join operation has three major ingredients:

 Two tables, called a left and a right table, respectively
 One or more predicates, which are the series of conditions that determine how records between the two tables are joined
 A method to indicate how we perform the join when the predicate succeeds and when it fails

'''
## A bare-bone recipe for a join in PySpark

'''
[left ].join([right],
on=[predicate],
how = [method])

'''
logs_and_channel = logs.join(
    logs_identifier,
    on='LogServiceID',
    how='inner'
)

logs_and_channel_verbose = logs.join(
    logs_identifier,
    logs["LogServiceID"]== logs_identifier['LogServiceID']
)

logs_and_channel_verbose.printSchema()

## Using the simplified syntax for equi-joins

logs_and_channels = logs.join(logs_identifier,'LogServiceID')

logs_and_channels.printSchema()

logs_and_channels.show(5)

## Using the origin name of the column for unambiguous selection

logs_and_channels_verbose = logs.join(
    logs_identifier,logs["LogServiceID"]== logs_identifier['LogServiceID']
)

logs_and_channels_verbose.printSchema()


logs_and_channels_verbose.drop(logs_identifier["LogServiceID"]).select("LogServiceID")

## Aliasing our tables to resolve the origin

logs_and_channels_verbose = logs.alias("left").join(
    logs_identifier.alias("right"),
    logs["LogServiceID"]== logs_identifier['LogServiceID'],
)

logs_and_channels_verbose.show(5)

## Linking the category and program class tables using two left joins

cd_category = spark.read.csv(
    '/home/surani/python_projects/DataAnalysisWithPythonAndPySpark/Data/broadcast_logs/ReferenceTables/CD_Category.csv',
    sep ="|",
    header=True,
    inferSchema=True,
).select("CategoryID","CategoryCD",
         F.col("EnglishDescription").alias("Category_Description"),
         )

cd_program_class = spark.read.csv(
    '/home/surani/python_projects/DataAnalysisWithPythonAndPySpark/Data/broadcast_logs/ReferenceTables/CD_ProgramClass.csv',
    sep ="|",
    header=True,
    inferSchema=True,
).select(
    "ProgramClassID",
    "ProgramClassCD",
    F.col("EnglishDescription").alias("ProgramClass_Description"),
)


full_log = logs_and_channels.join(cd_category,'CategoryID',how='left').join(
    cd_program_class,'ProgramClassID',how='left'
)

full_log.printSchema()

full_log.show(5)

full_log.count()

'''
In practical terms, we’ll use groupby()
to answer our original question: what are the channels with the greatest and leastSummarizing the data via groupby and GroupedData
101
proportion of commercials? To answer this, we have to take each channel and sum the
duration_seconds in two ways:
 One to get the number of seconds when the program is a commercial
 One to get the number of seconds of total programming

'''
## Displaying the most popular types of programs

(full_log
 .groupby("ProgramClassCD","ProgramClass_Description")
 .agg(F.sum("duration_seconds").alias("duration_total"))
 .orderBy("duration_total",ascending=False).show(100,False)
 
)

## Alternatively

full_log.groupby("ProgramClassCD","ProgramClass_Description").agg(
    {"duration_seconds":"sum"}
    
).withColumnRenamed("sum(duration_seconds)","duration_total").orderBy(
    "duration_total",ascending=False
).show(100,False)


full_log.groupBy(
    "ProgramClassCD","ProgramClass_Description"
).agg(
    F.sum(F.col("Duration_seconds"))
    
).alias("Duration_seconds").show()

## Computing only the commercial time for each program in our table

'''

When the field of the column ProgramClass, trimmed of spaces at the beginning and
end of the field, is in our list of commercial codes, then take the value of the field in the
column duration_seconds. Otherwise, use zero as a value.

F.when(
F.trim(F.col("ProgramClassCD")).isin(
["COM", "PRC", "PGI", "PRO", "PSA", "MAG", "LOC", "SPO", "MER", "SOL"]
),
F.col("duration_seconds"),
).otherwise(0)

(
F.when([BOOLEAN TEST], [RESULT IF TRUE])
.when([ANOTHER BOOLEAN TEST], [RESULT IF TRUE])
.otherwise([DEFAULT RESULT, WILL DEFAULT TO null IF OMITTED])
)
'''
## Using our new column into agg() to compute our final answer

answer = (
    full_log.groupBy('LogIdentifierID')
    .agg(
        F.sum(
            F.when(
                F.trim(F.col("ProgramClassCD")).isin(
                    ["COM", "PRC", "PGI", "PRO", "PSA", "MAG", "LOC", "SPO", "MER", "SOL"]
                ),
                F.col("duration_seconds"),
            ).otherwise(0)
        ).alias("duration_commercial"),
        F.sum("duration_seconds").alias("duration_total"),
        
    ).withColumn("commercial_ratio",
                 F.col("duration_commercial")/F.col("duration_total")
    )
)

answer.printSchema()

answer.orderBy("commercial_ratio",ascending=False).show(10)

## Dropping only the records that have a null commercial_ratio value

answer_no_null = answer.dropna(subset=["commercial_ratio"])

answer_no_null.orderBy(
    "commercial_ratio",ascending=False
).show(1000,False)

answer_no_null.orderBy(
    "commercial_ratio",ascending=False
).count()

## Filling values to our heart’s content using fillna()
'''
The yin to dropna()’s yang is to provide a default value to the null values. This section covers the fillna() method to replace null values.
fillna() is even simpler than dropna(). This data frame method takes two parameters:

 The value, which is a Python int, float, string, or bool. PySpark will only fill the compatible columns; for instance, if we were to fillna("zero"), our
commercial_ratio, being a double, would not be filled.
 The same subset parameter we encountered in dropna(). We can limit the scope of our filling to only the columns we want.

'''
## Filling numerical records with zero using the fillna() method

answer_no_null = answer.fillna(0)

answer_no_null.orderBy(
    "commercial_ratio",ascending=False
).show(1000,False)

answer_no_null.count()

'''
Summary:


* PySpark implements seven join functionalities, using the common “what?,” “on what?,” and “how?” questions: cross, inner, left, right, full, left semi and left anti.
Choosing the appropriate join method depends on how to process the records that resolve the predicates and those that do not.

* PySpark keeps lineage information when joining data frames. Using this information, we can avoid column naming clashes.

* You can group similar values using the groupby() method on a data frame. The method takes a number of column objects or strings representing columns and
returns a GroupedData object.

* GroupedData objects are transitional structures. They contain two types of columns: the key columns, which are the one you “grouped by” with, and the group
cell, which is a container for all the other columns. The most common way to return to a data frame is to summarize the values in the column via the agg()
function or via one of the direct aggregation methods, such as count() or min().

* You can dro.show(1000,F)p records containing null values using dropna() or replace them with another value with the fillna() method.
'''















