from math import log
import os
import sys
#from pyspark.sql.functions import split , col, explode, lower, regexp_extract , length
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession , SQLContext
import numpy as np


##pypandoc  pyspark

spark = (SparkSession
.builder
.appName("Analyzing the vocabulary of Pride and Prejudice.")
.getOrCreate())



DIRECTORY = "./files"
logs = spark.read.csv(
os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8.CSV"),
sep="|",
header=True,
inferSchema=True,
timestampFormat="yyyy-MM-dd",
)

print('variable pure')
print(logs)

logs.select(
F.col("Duration"),
(
F.col("Duration").substr(1, 2).cast("int") * 60 * 60
+ F.col("Duration").substr(4, 2).cast("int") * 60+ F.col("Duration").substr(7, 2).cast("int")
).alias("Duration_seconds"),
).distinct().show(5)


logs=logs.withColumn("duration_seconds_2",(
    F.col('Duration').substr(1,2).cast('int')*60*60
    +F.col('Duration').substr(4,2).cast('int')*60
    +F.col('Duration').substr(7,2).cast('int')
    ))

logs.printSchema()
logs.select(F.col('duration_seconds_2')).show()

logs=logs.withColumnRenamed('duration_seconds_2','duration_seconds_Edit')
logs.printSchema()
print('loop')

logs.toDF(*[x.lower() for x in logs.columns]).printSchema()

print('sorted the coluns')
logs.select(sorted(logs.columns)).printSchema()

print('print describe')
for index in logs.columns:
    logs.describe(index).show()