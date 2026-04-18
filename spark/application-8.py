from math import log
from operator import contains
import os
import sys
#from pyspark.sql.functions import split , col, explode, lower, regexp_extract , length
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession , SQLContext
import numpy as np

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

log_identifier = spark.read.csv(
os.path.join(DIRECTORY, "LogIdentifier.csv"),
sep="|",
header=True,
inferSchema=True,
)

log_identifier.show(5)

log_identifier.printSchema()

log_identifier=log_identifier.where(F.col('PrimaryFG')==1)
print(log_identifier.count())

log_identifier.show(5)

logs=logs.join(log_identifier,on='LogServiceID',how='inner')

logs.printSchema()

logs.select(F.col('LogIdentifierID'),F.col('LogServiceID'),F.col('PrimaryFG'),F.col('LogDate')).show(10)

logs_and_channels_verbose = logs.join(
log_identifier, logs["LogServiceID"] == log_identifier["LogServiceID"]
)


logs_and_channels_verbose = logs.alias("left").join(log_identifier.alias("right"),logs["LogServiceID"] == log_identifier["LogServiceID"],)
#logs_and_channels_verbose.drop(F.col("right.LogServiceID")).select("LogServiceID").select(F.col("left.LogServiceID")).show(24)

logs_and_channels_verbose.select(F.col("left.LogServiceID")).show(24)

