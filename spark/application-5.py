import os
import sys
#from pyspark.sql.functions import split , col, explode, lower, regexp_extract , length
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




logs=logs.select('BroadcastLogID','LogServiceID','LogDate',"Duration")


logs.show(5,False)
##logs = logs.drop("BroadcastLogID", "LogDate")
logs = logs.select(
[x for x in logs.columns if x not in ["BroadcastLogID", "LogServiceID"]]
)

logs= logs.select(
F.col('LogDate').substr(1,4).alias('year'),
F.col('LogDate').substr(3,2).alias('month'),
F.col('LogDate').substr(6,2).alias('day'),
F.col('Duration').substr(1,2).alias('dur_hours'),
F.col('Duration').substr(4,2).alias('dur_minutes'),
F.col('Duration').substr(7,2).alias('dur_seconds'),

)

logs.distinct().show(5,False)




print('end-fim')
