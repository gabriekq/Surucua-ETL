from math import log
from operator import contains
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


logs_clean= logs.select([column for column in logs.columns if  'ID' not in column ])

logs_clean.select('*').show()





for name in logs.columns:
   if 'ID' in name:
       print(name)
       logs=logs.drop(name)
       
              
logs.select('*').show()