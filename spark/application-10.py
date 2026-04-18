
from math import log
from operator import contains
import os
import sys
#from pyspark.sql.functions import split , col, explode, lower, regexp_extract , length
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession , SQLContext
import numpy as np


#exercice Exercise 5.5

DIRECTORY = "./files"

spark = (SparkSession.builder.appName("Analyzing the vocabulary of Pride and Prejudice.").getOrCreate())


log_identifier = spark.read.csv(
    os.path.join(DIRECTORY,"LogIdentifier.csv"),
    sep="|",
    header=True,
    inferSchema=True,
)

call_signs_df= spark.read.csv(
    os.path.join(DIRECTORY,"Call_Signs.csv"),
    sep=",",
    header=True,
    inferSchema=True,
)

result_df=call_signs_df.join(log_identifier.alias('ident'),on='LogIdentifierID',how='inner').select(F.col('ident.LogIdentifierID'),F.col('ident.LogServiceID'),F.col('Undertaking_Name'))

result_df.show(10,truncate=False)



