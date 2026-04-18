import os
import sys
#from pyspark.sql.functions import split , col, explode, lower, regexp_extract , length
import pyspark.sql.functions as F
from pyspark.sql import SparkSession , SQLContext



##pypandoc  pyspark

spark = (SparkSession
.builder
.appName("Analyzing the vocabulary of Pride and Prejudice.")
.getOrCreate())



##book = spark.read.text("./files/1342-0.txt")

results = (
spark.read.text("./files")
.select(F.split(F.col("value"), " ").alias("line"))
.select(F.explode(F.col("line")).alias("word"))
.select(F.lower(F.col("word")).alias("word"))
.select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
.where(F.col("word") != "")
.groupby("word")
.count()
)

##results.where()
results.write.csv("./files/simple_count.csv")