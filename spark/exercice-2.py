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

def report_unique_words(file_name):
    results = (
    spark.read.text("./files/"+file_name)
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
    .where(F.col("word") != "")
    .groupby("word")
    .count().where(F.col('count')==1)
    )
    results.write.csv("./files/simple_count.csv")



def sample_results(file_name):
    results = (
    spark.read.text("./files/"+file_name)
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))  
    .where(F.col("word") != "") 
    .groupby("word")
    .count().where(F.col('count')==1)
    )

    results.withColumn(
    "first_letter", F.substring(F.col("word"), 1, 1)
    ).groupby(F.col("first_letter")).sum().orderBy(
    "sum(count)", ascending=False
    ).show(5)
    

def sample_results_2(file_name):
    results = (
    spark.read.text("./files/"+file_name)
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))  
    .where(F.col("word") != "") 
    .groupby("word")
    .count().where(F.col('count')==1)
    )

    results.withColumn(
    "first_letter_vowel",
    F.substring(F.col("word"), 1, 1).isin(["a", "e", "i", "o", "u"]),
    ).groupby(F.col("first_letter_vowel")).sum().show()



#sample_results("1342-0.txt")

sample_results_2("1342-0.txt")
#report_unique_words("1342-0.txt")

