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



book = spark.read.text("./files")

lines = book.select(F.split(book.value, " ").alias("line"))

words = lines.select(F.explode(F.col("line")).alias("word"))

words_lower = words.select(F.lower(F.col("word")).alias("word_lower"))

words_clean = words_lower.select(
F.regexp_extract(F.col("word_lower"), '[a-z]+', 0).alias("word")
)

words_NoNull=words_clean.select(F.col("word")).where(F.col("word") != "is")

words_no_is_not_the_if = (words_NoNull.where(~F.col("word").isin(["no", "is", "the", "if"])))

print(words.show(15))
print(words_lower.show(15))
print(words_clean.show(15))

print(words_NoNull.show(15))

print(words_no_is_not_the_if.show(15))

big_words=words_clean.select(F.col("word")).where(F.length(F.col("word")) > 3)
print(big_words.show(15))


results = words_NoNull.groupby(F.col("word")).count()
results=results.orderBy(F.col("word").desc()) ##.show(10)

results.write.csv("./files/simple_count.csv")


print(results)
print(results.show())