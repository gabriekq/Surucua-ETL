
import os
import sys
from pyspark.sql.functions import split , col
from pyspark.sql import SparkSession

##pypandoc  pyspark

spark = (SparkSession
.builder
.appName("Analyzing the vocabulary of Pride and Prejudice.")
.getOrCreate())



book = spark.read.text("./files/1342-0.txt")

lines = book.select(split(book.value, " ").alias("line"))

lines2= book.select(split(col('value'),' ') )

lines3 = lines.withColumnRenamed("split(value, , -1)", "line")

##print(book.printSchema())
##print(book.show(truncate=True,n=5))

#print(lines.show(5))

#print(lines2.show(5))

print(lines3.show(5))


