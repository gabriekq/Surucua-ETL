import os
import sys
from pyspark.sql.functions import split , col, explode, lower, regexp_extract , length, greatest
from pyspark.sql import SparkSession
from pyspark.sql.types import *

##pypandoc  pyspark

spark = (SparkSession
.builder
.appName("Analyzing the vocabulary of Pride and Prejudice.").master("local[*]")
.getOrCreate())


exo2_2_df = spark.createDataFrame(
[["test", "more test", 10_000_000_000]], ["one", "two", "three"]
)


print(len([x for x, y in exo2_2_df.dtypes if y != "string"]))


exo2_3_df = (
spark.read.text("./files/1342-0.txt")
.select(length(col("value")))
.withColumnRenamed("length(value)", "number_of_char")
)


exo2_3_df_1 = (
spark.read.text("./files/1342-0.txt")
.select(length(col("value")))
.alias("number_of_char")
)


exo2_4_df = spark.createDataFrame(
[["key", 10_000, 20_000]], ["key", "value1", "value2"]
)

print(exo2_3_df_1.printSchema())

print(exo2_3_df.printSchema())


#words_NoNull=words_clean.select(col("word")).where(col("word") != "is")

df =spark.createDataFrame([('Alice', 1)], ['name', 'age']).show()

