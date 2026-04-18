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



my_grocery_list = [
["Banana", 2, 1.74],
["Apple", 4, 2.04],
["Carrot", 1, 1.09],
["Cake", 1, 10.99],
]

df_grocery_list = spark.createDataFrame(
my_grocery_list, ["Item", "Quantity", "Price"]
)


df_grocery_list.printSchema()
df_grocery_list.show()