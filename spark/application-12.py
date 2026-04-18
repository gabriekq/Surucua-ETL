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

dir_JSON="./files/json/"

spark = (SparkSession.builder.appName("Analyzing the vocabulary of Pride and Prejudice.").getOrCreate())

shows=spark.read.option("multiline","true").json(dir_JSON)

#print('coluns -> ',len(shows.columns))
#print('count value',shows.count())
print('--------------------------------')
#shows.printSchema()
#print('coluns -> ',shows.columns)

shows.select(F.col('name'),F.col('genres').getItem(0)).show()

array_subset = shows.select("name",
                            shows.genres[0].alias("dot_and_index"),
                            F.col("genres")[0].alias("col_and_index"),
                            shows.genres.getItem(0).alias("dot_and_method"),
                            F.col("genres").getItem(0).alias("col_and_method"),)

array_subset.show(10,truncate=False)

shows_2=array_subset.select('name',
             F.lit('Comedy').alias('one'),
             F.lit('Horror').alias('two'),
             F.lit('Drama').alias('three'),
             F.col('dot_and_index')
             ).select('name',F.array('one','two','three').alias('Some_Genres'),
                             F.array_repeat("dot_and_index", 5).alias("Repeated_Genres"),

                      
             )

shows_2.show(10,truncate=False)

shows_2.select("name", F.size("Some_Genres"), F.size("Repeated_Genres")).show()

shows_2.select("name", F.array_distinct('Repeated_Genres'),F.array('Repeated_Genres') ).show(1,False)

shows_2.select('name',F.col('Some_Genres')[2],F.array_intersect( F.col('Some_Genres'),F.col("Repeated_Genres")   )).alias('intersect').show(truncate=False)

shows_2.select(F.col('Some_Genres'),F.array_position("Some_Genres", "Drama")).show(truncate=False)

columns=['name','language','type']

shows_map=shows.select(*[F.lit(column) for column in columns ], F.array(*columns).alias("values") )

shows_map=shows_map.select(F.array(*columns).alias('keys'),'values')

shows_map.show(truncate=True)


shows_map = shows_map.select(F.map_from_arrays("keys", "values").alias("mapped"))

shows_map.printSchema()

shows_map.select(F.col('mapped.name'),F.col("mapped")["name"],shows_map.mapped["name"]).show()

shows.select("schedule").show(20)

shows.select(F.col("_embedded")).printSchema()
