
from math import log
from operator import contains
import os
import sys
#from pyspark.sql.functions import split , col, explode, lower, regexp_extract , length
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession , SQLContext
import numpy as np
import pyspark.sql.types as T

#exercice Exercise 5.5

dir_JSON="./files/json/shows-silicon-valley.json"

spark = (SparkSession.builder.appName("Analyzing the vocabulary of Pride and Prejudice.").getOrCreate())

shows=spark.read.option("multiline","true").json(dir_JSON,mode="FAILFAST")

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

columns=['name','language','type']

shows_map=shows.select(*[F.lit(column) for column in columns ], F.array(*columns).alias("values") )

shows_map=shows_map.select(F.array(*columns).alias('keys'),'values')

shows_map = shows_map.select(F.map_from_arrays("keys", "values").alias("mapped"))

shows_map.printSchema()



shows.select("schedule").show(20)

shows.select(F.col("_embedded")).printSchema()

shows_clean = shows.withColumn("episodes", F.col("_embedded.episodes")).drop("_embedded")

#shows_clean.show(truncate=False,n=20)

shows_clean.printSchema()

episodes_name = shows_clean.select(F.col("episodes.name"))
episodes_name.printSchema()

episodes_name.select(F.explode("name").alias("name")).show(20, False)

## filds
episode_links_schema = T.StructType([ T.StructField("self", T.StructType([T.StructField("href", T.StringType())]))])

episode_image_schema = T.StructType([T.StructField("medium", T.StringType()),T.StructField("original", T.StringType()),])

episode_schema = T.StructType([
T.StructField("_links", episode_links_schema),
T.StructField("airdate", T.DateType()),
T.StructField("airstamp", T.TimestampType()),
T.StructField("airtime", T.StringType()),
T.StructField("id", T.StringType()),
T.StructField("image", episode_image_schema),
T.StructField("name", T.StringType()),
T.StructField("number", T.LongType()),
T.StructField("runtime", T.LongType()),
T.StructField("season", T.LongType()),
T.StructField("summary", T.StringType()),
T.StructField("url", T.StringType()),
]
)

embedded_schema = T.StructType([T.StructField("_embedded",T.StructType([T.StructField("episodes", T.ArrayType(episode_schema))]),)])
## filds

for column in ["airdate", "airstamp"]:
 shows.select(f"_embedded.episodes.{column}").select(F.explode(column)).show(5,truncate=False)