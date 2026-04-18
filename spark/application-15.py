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
from py4j.protocol import Py4JJavaError

dir_JSON="./files/json/shows-silicon-valley.json"


episode_links_schema = T.StructType([ T.StructField("self", T.StructType([T.StructField("href", T.StringType())]))])

episode_image_schema = T.StructType([T.StructField("medium", T.StringType()),T.StructField("original", T.StringType()),])

episode_schema_BAD = T.StructType([
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
])

embedded_schema2 = T.StructType([T.StructField("_embedded",T.StructType([T.StructField("episodes", T.ArrayType(episode_schema_BAD))]),)])


spark = (SparkSession.builder.appName("Analyzing the vocabulary of Pride and Prejudice.").getOrCreate())

shows_with_schema_wrong=spark.read.option("multiline","true").json(dir_JSON,mode="FAILFAST",schema=embedded_schema2)

shows_with_schema_wrong=shows_with_schema_wrong.withColumn("episodes", F.col("_embedded.episodes")).drop("_embedded")

shows_with_schema_wrong_1=shows_with_schema_wrong.select(F.col('episodes.url'),F.col('episodes.name'))



try:
 shows_with_schema_wrong_1.select(F.explode("name").alias("name"),F.explode("url").alias("url") ).show(20, False)
 #shows_with_schema_wrong.select(F.col('episodes.url')).show(truncate=False,n=20)
except Py4JJavaError:
 pass
