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
import pprint
import json

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

##Listing 6.21 Pretty-printing the schema
shows_with_schema=shows_with_schema_wrong.select('*')

shows_with_schema.printSchema()

other_shows_schema = T.StructType.fromJson(
json.loads(shows_with_schema.schema.json())
)

print(other_shows_schema == shows_with_schema.schema)

##Listing 6.21 Pretty-printing the schema


episodes =shows_with_schema.select(
F.explode("_embedded.episodes").alias("episodes"),
F.col("episodes.id"),
F.col("episodes.url"),
)

#episodes.show(5, truncate=70)

episode_name_id=shows_with_schema.select(
    F.map_from_arrays(
    F.col("_embedded.episodes.id"), F.col("_embedded.episodes.name")
).alias("name_id"))

episode_name_id = episode_name_id.select(
F.posexplode("name_id").alias("position", "id", "name")
)

#episode_name_id.show(5)

collected = episodes.groupby("id").agg(
F.collect_list("episodes").alias("episodes")
)
collected.count()
