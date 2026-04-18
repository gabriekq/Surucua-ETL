import os
import sys
#from pyspark.sql.functions import split , col, explode, lower, regexp_extract , length
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession , SQLContext
import numpy as np
import json
import pprint

#exercice Exercise 5.5

jsonFile = """{"name": "Sample name","keywords": ["PySpark", 3.2, "Data"]}"""

spark = (SparkSession.builder.appName("Analyzing the vocabulary of Pride and Prejudice.").getOrCreate())



exo6_1_json = {"name": "Sample name","keywords": ["PySpark", "Python", "Data"],}

exo6_1_json = json.dumps(exo6_1_json)

sol6_1 = spark.read.json(spark.sparkContext.parallelize([exo6_1_json]))

sol6_1.printSchema()

