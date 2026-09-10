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

dir_csv="./files/CD_ProgramClass.csv"


spark = (SparkSession.builder.appName("Analyzing the vocabulary of Pride and Prejudice.").getOrCreate())

shows_with_schema_csv=spark.read.option("multiline","true").csv(dir_csv, sep='|',header=True,inferSchema=True)

##Listing 6.21 Pretty-printing the schema


shows_with_schema_csv=shows_with_schema_csv.select(F.col('*'))

shows_with_schema_csv.printSchema()

shows_with_struct= shows_with_schema_csv.select(
    
    F.struct(
        F.col('ProgramClassID'),F.col('ProgramClassCD'),F.col('EnglishDescription')   


     )
    
)

shows_with_struct.show(n=20,truncate=False)
print('end')

