import os
import sys
#from pyspark.sql.functions import split , col, explode, lower, regexp_extract , length
import pyspark.sql.functions as F
from pyspark.sql import SparkSession , SQLContext , SaveMode



##pypandoc  pyspark

spark = (SparkSession
.builder
.appName("Analyzing the vocabulary of Pride and Prejudice.")
.getOrCreate())

dir_JSON="./files/recipes_extended.json"
##json_df =spark.read.json(dir_JSON)
spark.conf.set("spark.sql.debug.maxToStringFields",1000*10)

json_df =spark.read.option("multiline","true").json(dir_JSON)
json_df.printSchema()

recipe_df =(json_df.select(F.col('recipe_title'),
                           F.col('category'),
                           F.col('num_ingredients'),
                           F.col('num_steps').alias('steps'),
                           F.col('primary_taste'),
                           F.col('secondary_taste'),
                           F.col('cook_speed'),
                           F.col('est_prep_time_min'),
                           F.col('est_cook_time_min'),
                           F.col('difficulty'),
                           F.col('is_vegan').alias('vegan'),
                           F.col('is_vegetarian').alias('vegetarian'),
                           F.col('is_halal').alias('halal'),
                           F.col('is_kosher').alias('kosher'),
                           F.col('is_nut_free').alias('nut_free'),
                           F.col('is_dairy_free').alias('dairy_free'),
                           F.col('is_gluten_free').alias('gluten_free'),
                           F.col('healthiness_score'),
                           F.col('health_level')
                          )
            )

recipe_df = (recipe_df
             .withColumn('gluten_free_edited',F.when(recipe_df.gluten_free =='TRUE','YES').otherwise('NO')) 
             .withColumn('dairy_free_edited',F.when(recipe_df.dairy_free =='TRUE','YES').otherwise('NO'))
             .withColumn('nut_free_edited',F.when(recipe_df.nut_free =='TRUE','YES').otherwise('NO'))
             .withColumn('kosher_edited',F.when(recipe_df.kosher =='TRUE','YES').otherwise('NO'))
             .withColumn('vegetarian_edited',F.when(recipe_df.vegetarian =='TRUE','YES').otherwise('NO'))
             .withColumn('vegan_edited',F.when(recipe_df.vegan =='TRUE','YES').otherwise('NO'))
             .drop('vegetarian')
             .drop('vegan')
             .drop('halal')
             .drop('kosher')
             .drop('nut_free')
             .drop('dairy_free')
             .drop('gluten_free')
             )


recipe_df.coalesce(1).write.format('com.databricks.spark.csv').csv("./files/recipecsv",header=True)



