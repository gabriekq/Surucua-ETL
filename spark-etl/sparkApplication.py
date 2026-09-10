from ast import Global, If
from math import log
from operator import contains
import os
import sys
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession , SQLContext
import numpy as np
import pyspark.sql.types as T
from py4j.protocol import Py4JJavaError
import pprint
import json
from surucuaForCsv import ManageData
import csv
import warnings


def formating_Data_Frame(df_final_param):
 main_list=[]
 for index,row in df_final_param.iterrows():
     list_values_row=[]
     [list_values_row.append(row.iloc[index]) for index in range(len(row))]
     tuple_element = tuple(list_values_row)
     main_list.append(tuple_element)
 
 return main_list


def process_file_rows():
   global shows_with_schema_csv
   warnings.filterwarnings("ignore")
   window_func= Window.partitionBy(list_columns_csv[1]).orderBy(coll_key)
   shows_with_schema_csv=shows_with_schema_csv.withColumn('dense_rank_key',F.dense_rank().over(window_func)) #.repartition(15,list_columns_csv[1])
   max_value_dense=shows_with_schema_csv.select(F.max('dense_rank_key')).collect()[0][0]

#   print('max value dense ',max_value_dense)
   
   if max_value_dense > 1000000:
       row_count= shows_with_schema_csv.select(F.count(F.col('*'))).collect()[0][0]
       print('row count ',row_count)
       step_used=90000
   else:
       step_used=1

   max_value_dense=max_value_dense+1
   for index in range(1,max_value_dense,step_used):
    print('current position ',index,' of ',max_value_dense)
    stop_value=index+step_used
    list_dense=np.arange(start=index,stop=stop_value).tolist()
    #print('list to search -> ',list_dense)
    df_final=shows_with_schema_csv.where(F.col('dense_rank_key').isin(list_dense)).select(list_columns_csv).toPandas()
    
    list_dense.clear()
    print('converting')
    list_to_save=formating_Data_Frame(df_final_param=df_final)
    print('saving...')
    manag_sql.insert_data_rows(table_name=table_name,columns_list=list_columns_csv,data_rows=list_to_save)
    print('removing DF')
    df_final.drop(index=df_final.index,columns=list_columns_csv, inplace=True)
    print('saved on database rows count ',len(list_to_save))
    shows_with_schema_csv.unpersist(blocking=False)
    list_to_save.clear()
    
    

 
  
def list_csv_files(path_dir_param):
    all_files=os.listdir(path=path_dir_param)
    files_filtered=[]
    for file in all_files:
        if file.endswith(".csv") == True:
            files_filtered.append(file)
  
    return files_filtered

def get_csv_delimiter(csv_file):
    with open(csv_file,newline='',encoding="utf-8",errors="ignore") as csvFile:
        print('get_csv_delimiter start')
        dialect = csv.Sniffer().sniff(csvFile.read(1024))
        csv_delimiter = dialect.delimiter
        print('get_csv_delimiter end ',csv_delimiter)
        return csv_delimiter


manag_sql=ManageData.ManageData()
manag_sql.connect_DB()
df_final = pd.DataFrame()


dir_csv_files='./files/'
spark = (SparkSession.builder.appName("Analyzing the vocabulary of Pride and Prejudice.")
         .config("spark.executor.memory", '16g') #16
         .config("spark.driver.maxResultSize", '3g')
         .config("spark.driver.memory",'8g') # 4
         .config('spark.executor.cores', '13')
         .config('spark.dynamicAllocation.enabled', 'true') # padrao desativado
         .config('spark.dynamicAllocation.initialExecutors', '10') # padrao desativado
         .config('spark.dynamicAllocation.minExecutors', '4')
         .config('spark.cores.max', '13')
         .config('spark.sql.files.maxPartitionBytes', '934217728')
         .config('spark.sql.files.openCostInBytes', '4194304')
         .config('spark.sql.files.minPartitionNum', '17')
         .config('spark.sql.files.maxPartitionNum', '265')
         .config('spark.sql.shuffle.partitions','3')
         .config('spark.dynamicAllocation.shuffleTracking','true')  # para o primeiro funcionar
        .config('spark.sql.broadcastTimeout','500')
        .config('spark.sql.adaptive.enabled','true')
         .config('spark.sql.dynamicPartitionPruning.enabled','true')
         .config('spark.sql.optimizer.dynamicPartitionPruning.enforceBroadcastReuse','true')
         .getOrCreate())

csv_files_names=list_csv_files(path_dir_param=dir_csv_files)
print(csv_files_names)

#dir_csv="./files/CD_ProgramClass.csv"


## loop starts here  remenber to remove the connection from the place that is now
for file_name in csv_files_names:

 full_path_csv=dir_csv_files+file_name
 csv_delimiter=get_csv_delimiter(csv_file=full_path_csv)

 shows_with_schema_csv=spark.read.option("multiline","false").csv(full_path_csv, sep=csv_delimiter,header=True,inferSchema=False)

 print('Opend file ',file_name)
 coll_key=shows_with_schema_csv.columns[0]
 list_columns_csv =shows_with_schema_csv.columns

 
 table_name= str(file_name).split(sep='.')[0]+'_TBL'
 manag_sql.create_table(table_name=table_name)
 manag_sql.add_columns_tbl(columns_list=list_columns_csv,table_name=table_name)
 manag_sql.drop_tamplete_collumn(table_name=table_name)
 process_file_rows() 

 print('Done for: ',file_name)
