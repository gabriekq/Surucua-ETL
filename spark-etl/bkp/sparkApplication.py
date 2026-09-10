from ast import Global
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
from surucuaForCsv import ManageData
import csv


def formating_Data_Frame(df_final_param):
 main_list=[]
 for index,row in df_final.iterrows():
     list_values_row=[]
     [list_values_row.append(row.iloc[index]) for index in range(len(row))]
     main_list.append(list_values_row)
 
 return main_list


def process_file_rows(params_func):
    
    while(params_func['exit_now']!=1):

     if params_func['min_value_start'] is None:

      params_func['min_value_start']=shows_with_schema_csv.select(F.min(F.col(coll_key))).toPandas()
      min_value_table=params_func['min_value_start'].iloc[0].iloc[0]
      for row_name in list_columns_csv:
       data_frame=shows_with_schema_csv.select(row_name).where(F.col(coll_key) >=min_value_table).sort(F.asc(coll_key)).limit(20000).toPandas()
       df_final[row_name]=data_frame[row_name].tolist();

     else:
      min_value_table=manag_sql.max_value_fromColumn(table_name=table_name,column_max=coll_key)
      for row_name in list_columns_csv:
       data_frame=shows_with_schema_csv.select(row_name).where(F.col(coll_key) >min_value_table).sort(F.asc(coll_key)).limit(20000).toPandas()
       df_final[row_name]=data_frame[row_name].tolist();
      

     main_list_rows=formating_Data_Frame(df_final)
     manag_sql.insert_data_rows(table_name=table_name, columns_list=list_columns_csv,data_rows=main_list_rows)
     df_final.drop(index=df_final.index,columns=list_columns_csv, inplace=True)
     if(len(main_list_rows)<20000):
      params_func['exit_now']=1


  
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
         .config("spark.executor.memory", '42g')
         .config("spark.driver.memory",'42g')
         .config('spark.executor.cores', '10')
         .config('spark.cores.max', '10')
         .config('spark.sql.files.maxPartitionBytes', '5934217728')
         .config('spark.sql.files.openCostInBytes', '114194304')
         .config('spark.sql.files.minPartitionNum', '17')
         .config('spark.sql.files.maxPartitionNum', '20')
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

 params = {
  'min_value_start':None,
  'exit_now':0  
  }
 
 process_file_rows(params_func=params)
 print('Done for: ',file_name)
