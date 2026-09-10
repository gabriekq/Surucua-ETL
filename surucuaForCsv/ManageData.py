from os import replace
import string
#from dotenv import load_dotenv
#from mssql_python import connect
#from os import getenv
import pyodbc
import pandas as pd

class ManageData:

    def __init__(self):
        self.connection = None
        

    def connect_DB(self):
        print('connecting DB')
        conn_str = ('Driver=ODBC Driver 17 for SQL Server;Server=GABRIEL-PC;Database=master;Trusted_Connection=yes;')
        connection = pyodbc.connect(conn_str)
        connection.autocommit = False
        connection.timeout = 0
        self.connection=connection
        self.cursor = self.connection.cursor()
        self.cursor.fast_executemany = False
        
        

    def disconect_postgress(self,connection):
        print('close the connection')
        connection.close()

    def create_table(self,table_name):
        print('creating table -> ',table_name)
        table_command='CREATE TABLE {table} (TBDelete Text);'.format(table=table_name)
        try:
            self.cursor.execute(table_command)
        except pyodbc.Error as ex:
            print(ex[1])
            self.connection.rollback()
        else:
            self.connection.commit()

    def add_columns_tbl(self,columns_list,table_name):
        
        for index , field in enumerate(columns_list):
         sql_add_collun='ALTER TABLE {table} ADD {field_name} TEXT;'.format(table=table_name,field_name=field)
         print('Statement to be execute -> ',sql_add_collun)
         try:
             self.cursor.execute(sql_add_collun)
         except pyodbc.Error as ex:
             print(ex[1])
             self.connection.rollback()
         else:
             self.connection.commit()

    def drop_tamplete_collumn(self,table_name):
        
        sql_add_collun='ALTER TABLE {table} DROP COLUMN TBDelete ;'.format(table=table_name)
        print('Statement to be execute -> ',sql_add_collun)
        try:
            self.cursor.execute(sql_add_collun)
        except pyodbc.Error as ex:
            print(ex[1])
            self.connection.rollback()
        else:
            self.connection.commit()

        
       
    def format_name_column(self,columns_list):

        for index, field in enumerate(columns_list):    
         columns_list[index]=str(columns_list[index]).replace(' ','_')
            
    def insert_data_rows(self,table_name,columns_list,data_rows):
        sql_insert_statemment='INSERT INTO '+table_name
        sql_colluns=' ('+str().join( column+',' for column in columns_list)[:-1]+')'

        sql_values_statement=' VALUES'+' ('+str().join( '?,' for column in columns_list)[:-1]+')'
        sql_insert_statemment = sql_insert_statemment+sql_colluns+sql_values_statement
        try:
         self.cursor.executemany(sql_insert_statemment,data_rows)
        except:
         self.connection.rollback()
        else:
         self.connection.commit()
         
        

