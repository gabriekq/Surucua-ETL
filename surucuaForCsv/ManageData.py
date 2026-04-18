from os import replace
import string
import psycopg2 as psycopg2

class ManageData:

    def __init__(self):
        self.connection = None
        self.sql_create_table='CREATE TABLE {table} () ;'

    def connect_DB(self):
        print('connecting DB')
        connection = psycopg2.connect(database='postgres', host='192.168.99.100', port='5432', user='postgres',
                                      password='password')
        connection.autocommit = False
        self.connection=connection

    def disconect_postgress(self,connection):
        print('close the connection')
        connection.close()

    def create_table(self,table_name):
        print('creating table -> ',table_name)
        cursor=self.connection.cursor()
        table_command=self.sql_create_table.format(table=table_name)
        cursor.execute(table_command)
        self.connection.commit()

    def add_columns_tbl(self,columns_list,table_name):
        cursor=self.connection.cursor()
        for index , field in enumerate(columns_list):
         sql_add_collun='ALTER TABLE {table} ADD {field_name} TEXT;'.format(table=table_name,field_name=field)
         print('Statement to be execute -> ',sql_add_collun)
         cursor.execute(sql_add_collun)
         self.connection.commit()
       
    def format_name_column(self,columns_list):

        for index, field in enumerate(columns_list):    
         columns_list[index]=str(columns_list[index]).replace(' ','_')
            
    def insert_data_rows(self,table_name,columns_list,data_rows):
        sql_insert_statemment='INSERT INTO '+table_name
        sql_colluns=' ('+str().join( column+',' for column in columns_list)[:-1]+')'

        sql_values_statement=' VALUES'+' ('+str().join( '%s,' for column in columns_list)[:-1]+')'

        sql_insert_statemment = sql_insert_statemment+sql_colluns+sql_values_statement
        cursor=self.connection.cursor()
        cursor.executemany(sql_insert_statemment,data_rows)
        self.connection.commit()
        
