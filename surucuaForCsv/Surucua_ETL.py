
from email import header
from os import listdir, replace
import threading
from threading import *
from datetime import datetime

import ManageData 
import ManageFile

## get file from folder

start = datetime.now()

folder_name='D:/Programacao/DATA-SET/ETL_DATA/'

file_name=listdir(folder_name)[0]
tbl_name=file_name.split(".")[0]+'_TBL'
file_path=folder_name+file_name

manag_sql=ManageData.ManageData()
manag_sql.connect_DB()
manag_sql.create_table(table_name=tbl_name)

file_managed=ManageFile.ManageFile(file_path)



headers_list=file_managed.read_file_header()

manag_sql.format_name_column(headers_list)

manag_sql.add_columns_tbl(headers_list,tbl_name)

loop_exit=False

while loop_exit==False:

    file_rows_list=file_managed.read_next_rows_2(1500)
    ##manag_sql.insert_data_rows(table_name=tbl_name,columns_list=headers_list,data_rows=file_rows_list.copy())

    my_thread=Thread(target=manag_sql.insert_data_rows(table_name=tbl_name,columns_list=headers_list,data_rows=file_rows_list.copy()))
    my_thread.start()

    if len(file_rows_list)==0:
        print(threading.activeCount())
        my_thread.join()
        loop_exit=True

file_managed.close_file()
file_managed.move_file(folder_name,file_name)
end_execution =datetime.now()
 
print('overall execution -> ',end_execution-start)
