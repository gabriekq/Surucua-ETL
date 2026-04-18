
import csv
import pandas as pd
import shutil
import os

class ManageFile:

    def __init__(self,file_name_path):
        self.file_name_path=file_name_path
        self.index_next_rows=0
        self.file=open(self.file_name_path, "r",encoding="utf8")

    def read_file_header(self):
       reader= pd.read_csv(self.file_name_path)
       return list(reader.columns)

    def read_next_rows(self,numbers_rows):
        reader=csv.reader(open(self.file_name_path,'r'))
        csv_lines=[]
        for index, line in enumerate(reader, start=self.index_next_rows):

            if(index<=1):
                continue

            if len(csv_lines) <numbers_rows:
                csv_lines.append(line)
            else:
                self.index_next_rows=index
                break
        return csv_lines

    def read_next_rows_2(self,numbers_rows):
        csv_lines=[]
        for index, line in enumerate(csv.reader(self.file), start=self.index_next_rows):

            if(index==0):
                continue

            csv_lines.append(line)
            if len(csv_lines) == numbers_rows:
                self.index_next_rows=index
                break

        return csv_lines


    def move_file(self,folder_path,file_name):
        target_dir= folder_path.split('/')
        target_dir_path=''
        for  value in target_dir:
            if value is target_dir[len(target_dir)-2]:
                break
            target_dir_path+=str().join(value+'/')
        
        print('Target dir used after import '+target_dir_path)
        shutil.move(os.path.join(folder_path, file_name), target_dir_path)

                 
             
    def close_file(self):
        self.file.close()
        print('close file  -> ',self.file.closed)





