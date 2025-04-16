import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine , inspect , DateTime
import os
import warnings    
import requests
import json

class Transformation:
    def __init__(self, table_name, engine):
        self.table_name = table_name
        self.engine = engine
        self.create_engine()

    def create_engine(self):
        engine=create_engine(self.engine)
        return engine
    
    def insp_table(self):
        engine = self.create_engine()  
        inspector = inspect(engine)  
        tables = inspector.get_table_names()
        return tables

    def read_from_sql(self , table_name):
        new_df = pd.read_sql(f"select * from {table_name}", con=self.engine)
        return new_df
    
    def read_from_csv(self , csv_file_name):
        df = pd.read_csv(csv_file_name)
        return df
    
    def calc_perfomance(self , table_name):
        df = self.read_from_sql(table_name)
        df["track_performance"]= df["shipped_date"] - df["required_date"]
        df["track_performance_str"] = df["track_performance"].astype(str).str[:2]

        # print(df)
        return df
    
    def track_delivery_performance(self , table_name):
        df = self.calc_perfomance(table_name)
        

        if not df["track_performance_str"].empty:
        
            # if (df["track_performance_str"].str.contains("-").any()): 
            #     df["delivery_on_required"] = False
            # else:
            #     df["delivery_on_required"] = True

            df["delivery_on_required"]=~df["track_performance_str"].str.contains("-")


            print(df["delivery_on_required"])
            return df
 
    def lookup_order_status_num(self , csv_file_name , csv_file_name2):
        df1=self.read_from_csv(csv_file_name)
        df2=self.read_from_csv(csv_file_name2)
        
        # df2["order_status"]= df[""]
        # print(new_df)
        
