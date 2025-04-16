import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine , inspect , DateTime
import os
import warnings    
import requests
import json


class Extraction:
    def __init__(self, table_name,engine , dbtype):
        self.table_name = table_name
        self.engine = engine
        self.dbtype = dbtype

    def dbtype_check(self):
        if self.dbtype == "sql":
            return "sql"
        else: return "csv"

    def get_today(self):
        today = pd.to_datetime("today").date()
        return today
    
    def extract_value_from_api(self, api_url):
        response = requests.get(api_url)
        data = response.json()
        return data["rates"]["EGP"]

    def extract_from_api(self, api_url):
        response = requests.get(api_url)
        data = response.json()

        os.makedirs('Extraction', exist_ok=True)  
        table_name = "Currency"
        
        output_file_path = os.path.join('Extraction', f"{table_name}.csv")
        
        data_dict=dict({})
        
        rates_dict = data["rates"]
        df = pd.DataFrame({'Currency': rates_dict.keys(), 'ExchangeRate': rates_dict.values()})
        
        df.to_csv(output_file_path, index=False)
        
        return data["rates"]
    
    def create_engine(self):
        engine=create_engine(self.engine)
        return engine
    
    def insp_table(self):
        engine = self.create_engine()  
        inspector = inspect(engine)  
        tables = inspector.get_table_names()
        return tables

    def read_from_sql(self):
        new_df = pd.read_sql(f"select * from {self.table_name}", con=self.engine)
        return new_df
    
    def read_from_csv(self):
        df = pd.read_csv(f"landing\\{self.table_name}")
        return df
    
    def trans_csv_name(self):
        csv_file_name = self.table_name.split(".")[0]
        return csv_file_name
    
    def load_to_sql(self,df , table_name , engine):
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
    
    
    def save_to_csv(self, df, directory='Extraction'):
        os.makedirs(directory, exist_ok=True)  
        table_name = self.trans_csv_name() 
        
        output_file_path = os.path.join(directory, f"{table_name}.csv")

        df.to_csv(output_file_path, index=False)


ext=Extraction("Currency", "sqlite:///example.db", "sql")
api_url = "https://openexchangerates.org/api/latest.json?app_id=a92f8bad8e044bc79949a676886da2c8"
data = ext.extract_from_api(api_url)
# print(data)
