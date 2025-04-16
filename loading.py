import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine , inspect , DateTime
import os
import warnings    


class loading:
    def __init__(self, csv_file_name,engine):
        self.csv_file_name = csv_file_name
        self.engine = engine

    def create_engine(self):
        engine=create_engine(self.engine)
        return engine
    
    def insp_table(self):
        engine = self.create_engine()  
        inspector = inspect(engine)  
        tables = inspector.get_table_names()
        return tables

    def read_from_csv(self):
        df = pd.read_csv(f"source\\{self.csv_file_name}")
        return df
    
    def trans_csv_name(self):
        csv_file_name = self.csv_file_name.split(".")[0]
        return csv_file_name
    
    def load_to_sql(self,df , table_name , engine):
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
    
    


    def save_to_csv(self, df, directory='landing'):
        os.makedirs(directory, exist_ok=True)  
        table_name = self.trans_csv_name() 
        
        output_file_path = os.path.join(directory, f"{table_name}.csv")

        df.to_csv(output_file_path, index=False)