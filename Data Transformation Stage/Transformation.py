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


            df.to_sql(table_name, con=self.engine, if_exists='replace', index=False)
            return df
 
    def lookup_order_status_num(self, lookup_csv, table_name):
        raw_lookup = self.read_from_csv("E:\\ITI 9 Months\\Python\\bicycle-store-etl-pipeline\\lookup directory\\lookup.csv")

        keys = raw_lookup.columns.astype(int)
        values = raw_lookup.iloc[0].values

        status_map = dict(zip(keys, values))

        orders_df = self.read_from_sql(table_name)

        orders_df["order_status"] = orders_df["order_status"].map(status_map)
        orders_df.to_sql(table_name, con=self.engine, if_exists='replace', index=False)
        return orders_df


    def add_locality_flag(self, orders_table, customers_csv, stores_csv):
        orders_df = self.read_from_sql(orders_table)

        customers_df = self.read_from_csv(customers_csv)
        stores_df = self.read_from_csv(stores_csv)

        merged_df = orders_df.merge(customers_df[["customer_id", "city"]], on="customer_id", how="left")
        merged_df.rename(columns={"city": "customer_city"}, inplace=True)

        merged_df = merged_df.merge(stores_df[["store_id", "city"]], on="store_id", how="left")
        merged_df.rename(columns={"city": "store_city"}, inplace=True)

        merged_df["locality_flag"] = merged_df["customer_city"].str.strip().str.lower() == merged_df["store_city"].str.strip().str.lower()

        merged_df.drop(columns=["customer_city", "store_city"], inplace=True)

        merged_df.to_sql(orders_table, con=self.engine, if_exists='replace', index=False)
        return merged_df
