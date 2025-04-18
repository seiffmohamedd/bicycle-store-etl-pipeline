import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
import os

class Business:
    def __init__(self, source_engine):
        self.source_engine = create_engine(source_engine)

    def create_fact_sales(self, output_dir='Information_Mart'):
        orders = pd.read_sql("SELECT * FROM orders", con=self.source_engine)
        order_items = pd.read_sql("SELECT * FROM order_items", con=self.source_engine)

        fact_table = pd.merge(order_items, orders, on="order_id")
        fact_table = fact_table[[
            "order_id", "customer_id", "staff_id", "store_id",
            "product_id", "quantity", "list_price", "discount"
        ]]

        os.makedirs(output_dir, exist_ok=True)
        fact_table.to_csv(os.path.join(output_dir, "fact_sales.csv"), index=False)
        return fact_table

    def create_fact_inventory(self, products_path, stocks_path, output_dir='Information_Mart'):
        products = pd.read_csv(products_path)
        stocks = pd.read_csv(stocks_path)

        fact_inventory = pd.merge(stocks, products, on="product_id")
        fact_inventory = fact_inventory[[
            "store_id", "product_id", "product_name", 
            "brand_id", "category_id", "quantity", "list_price"
        ]]

        os.makedirs(output_dir, exist_ok=True)
        fact_inventory.to_csv(os.path.join(output_dir, "fact_inventory.csv"), index=False)
        return fact_inventory
