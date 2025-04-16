
import pandas as pd 
import numpy as np
from sqlalchemy import create_engine , text , table , DateTime


df=pd.read_csv('E:\\ITI 9 Months\\Python\\Days\\Use Case\\source\\customers.csv')
df.head(10)