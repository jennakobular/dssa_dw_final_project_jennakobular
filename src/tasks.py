import pandas as pd
from clients.config import config
import psycopg
 
def connect_to_dvdrental():
    params=config()
    conn= psycopg.connect(**params)
    cur = conn.cursor()
    return cur
 
 
""" task to select from database table"""
 
def select_from_table(cur, table_name):
    results= cur.execute(f"SELECT * FROM {table_name};").fetchall()
    return results


 


if __name__ == "__main__":
   connect_to_dvdrental()
 
