import pandas as pd
from clients.config import config
import psycopg
from datetime import datetime as dt 
 
def connect_to_dvdrental():
    params=config()
    conn= psycopg.connect(**params)
    cur = conn.cursor()
    return cur
 
 
""" task to select from database table"""
 
def select_from_table(cur, table_name):
    results= cur.execute(f"SELECT * FROM {table_name};").fetchall()
    return results

"""task to convert result to a dataframe"""

def convert_pandas(results):
    df = pd.DataFrame(results)
    return df


def main():
    
   """ 1) need to figure out what to put in here (def main()) -- it seems like this area CAN be hardcoded but unsure of syntax to use still. pg 222 in data wrangling has example of def main()
   
 2) need to figure out how to run through the nec. table names for select_from_table() funct. --> i believe this is touched upon in worker video
 
 3) need to rewatch first task video again -- these have to be extremely basic, no hardcoding
 
 4) first function connect_to_dvd definitely works
 
 5) need to figure out if i should delete tables from pgadmin and just create in code -- i think that may make more sense for me"""

if __name__ == "__main__":
    main()
 
