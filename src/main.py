#entry point, main script that will build star schema for project
#this is where data processing script goes
from clients.config import config
import psycopg
from tasks import Task
import pandas as pd

def connect_to_dvdrental():
    params=config()
    conn= psycopg.connect(**params)
    cur = conn.cursor()
    return cur

task0=Task(connect_to_dvdrental)
task0.run()

def select_from_table(cur, table_name):
    results= cur.execute(f"SELECT * FROM {table_name};").fetchall()
    return results

def convert_pandas(results):
    df = pd.DataFrame(results)
    return df

task1=Task(select_from_table)
task1.run(cur= task0, table_name='actor')