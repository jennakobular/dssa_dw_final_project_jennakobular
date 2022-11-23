#entry point, main script that will build star schema for project
#this is where data processing script goes
from clients.config import config
import psycopg
from psycopg import Cursor
from tasks import Task
import pandas as pd
from pypika import Schema, Column, PostgreSQLQuery

#Parameters
DW = Schema('dssa')

#Table Definitions

FACT_RENTAL = (
    Column('sk_customer','INT', False),
    Column('sk_date', 'INT', False),
    Column('sk_store','INT', False),
    Column('sk_film', 'INT', False),
    Column('sk_staff', 'INT', False),
    Column('count_rentals', 'INT',False)
)

DIM_CUSTOMER = (
    Column('sk_customer', 'INT', False),
    Column('name','VARCHAR(100)', False),
    Column('email', 'VARCHAR(100)', False)
)

DIM_STAFF = (
    Column('sk_staff', 'INT', False),
    Column('name','VARCHAR(100)', False),
    Column('email', 'VARCHAR(100)', False)
)

DIM_FILM = (
    Column('sk_film', 'INT', False),
    Column('rating_code', 'VARCHAR(20)', False),
    Column('film_duration', 'INT', False),
    Column('rental_duration', 'INT', False),
    Column('language', 'CHAR(20)', False),
    Column('release_year','INT', False),
    Column('title', 'VARCHAR(255)',False)
    )

DIM_DATE = (
    Column('sk_date', 'INT', False),
    Column('quarter', 'INT', False),
    Column('year', 'INT', False),
    Column('month', 'INT', False),
    Column('day', 'INT',False)    
)

DIM_STORE = (
    Column('sk_store', 'INT',False),
    Column('name', 'VARCHAR(100)',False),
    Column('address','VARCHAR(50)',False),
    Column('city', 'VARCHAR(50)',False),
    Column('state', 'VARCHAR(20)', False),
    Column('country','VARCHAR(50)',False)
)
    
    

def connect_to_dvdrental():
    params=config()
    conn= psycopg.connect(**params)
    cursor = conn.cursor()
    return cursor

task0=Task(connect_to_dvdrental)
task0.run()

def create_schema(cursor, schema_name):
    a = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    cursor.execute(a)
    return cursor

def select_from_table(cursor, table_name):
    results= cursor.execute(f"SELECT * FROM {table_name};").fetchall()
    return results

def convert_pandas(results):
    df = pd.DataFrame(results)
    return df


"""task1=Task(select_from_table)
task1.run(cur= task0, table_name='actor')"""