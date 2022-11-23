import pandas as pd
import psycopg


"""i think i can delete this, this is the same as main.py -- just keeping for my notes rn"""


"""" USE PYPIKA FOR POSTGRESQL BUILDER  - looks like will need to pip install
from pypika import PostgreSQLQuery
from pypika import Query, Schema, Column

will need to import MY pipeline & MY Task & Conifg

this is where I'll build out my DSSA Schema ie

fact_rental = (
    Column('sk_customer', 'INT', False)
     ...
    ....
    
)

in worker vid this is ~16min in -- I am not here yet w. my project, will need to revisit



"""

""" HERE FUNCTIONS WILL GO"""

def create_cursor():
    params=config()
    conn= psycopg.connect(**params)
    cur = conn.cursor()
    return cur

def create_schema(cur, scehma_name):
    q = f"CREATE SCHEMA IF NOT EXISTS (schema_name);"
    cur.execute(q)
    return cur

"""18min in, create table, need pypika installed so i can actually utilize"""
"""stopped @20min in, need to get caught up to this point"""