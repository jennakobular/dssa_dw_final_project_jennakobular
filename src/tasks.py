import pandas as pd
from psycopg import Connection
from clients.postgres import PostgresClient

DATABASECONFIG = '.config/database.ini'

def setup_client(config_file, section):
    client=PostgresClient
    cursor= client.connect_from_config(path=config_file, section=section)
    return cursor 

def select_from_table(cursor, table_name:str, schema:str):
    cursor.execute(f"SET search_path To {schema}, public;")
    results = cursor.execute(f"SELECT * FROM {table_name};").fetchall()
    print(results)


