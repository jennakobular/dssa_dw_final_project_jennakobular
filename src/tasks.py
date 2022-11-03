import pandas as pd
from clients.config import config
import psycopg

def connect():
	""" Connect to the PostgreSQL database server """
	conn = None
	try:
		# read connection parameters
		params = config()

		# connect to the PostgreSQL server
		print('Connecting to the PostgreSQL database...')
		conn = psycopg.connect(**params)
		
		# create a cursor
		cur = conn.cursor()
		
	# execute a statement
		print('PostgreSQL database version:')
		cur.execute('SELECT version()')

		# display the PostgreSQL database server version
		db_version = cur.fetchone()
		print(db_version)
		
	# close the communication with the PostgreSQL
		cur.close()
	except (Exception, psycopg.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
			print('Database connection closed.')


if __name__ == '__main__':
	connect()
 
""" task to select from database table"""
 
def select_from_table(cur, table_name, schema):
  params = config()
  conn = psycopg.connect(**params)
  cur = conn.cursor()
  cur.execute(f"SET search_path TO {schema}, public;")
  results= cur.execute(f"SELECT * FROM {table_name};").fetchall()
  conn.close()
  return results

""" This may be a better def to get connection than the original?

def get_connection():
    params = config()
    try:
        return psycopg.connect(**params)
    except:
        return False
  
conn = get_connection()
  
if conn:
    print("Connection to the PostgreSQL established successfully.")
else:
    print("Connection to the PostgreSQL encountered and error.")

"""

""" even more basic way to get connection -- may be the best
def connect_to_dvdrental():
    params = config()
    conn = psycopg.connect(params)
    cur = conn.cursor()
    return cur
"""