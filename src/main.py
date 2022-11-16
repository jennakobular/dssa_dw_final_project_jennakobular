#entry point, main script that will build star schema for project
#this is where data processing script goes
from clients.config import config
import psycopg
from tasks import Task

def connect_to_dvdrental():
    params=config()
    conn= psycopg.connect(**params)
    cur = conn.cursor()
    return cur

task0=Task(connect_to_dvdrental)
task0.run()
