import os
from sqlalchemy import create_engine 

host ='localhost'
port = 5432
user = 'postgres'
password = 'Frog$1Ow'
db = 'dvdrental'
dbtype = "postgresql+psycopg2"

SQLALCHEMY_DATABASE_URI = f"{dbtype}://{user}:{password}@{host}:{port}/{db}"

conn = create_engine(SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)