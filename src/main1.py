import os
from sqlalchemy import create_engine  
import pandas as pd 
from clients.config import PostgresClient


#these are the credentials for sqlalchemy create engine - need to put them somewhere else
host ='localhost'
port = 5432
user = 'postgres'
password = 
db = 'dvdrental'
dbtype = "postgresql+psycopg2"

SQLALCHEMY_DATABASE_URI = f"{dbtype}://{user}:{password}@{host}:{port}/{db}"

conn = create_engine(SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)


#Defintion of Functions
#Takes care of set up part of the ETL
def create_conn(path, section):
    client = PostgresClient()
    con = client.connect_from_config(path, section, autocommit=True)
    return con

# Takes care of the "Extract" part  of ETL
def read_table(sql, con) -> pd.DataFrame:
    df = pd.read_sql(sql=sql, con=con)
    return df

# Takes care of the "Load" part  of ETL
def write_table(df:pd.DataFrame, name, con, schema, if_exist='replace') -> pd.DataFrame:
    df.to_sql(name=name, con=con, if_exists=if_exist, schema=schema, index=False, method='multi')
    return None

def transform_customer(df):
    df.rename(columns={'customer_id': 'sk_customer'}, inplace=True)
    df['name'] = df.first_name + " " + df.last_name
    dim_customer = df[['sk_customer', 'name', 'email']].copy()
    dim_customer.drop_duplicates(inplace=True)
    return dim_customer

def transform_staff(df):
    df.rename(columns={'staff_id':'sk_staff'}, inplace=True)
    df['name'] = df.first_name + " " + df.last_name 
    dim_staff = df[['sk_staff', 'name','email']].copy()
    dim_staff.drop_duplicates(inplace=True)
    return dim_staff


#def transform_store(df,df1,df2,df3,df4):
 #  df.rename(columns={'store_id':'sk_store'}, inplace=True)
  # dim_store=df[['sk_store']]
   #dim_store
   

#ef transform_film():

def transform_date():


#def transform_factrental():


def main():
    
    #parameters to connect to postgresql
    SQLALCHEMY_DATABASE_URI = f"{dbtype}://{user}:{password}@{host}:{port}/{db}"
    conn = create_engine(SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
    
    #customer table dw parameters
    customer = read_table(sql='SELECT * FROM public.customer', con=conn) 
    dim_customer = transform_customer(df=customer)
    load_dim_customer = write_table(df=dim_customer, con=conn, name='dim_customer', schema='dssa', if_exist='replace')
    
    #staff table dw parameters
    staff = read_table(sql='SELECT * FROM public.staff', con=conn) 
    dim_staff= transform_staff(df=staff)
    load_dim_staff = write_table(df=dim_staff, con=conn, name='dim_staff', schema='dssa', if_exist='replace')
    
    #store table dw parameters
    store = read_table(sql='SELECT * FROM public.store', con=conn)
    name= read_table(sql='SELECT * from public.staff', con=conn)
    address = read_table(sql='SELECT * from public.address', con=conn)
    city = read_table(sql='SELECT * from public.city', con=conn)
    country= read_table(sql='SELECT * FROM public.country', con=conn)
    dim_store= transform_store(df=store,df1=name,df2=address,df3=city,df4=country)
    
if __name__ == '__main__':
    main()
