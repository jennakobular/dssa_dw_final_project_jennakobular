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


#this function is not needed since now using sqlalchemy
def create_conn(path, section):
    client = PostgresClient()
    con = client.connect_from_config(path, section, autocommit=True)
    return con

#extracts from the dvdrental public schema
def read_table(sql, con):
    df = pd.read_sql(sql, con)
    return df

#loads transformed tables into the dssa schema
def write_table(df:pd.DataFrame, name, con, schema, if_exist='replace') -> pd.DataFrame:
    df.to_sql(name=name, con=con, if_exists=if_exist, schema=schema, index=False, method='multi')
    return None

#using pandas to transform customer table prior to loading into dssa schema
def transform_customer(cust_df):
    cust_df.rename(columns={'customer_id': 'sk_customer'}, inplace=True)
    cust_df['name'] = cust_df.first_name + " " + cust_df.last_name
    dim_customer = cust_df[['sk_customer', 'name', 'email']].copy()
    dim_customer.drop_duplicates(inplace=True)
    return dim_customer

#using pandas to transform staff table prior to loading into dssa schema
def transform_staff(staff_df):
    staff_df.rename(columns={'staff_id':'sk_staff'}, inplace=True)
    staff_df['name'] = staff_df.first_name + " " + staff_df.last_name 
    dim_staff = staff_df[['sk_staff', 'name','email']].copy()
    dim_staff.drop_duplicates(inplace=True)
    return dim_staff


def transform_store(store_df,staff_df,address_df,city_df,country_df):
    
    
    
    
    return dim_store
   
#using pandas to transform film table before loading into dssa schema 
def transform_film(film_df, lang_df):
    film_df.rename(columns={'film_id':'sk_film', 'rating':'rating_code','length':'film_duration'}, inplace=True)
    lang_df.rename(columns={'name':'language'}, inplace=True)
    film_df = film_df.merge(lang_df, how='inner', on='language_id')
    dim_film= film_df[['sk_film', 'rating_code','film_duration','rental_duration','language','release_year','title']].copy()
    dim_film.drop_duplicates(inplace=True)
    return dim_film

#def transform_date():


#def transform_factrental():


def main():
    
    #parameters to connect to postgresql
    SQLALCHEMY_DATABASE_URI = f"{dbtype}://{user}:{password}@{host}:{port}/{db}"
    conn = create_engine(SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
    
    #customer table dw parameters
    customer = read_table(sql='SELECT * FROM public.customer', con=conn) 
    dim_customer = transform_customer(cust_df=customer)
    load_dim_customer = write_table(df=dim_customer, con=conn, name='dim_customer', schema='dssa', if_exist='replace')
    
    #staff table dw parameters
    staff = read_table(sql='SELECT * FROM public.staff', con=conn) 
    dim_staff= transform_staff(staff_df=staff)
    load_dim_staff = write_table(df=dim_staff, con=conn, name='dim_staff', schema='dssa', if_exist='replace')
    
    #store table dw parameters
    #store = read_table(sql='SELECT * FROM public.store', con=conn)
   # name= read_table(sql='SELECT * from public.staff', con=conn)
   # address = read_table(sql='SELECT * from public.address', con=conn)
   # city = read_table(sql='SELECT * from public.city', con=conn)
   # country= read_table(sql='SELECT * FROM public.country', con=conn)
    #dim_store= transform_store(df=store,df1=name,df2=address,df3=city,df4=country)
    
    #film table dw parameters
    film = read_table(sql='SELECT * FROM public.film', con=conn)
    language= read_table(sql='SELECT * FROM public.language', con=conn)
    dim_film= transform_film(film_df=film, lang_df=language)
    load_dim_film = write_table(df=dim_film, con=conn, name='dim_film', schema='dssa', if_exist='replace')
    
if __name__ == '__main__':
    main()
