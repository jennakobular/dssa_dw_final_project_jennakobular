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
def write_table(df, name, con, schema, if_exist='replace'):
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

#using pandas to transform multiple tables from the public dvd rental schema before loading into dssa schema

def transform_store(store_df,staff_df,address_df,city_df,country_df):
    store_df.rename(columns={'store_id':'sk_store', 'manager_staff_id': 'staff_id'}, inplace=True)
    staff_df['name']= staff_df.first_name + " " + staff_df.last_name
    staff_df = staff_df[['staff_id', 'name']].copy()
    
    country_df= country_df[['country_id', 'country'].copy()]
    
    city_df = city_df[['city_id', 'city', 'country_id']].copy()
    city_df = city_df.merge(country_df, how='inner', on='country_id')
    
    address_df = address_df[['address_id', 'address', 'district', 'city_id']].copy()
    address_df = address_df.merge(city_df, how='inner', on='city_id')
    address_df.rename(columns={'district':'state'}, inplace=True)
    store_df= store_df.merge(staff_df,how='inner',on='staff_id')
    store_df= store_df.merge(address_df, how='inner', on='address_id')
    dim_store=store_df[['sk_store', 'name', 'address', 'city', 'state','country']].copy()
       
    return dim_store
   
#using pandas to transform film table before loading into dssa schema 
def transform_film(film_df, lang_df):
    film_df.rename(columns={'film_id':'sk_film', 'rating':'rating_code','length':'film_duration'}, inplace=True)
    lang_df.rename(columns={'name':'language'}, inplace=True)
    film_df = film_df.merge(lang_df, how='inner', on='language_id')
    dim_film= film_df[['sk_film', 'rating_code','film_duration','rental_duration','language','release_year','title']].copy()
    dim_film.drop_duplicates(inplace=True)
    return dim_film

#using pandas to transform and build the data table prior to loading into dssa schema
def transform_date(date_df):
    
    date_df['sk_date'] = date_df.rental_date.dt.strftime("%Y%m%d").astype('int')
    date_df['date'] = date_df.rental_date.dt.date
    date_df['quarter'] = date_df.rental_date.dt.quarter
    date_df['year'] = date_df.rental_date.dt.year
    date_df['month'] = date_df.rental_date.dt.month
    date_df['day'] = date_df.rental_date.dt.day
    dim_date = date_df[['sk_date', 'date', 'quarter', 'year', 'month', 'day']].copy()
    dim_date.drop_duplicates(inplace=True)
    return dim_date
    

def transform_factrental(customer_df,date_df,store_df,film_df,staff_df):
    
    
    
    return dim_factrental


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
    store = read_table(sql='SELECT * FROM public.store', con=conn)
    staff= read_table(sql='SELECT * from public.staff', con=conn)
    address = read_table(sql='SELECT * from public.address', con=conn)
    city = read_table(sql='SELECT * from public.city', con=conn)
    country= read_table(sql='SELECT * FROM public.country', con=conn)
    dim_store= transform_store(store_df=store,staff_df=staff,address_df=address,city_df=city,country_df=country)
    load_dim_store= write_table(df=dim_store,con=conn, name='dim_store', schema='dssa', if_exist='replace')
    
    #film table dw parameters
    film = read_table(sql='SELECT * FROM public.film', con=conn)
    language= read_table(sql='SELECT * FROM public.language', con=conn)
    dim_film= transform_film(film_df=film, lang_df=language)
    load_dim_film = write_table(df=dim_film, con=conn, name='dim_film', schema='dssa', if_exist='replace')
    
    #date table dw parameters
    date= read_table(sql='SELECT *  From public.rental',con=conn)
    dim_date=transform_date(date_df=date)
    load_dim_date=write_table(df=dim_date,con=conn,name='dim_date',schema='dssa',if_exist='replace')
    
    #fact table dw parameters
    
    
if __name__ == '__main__':
    main()
