import os 
import matplotlib
from common.tasks import Task 
from common.queues import QueueFactory
from sqlalchemy import create_engine  #for connection to PostgreSQL
import pandas as pd #for data processing and transformation
import networkx as nx
from networkx import (
    is_directed_acyclic_graph,
    is_weakly_connected,
    number_of_nodes,
    is_empty,
    topological_sort
)


#these are the credentials for sqlalchemy create engine 
host = os.environ["host"]
port = os.environ["port"]
user = os.environ["user"]
password = os.environ["password"]
db = os.environ["db"]
dbtype = os.environ["dbtype"]

SQLALCHEMY_DATABASE_URI = f"{dbtype}://{user}:{password}@{host}:{port}/{db}"

#creates a connection to the dvd rental database in PostgreSQL
def create_conn():
    con= create_engine(SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
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

#using pandas to transform and build the date table prior to loading into dssa schema
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
    
#using pandas to transform & build fact rental table prior to loading into dssa schema
def transform_factrental(rental_df,inventory_df,dim_date,dim_film,dim_staff,dim_store):
    
    rental_df.rename(columns={'customer_id':'sk_customer', 'rental_date':'date'}, inplace=True)
    #rental_df['date'] = rental_df.date.dt.date
    rental_df['date']=pd.to_datetime(rental_df.date).dt.date
    rental_df = rental_df.merge(dim_date, how='inner', on='date')
    rental_df = rental_df.merge(inventory_df, how='inner', on='inventory_id')
    rental_df = rental_df.merge(dim_film, how='inner', left_on='film_id', right_on='sk_film')
    
    rental_df = rental_df.merge(dim_staff, how='inner', left_on='staff_id', right_on='sk_staff')
    rental_df = rental_df.merge(dim_store, how='inner', on='name')
    
    rental_df = rental_df.groupby(['sk_customer', 'sk_date', 'sk_store', 'sk_film', 'sk_staff']).agg(count_rentals=('rental_id','count')).reset_index()
    
    dim_factrental = rental_df[['sk_customer', 'sk_date', 'sk_store', 'sk_film', 'sk_staff', 'count_rentals']].copy()
    return dim_factrental

def teardown(con):
    con= create_engine(SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
    con.dispose()
    return None
    


def main():
    """
    This is needed to actually run and execute the functions. The parameters needed in each function are defined below.
        
    """
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
    staff1= read_table(sql='SELECT * from public.staff', con=conn)
    address = read_table(sql='SELECT * from public.address', con=conn)
    city = read_table(sql='SELECT * from public.city', con=conn)
    country= read_table(sql='SELECT * FROM public.country', con=conn)
    dim_store= transform_store(store_df=store,staff_df=staff1,address_df=address,city_df=city,country_df=country)
    load_dim_store= write_table(df=dim_store,con=conn, name='dim_store', schema='dssa', if_exist='replace')
    
    #film table dw parameters
    film = read_table(sql='SELECT * FROM public.film', con=conn)
    language= read_table(sql='SELECT * FROM public.language', con=conn)
    dim_film= transform_film(film_df=film, lang_df=language)
    load_dim_film = write_table(df=dim_film, con=conn, name='dim_film', schema='dssa', if_exist='replace')
    
    #date table dw parameters
    date= read_table(sql='SELECT *  FROM public.rental',con=conn)
    dim_date=transform_date(date_df=date)
    load_dim_date=write_table(df=dim_date,con=conn,name='dim_date',schema='dssa',if_exist='replace')
    
    #fact table dw parameters
    rental = read_table(sql='SELECT * FROM public.rental', con=conn)
    inventory = read_table(sql='SELECT * FROM public.inventory',con=conn)
    dim_factrental = transform_factrental(rental_df=rental,inventory_df= inventory,dim_date=dim_date,dim_film=dim_film,dim_staff=dim_staff,dim_store=dim_store)
    load_dim_factrental=write_table(df=dim_factrental,con=conn,name='fact_rental', schema='dssa',if_exist='replace')    

#creating tasks
    connection = Task(create_conn)
    extract_customer=Task(read_table)
    load_customer=Task(write_table)
    customer_transform = Task(transform_customer)   
    extract_staff=Task(read_table)
    load_staff=Task(write_table)
    extract_store=Task(read_table)
    extract_address=Task(read_table)
    extract_city=Task(read_table)
    extract_country=Task(read_table)
    load_store=Task(write_table)
    staff_transform= Task(transform_staff)
    store_transform=Task(transform_store)
    extract_film= Task(read_table)
    extract_language= Task(read_table)
    load_film= Task(write_table)
    film_transform= Task(transform_film)
    extract_date=Task(read_table)
    load_date=Task(write_table)
    date_transform= Task(transform_date)
    extract_rental=Task(read_table)
    extract_inventory=Task(read_table)   
    load_fact=Task(write_table)
    factrental_transform= Task(transform_factrental)
    end_connection= Task(teardown) 

    #customer table tasks
    extract_customer.run(sql='SELECT * FROM public.customer', con=conn)
    customer_transform.run(cust_df=customer)
    load_customer.run(df=dim_customer, con=conn, name='dim_customer', schema='dssa', if_exist='replace')
    
    #staff table tasks
    extract_staff.run(sql='SELECT * FROM public.staff', con=conn)
    staff_transform.run(staff_df=staff)
    load_staff.run(df=dim_staff, con=conn, name='dim_staff', schema='dssa', if_exist='replace')
    
    #store table tasks
    extract_store.run(sql='SELECT * FROM public.store', con=conn)
    extract_staff.run(sql='SELECT * from public.staff', con=conn)
    extract_address.run(sql='SELECT * from public.address', con=conn)
    extract_city.run(sql='SELECT * from public.city', con=conn)
    extract_country.run(sql='SELECT * FROM public.country', con=conn)
    store_transform.run(store_df=store,staff_df=staff1,address_df=address,city_df=city,country_df=country)
    load_store.run(df=dim_store,con=conn, name='dim_store', schema='dssa', if_exist='replace')
    
    #film table tasks
    extract_film.run(sql='SELECT * FROM public.film', con=conn)
    extract_language.run(sql='SELECT * FROM public.language', con=conn)
    film_transform.run(film_df=film, lang_df=language)
    load_film.run(df=dim_film, con=conn, name='dim_film', schema='dssa', if_exist='replace')
    
    #date table tasks      
    extract_date.run(sql='SELECT *  FROM public.rental',con=conn)
    date_transform.run(date_df=date)
    load_date.run(df=dim_date,con=conn,name='dim_date',schema='dssa',if_exist='replace')
    
    #fact table tasks
    extract_rental.run(sql='SELECT * FROM public.rental', con=conn)
    extract_inventory.run(sql='SELECT * FROM public.inventory',con=conn)
    factrental_transform.run(rental_df=rental,inventory_df= inventory,dim_date=dim_date,dim_film=dim_film,dim_staff=dim_staff,dim_store=dim_store)
    load_fact.run(df=dim_factrental,con=conn,name='fact_rental', schema='dssa',if_exist='replace')    
    
    nodes= [(connection,extract_customer),(extract_customer, customer_transform), (customer_transform,load_customer),
            (connection, extract_staff),(extract_staff, staff_transform),(staff_transform, load_staff),
            (connection,extract_store),(extract_store, extract_staff), (extract_staff,extract_address),(extract_address, extract_city),(extract_city, extract_country), (extract_country,store_transform),(store_transform,load_store),
            (connection,extract_film), (extract_film, extract_language),(extract_language, film_transform),(film_transform, load_film),
            (connection,extract_date), (extract_date, date_transform),(date_transform,load_date),
            (connection,extract_rental), (extract_rental, extract_inventory), (extract_inventory,factrental_transform),(factrental_transform,load_fact)]
    
    DAG = nx.DiGraph(nodes)
    nx.draw(DAG)
    print(DAG)
    assert is_directed_acyclic_graph(DAG) == True
   # is_weakly_connected(DAG)
    assert is_empty(DAG) == False
    


    
if __name__ == '__main__':
    main()
