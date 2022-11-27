#this is another example of a possible way to connect to a database using psycopg and config parser, except this is a class as opposed to a function

from psycopg import connect, Connection
from psycopg.conninfo import make_conninfo
from configparser import ConfigParser


class PostgresClient:
    '''
    Postgres client for working with postgres databases in Python
    
    '''
    def __init__(self, host, port, user, password, dbname):
        
        """this reads credentials from a config file -- for example a database.ini"""
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = dbname
        
    def connect_from_config(self, path:str, section:str, **kwargs) -> Connection:
           
        conn_dict = {}
        config_parser = ConfigParser()
        
        #Below reads the config file
        config_parser.read(path)
        if config_parser.has_section(section):
            config_params = config_parser.items(section)
            for k,v in config_params:
                conn_dict[k]=v
                
        conn = connect(
            conninfo=make_conninfo(**conn_dict),
            **kwargs
        )
        
        conn._check_connection_ok()

        return conn
    
    def connect(self, **kwargs) -> Connection:
       
        conn = connect(   
            connfinfo=make_conninfo(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.database,
                **kwargs)
        )
        #This will check if the connection is okay - this will throw an error if not
        conn._check_connection_ok()
        
        return conn



