import pandas as pd
from clients.config import config
import psycopg


class Task():
    
    def __init__(self, func) -> None:
        self.func = func
        
    def run(self, *args, **kwargs):
        result = self.func(*args, **kwargs)
        return result


