import pandas as pd


class Task():
    
    def __init__(self, func):
        self.func = func
        
    def run(self, *args, **kwargs):
        result = self.func(*args, **kwargs)
        return result


