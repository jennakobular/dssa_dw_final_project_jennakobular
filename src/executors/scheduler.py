from apscheduler.schedulers.background import BackgroundScheduler

class DefaultScheduler(BackgroundScheduler):
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DefaultScheduler, cls).__new__(cls)
        return cls._instance