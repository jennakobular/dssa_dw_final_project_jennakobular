from asyncio import Queue as AsyncQueue
from queue import Queue as Queue
from multiprocessing import JoinableQueue 
from typing import Union

class QueueFactory:
    
    @staticmethod
    def factory(type: str = 'default') -> Union[Queue, AsyncQueue, JoinableQueue]:
        if type == 'default':
            return Queue()
        elif type == 'multi-threading':
            return Queue()
        elif type == 'multi-processing':
            return JoinableQueue()
        elif type == 'asyncio':
            return AsyncQueue
        else: 
            raise ValueError(type)