from asyncio import Queue as AsyncQueue
from queue import Queue as ThreadSafeQueue
from multiprocessing import JoinableQueue 
from typing import Union

class QueueFactory:
    
    @staticmethod
    def factory(type: str = 'default') -> Union[ThreadSafeQueue, AsyncQueue, JoinableQueue]:
        if type == 'default':
            return ThreadSafeQueue()
        elif type == 'multi-threading':
            return ThreadSafeQueue()
        elif type == 'multi-processing':
            return JoinableQueue()
        elif type == 'asyncio':
            return AsyncQueue
        else: 
            raise ValueError(type)