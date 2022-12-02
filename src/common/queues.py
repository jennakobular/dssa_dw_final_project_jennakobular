from queue import Queue as ThreadSafeQueue
from typing import Union

class QueueFactory:
    
    @staticmethod
    def factory(type: str = 'default') -> ThreadSafeQueue:
        """

        Args:
            type (str, optional): Defaults to 'default'.

        Raises:
            ValueError: If type is not "default"

        Returns:
            ThreadSafeQueue: a queue that can be used for storing the order of operation for tasks
        """
        if type == 'default':
            return ThreadSafeQueue()
        else: 
            raise ValueError(type)