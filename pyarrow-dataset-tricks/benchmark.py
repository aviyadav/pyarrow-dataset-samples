import os
from time import time
import psutil
from functools import wraps
from timeit import default_timer as timer


def measure_performance(func):
    """Measure execution time and memory usage."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())
        mem_before: float = process.memory_info().rss / (1024 ** 2)  # in MB

        start: float = time()
        result = func(*args, **kwargs)

        end = timer()
        elapsed_time = end - start

        mem_after: float = process.memory_info().rss / (1024 ** 2)  # in MB
        mem_used: float = mem_after - mem_before

        print(f"⏱ {func.__name__} took {elapsed_time:.2f}s, "
              f"used {mem_used:.1f} MB\n")
        return result
    return wrapper