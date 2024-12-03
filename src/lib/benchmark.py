import time
import logging
from functools import wraps
import inspect


logger = logging.getLogger(__name__)  # Create a logger instance

def benchmark(enabled=True):
    def decorator(func):
        if not enabled:
            return func  # Return the original function without wrapping

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            result = func(*args, **kwargs)  # Execute the synchronous function
            end_time = time.perf_counter()
            execution_time = end_time - start_time
            logger.info(f"Sync function '{func.__name__}' executed in {execution_time:.6f} seconds.")
            return result

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            result = await func(*args, **kwargs)  # Await the asynchronous function
            end_time = time.perf_counter()
            execution_time = end_time - start_time
            logger.info(f"Async function '{func.__name__}' executed in {execution_time:.6f} seconds.")
            return result

        # Determine if the function is asynchronous
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator
