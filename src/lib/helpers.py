import json
import time
import logging
from functools import wraps
import inspect
import asyncclick as click
from datetime import date


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


def update_report_batch_id(file_path, latest_value):
    """
    Update the report_batch_id in the JSON file to the latest_value and save the file.

    Args:
        file_path (str): Path to the JSON file.
        latest_value (int): New value for report_batch_id.

    Returns:
        None
    """
    try:
        # Load the JSON file
        with open(file_path, 'r') as file:
            data = json.load(file)
        
        # Update the report_batch_id
        if 'db' in data and 'params' in data['db']:
            data['db']['params']['report_batch_id'] = latest_value
        else:
            raise KeyError("The required keys (db -> params -> report_batch_id) are missing in the JSON file.")
        
        # Save the updated JSON back to the file
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        
        print(f"report_batch_id successfully updated to {latest_value}")
    
    except Exception as e:
        print(f"An error occurred: {e}")


def get_first_day_of_quarter(quarter_name):
    """
    Returns the first day of the given quarter as a string in the format 'YYYY-MM-DD'.
    
    Parameters:
    quarter_name (str): The quarter name in the format 'YYYYQX', e.g., '2024Q1', '2023Q4'.
    
    Returns:
    str: The first day of the quarter in the format 'YYYY-MM-DD'.
    """
    # Map of quarter to starting month
    quarter_to_month = {
        "Q1": "01",
        "Q2": "04",
        "Q3": "07",
        "Q4": "10"
    }
    
    # Extract the year and quarter
    try:
        year = quarter_name[:4]
        quarter = quarter_name[4:]
        if quarter not in quarter_to_month:
            raise ValueError("Invalid quarter format")
        
        # Formulate the first day of the quarter
        first_day = f"{year}-{quarter_to_month[quarter]}-01"
        return first_day
    except Exception as e:
        return f"Error: {e}"


def validate_year(ctx, param, value):
    current_year = date.today().year
    last_2years = current_year - 2
    if value and value.isdigit():
        year = int(value)
        if year >= last_2years and year <= current_year:
            return str(year)
    raise click.BadParameter(f'Invalid year or the data for year {year} is not available')
