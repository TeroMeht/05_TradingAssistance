import os
import csv
from datetime import datetime
from ibapi.ticktype import TickTypeEnum

def create_csv_if_not_exists(csv_file_path, headers=None):
    """Creates a CSV file with headers if it doesn't exist. Headers can be specified or left empty.
    
    Args:
        csv_file_path (str): Path to the CSV file to be created.
        headers (list, optional): List of headers to write to the CSV file. Defaults to None.
        
    Returns:
        bool: True if the CSV file was created, False if it already exists.
    """
    if not os.path.exists(csv_file_path):
        with open(csv_file_path, 'w', newline='') as file:
            writer = csv.writer(file)
            if headers:
                writer.writerow(headers)  # Write headers if provided
            print("CSV file did not exist. Created new file with headers.")
        return True
    return False

def recreate_csv_if_older_data(csv_file_path):
    """Checks if the data in the CSV file is older than today, and recreates the file if necessary."""
    if os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0:
        with open(csv_file_path, 'r', newline='') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Read headers
            first_row = next(reader, None)  # Get the first row of data

            if first_row:
                first_timestamp = first_row[0]
                first_date = None
                formats = [
                    '%Y-%m-%d %H:%M:%S.%f',  # Format with microseconds
                    '%Y%m%d %H:%M:%S'         # Compact format without dashes
                ]
                
                for fmt in formats:
                    try:
                        first_date = datetime.strptime(first_timestamp, fmt).date()
                        break
                    except ValueError:
                        continue
                
                if first_date is None:
                    print(f"Unrecognized timestamp format: {first_timestamp}")
                    return False
                
                today = datetime.today().date()
                
                if first_date < today:
                    print("Data is older than today. Recreating CSV file.")
                    with open(csv_file_path, 'w', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(headers)  # Rewrite headers
                    return True
    return False

def read_existing_timestamps(csv_file_path):
    """Reads existing timestamps from the CSV file."""
    existing_timestamps = set()
    if os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0:
        with open(csv_file_path, 'r', newline='') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Skip headers
            existing_timestamps = {row[0] for row in reader}  # Assuming timestamp is the first column
    return existing_timestamps

def filter_new_data(historical_data, existing_timestamps):
    """Filters out new rows based on the existing timestamps."""
    new_rows = []
    for row in historical_data:
        timestamp = row[0]  # Assuming timestamp is the first element in the row
        if timestamp not in existing_timestamps:
            new_rows.append(row)
    return new_rows

def append_new_data_to_csv(csv_file_path, new_rows, file_exists):
    """Appends new data to the CSV file."""
    with open(csv_file_path, 'a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists or os.path.getsize(csv_file_path) == 0:
            writer.writerow(['date', 'open', 'high', 'low', 'close', 'volume'])  # Write headers
        writer.writerows(new_rows)

def save_historical_data(csv_file_path, historical_data):
    """Main function to save historical data to a CSV file, handling creation and recreation if needed."""
    
    headers = ['date', 'open', 'high', 'low', 'close', 'volume']

    # Create CSV if it doesn't exist
    file_created = create_csv_if_not_exists(csv_file_path,headers)
    
    # Recreate CSV if it contains older data
    if not file_created:
        file_recreated = recreate_csv_if_older_data(csv_file_path)
    
    existing_timestamps = set()
    if not file_created and not file_recreated:  # Only read timestamps if the file wasn't recreated or created
        existing_timestamps = read_existing_timestamps(csv_file_path)
    
    # Filter and save new data
    new_rows = filter_new_data(historical_data, existing_timestamps)
    append_new_data_to_csv(csv_file_path, new_rows, os.path.exists(csv_file_path))
    
    return len(new_rows)

def handle_tick_price(csv_file_path, tick_buffer, tickType, price, buffer_limit):
    """Handles incoming tick price updates, buffers the data, and writes to CSV when the buffer is full.
    
    Args:
        csv_file_path (str): Path to the CSV file where tick data is stored.
        tick_buffer (list): A list to buffer incoming tick data before writing to disk.
        tickType (int): The type of tick received (e.g., LAST price).
        price (float): The price from the tick data.
        buffer_limit (int): The limit at which the buffer is flushed to disk.
    """
    # Create CSV if it doesn't exist
    file_created = create_csv_if_not_exists(csv_file_path,headers=None)
        # Recreate CSV if it contains older data
    if not file_created:
        file_recreated = recreate_csv_if_older_data(csv_file_path)
    
    
    # Only process LAST price ticks
    if TickTypeEnum.to_str(tickType) == "LAST":
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f%z')
        tick_buffer.append([timestamp, price])

        # Check if the buffer has reached the limit
        if len(tick_buffer) >= buffer_limit:
            with open(csv_file_path, 'a', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerows(tick_buffer)
                csv_file.flush()  # Ensure data is written to disk immediately
            tick_buffer.clear()  # Clear the buffer after writing
            
        print(f"Timestamp: {timestamp}, price: {price}")

