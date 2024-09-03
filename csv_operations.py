import os
import csv
from datetime import datetime
from ibapi.ticktype import TickTypeEnum
from datetime import datetime, timedelta


# true if csv is there, otherwise false
def is_csv_found(csv_file_path):
    return os.path.exists(csv_file_path)

   # Create or overwrite the CSV file with the specified headers
def create_csv_file(csv_file_path, headers):
    """Creates a CSV file at the given path with the specified headers.
    """
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        print(f"CSV file '{csv_file_path}' created with headers: {headers}")


def is_there_old_data(csv_file_path):
    """Checks if the data in the CSV file is older than today, and recreates the file if necessary."""
    second_row = read_second_row(csv_file_path)
    
    # If second_row is empty, return False
    if not second_row:
        return False

    try:
        first_date = process_timestamp_from_row(second_row)
        
        # If the first_date could not be parsed, return False
        if first_date is None:
            return False

        today = datetime.today().date()
        
        return first_date < today
    
    except Exception as e:
        print(f"Error while checking old data: {e}")
        return False

# Tää olettaa että csv file löytyi ja että sillä on headerit valmiina
def recreate_csv_file(csv_file_path):
    """Recreates a CSV file at the given path, keeping the existing headers intact.
    """

    with open(csv_file_path, 'r', newline='') as file:
        reader = csv.reader(file)
        headers = next(reader, [])  # Read the headers
        
    # Overwrite the CSV file but keep the headers
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        if headers:
            writer.writerow(headers)  # Write the headers back into the file
    print(f"CSV file '{csv_file_path}' recreated with existing headers.")
    return True

# Käy tsekkaamassa 2. rivin
def read_second_row(csv_file_path):
    """Reads the second row from a CSV file.
    """
    try:
        with open(csv_file_path, 'r', newline='') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the first row (typically the header)
            # Read the second row; returns an empty list if there is no second row
            second_row = next(reader, None)
            if second_row is None:
                print(f"No second row found: {csv_file_path}")
                return []
            return second_row
    except FileNotFoundError:
        print(f"File not found: {csv_file_path}")
        return []
    except Exception as e:
        print(f"Error reading file: {e}")
        return []


def parse_timestamp(timestamp):
    """Parses a timestamp and returns the date if successful, or None if the format is unrecognized.
    """
    formats = [
        '%Y-%m-%d %H:%M:%S.%f',  # Format with microseconds
        '%Y%m%d %H:%M:%S'         # Compact format without dashes
    ]
    
    for fmt in formats:
        try:
            date = datetime.strptime(timestamp, fmt).date()
            return date
        except ValueError:
            continue
    
    print(f"Unrecognized timestamp format: {timestamp}")
    return None


def process_timestamp_from_row(row):
    """Processes the timestamp from a given row.
    """
    if row:
        first_timestamp = row[0]
        first_date = parse_timestamp(first_timestamp)
        return first_date
    
    return None


def recreate_csv_if_older_data(csv_file_path):

    """Checks if the data in the CSV file is older than today, and recreates the file if necessary."""
    second_row = read_second_row(csv_file_path)

    # If the second row is empty, there is no data to check, so return False
    if not second_row:
        return False

    # Process the timestamp from the second row
    first_date = process_timestamp_from_row(second_row)
    
    # If the timestamp could not be parsed, return False
    if first_date is None:
        return False
        
    # Check if the date in the second row is older than today
    today = datetime.today().date()
    if first_date < today:
        recreate_csv_file(csv_file_path)
        return True
        
    return False

def read_existing_timestamps(csv_file_path):
    """Reads existing timestamps from the CSV file."""
    with open(csv_file_path, 'r', newline='') as file:
        reader = csv.reader(file)
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

def append_new_data_to_csv(csv_file_path, new_rows):
    """Appends new data to the CSV file."""
    with open(csv_file_path, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(new_rows)
        # Print the new rows that were added
    if new_rows:
        print(f"{len(new_rows)} new rows added to '{csv_file_path}'.")
        last_row = new_rows[-1]  # Get the last row that was added
        print("Last row added:", last_row)


"Muuta historiadatan formaatti ennen tallentamista"            
def convert_datetime_format(original_datetime_str):
    """Converts a datetime string from 'YYYYMMDD HH:MM:SS' format to 'YYYY-MM-DD HH:MM:SS' format.
    """
    # Define the original format and the target format
    original_format = '%Y%m%d %H:%M:%S'
    target_format = '%Y-%m-%d %H:%M:%S'
    
    # Parse the original datetime string to a datetime object
    datetime_obj = datetime.strptime(original_datetime_str, original_format)
    
    # Format the datetime object to the target format
    converted_datetime_str = datetime_obj.strftime(target_format)
    
    return converted_datetime_str

def process_last_price_tick(tick_buffer, tickType, price, buffer_limit, csv_file_path):
    """Processes LAST price ticks, buffers the data, and writes to CSV when the buffer is full.
    """
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

        # Print the processed tick data
        #print(f"Timestamp: {timestamp}, price: {price}")

def has_excess_data(csv_file_path, max_lines=500):
    """
    Checks if a CSV file has more than `max_lines` lines (including the header).
    """
    with open(csv_file_path, 'r', newline='') as file:
        reader = csv.reader(file)
        total_lines = sum(1 for _ in reader)

    return total_lines > max_lines

def remove_excess_data(csv_file_path, lines_to_remove=400):
    """
    Removes the first `lines_to_remove` data rows from a CSV file, leaving the header and the remaining rows intact.
.
    """
    with open(csv_file_path, 'r', newline='') as file:
        reader = csv.reader(file)
        rows = list(reader)

    if len(rows) > lines_to_remove + 1:  # +1 to account for the header row
        header = rows[0]
        remaining_rows = rows[:1] + rows[lines_to_remove + 1:]  # Keep the header and remove the first `lines_to_remove` rows

        with open(csv_file_path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(remaining_rows)

        print(f"Removed the first {lines_to_remove} data rows from '{csv_file_path}'. {len(remaining_rows) - 1} rows remain.")
        return True

    return False


def save_historical_data(csv_file_path, historical_data):
    """Main function to save historical data to a CSV file, handling creation and recreation if needed."""
    
    headers = ['date', 'open', 'high', 'low', 'close', 'volume']
    
    if not is_csv_found(csv_file_path):
    # Create CSV if it doesn't exist
        create_csv_file(csv_file_path,headers)

    if is_there_old_data(csv_file_path): # Jos csv:ssä on vanhaa dataa tee se uudelleen
    # Recreate leaving headers if there was old data to be found
        recreate_csv_file(csv_file_path)
        existing_timestamps = set()  # Start with an empty set since the file was recreated
    else:
        # Only read existing timestamps if the file was not recreated
        existing_timestamps = read_existing_timestamps(csv_file_path)
    
    # Filter and save new data
    new_rows = filter_new_data(historical_data, existing_timestamps)
    append_new_data_to_csv(csv_file_path, new_rows)


def save_market_data(csv_file_path, tick_buffer, tickType, price, buffer_limit):
    """Handles incoming tick price updates, buffers the data, and writes to CSV when the buffer is full.

    """
    headers = ['time','price']

    if not is_csv_found(csv_file_path):   # Luo uusi csv jos sitä ei löydy
    # Create CSV if it doesn't exist
        create_csv_file(csv_file_path,headers)

    if is_there_old_data(csv_file_path): # Jos csv:ssä on vanhaa dataa tee se uudelleen
    # Recreate leaving headers if there was old data to be found
        recreate_csv_file(csv_file_path)
    
    if has_excess_data(csv_file_path, max_lines=200):
        # Step 2: If there is too much data, remove the excess
        remove_excess_data(csv_file_path, lines_to_remove=100)

  # Process the tick price data
    process_last_price_tick(tick_buffer, tickType, price, buffer_limit, csv_file_path)

