import requests
import sqlite3
import json
from datetime import date, timedelta
from config import MARKETSTACK_API_KEY

def create_db_table(db_path):
    """Create the stock_history table in the database if it doesn't exist."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            record_date TEXT,
            open_price REAL,
            close_price REAL,
            high_price REAL,
            low_price REAL,
            volume INTEGER,
            return_percentage REAL,
            price_range REAL,
            full_data_json TEXT,
            UNIQUE(symbol, record_date)
        )
    ''')
    conn.commit()
    conn.close()


def get_date_chunks(start_date, end_date, chunk_size):
    """Split a date range into chunks for batch API requests."""
    date_list = []
    current_start = start_date
    
    while current_start <= end_date:
        # Calculate the end date for this chunk
        current_end = current_start + timedelta(days=chunk_size - 1)
        
        if current_end > end_date:
            current_end = end_date
            
        # Add the (start date, end date) pair to the list
        date_list.append((current_start, current_end))
        
        # Move start date forward for the next loop
        current_start = current_end + timedelta(days=1)
        
    return date_list


def save_to_db(db_path, stock_data):
    """Save stock data to the database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    count = 0
    
    for record in stock_data:
        symbol = record.get('symbol')
        record_date = record.get('date', '')[:10]  # Extract YYYY-MM-DD from datetime
        open_price = record.get('open')
        close_price = record.get('close')
        high_price = record.get('high')
        low_price = record.get('low')
        volume = record.get('volume')
        
        # Calculate derived metrics
        if open_price and close_price and open_price != 0:
            return_percentage = ((close_price - open_price) / open_price) * 100
        else:
            return_percentage = None
            
        if high_price and low_price:
            price_range = high_price - low_price
        else:
            price_range = None
        
        # Serialize the full record for storage
        full_json_str = json.dumps(record)
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO stock_history 
                (symbol, record_date, open_price, close_price, high_price, 
                 low_price, volume, return_percentage, price_range, full_data_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (symbol, record_date, open_price, close_price, high_price,
                  low_price, volume, return_percentage, price_range, full_json_str))
            count += 1
        except Exception as e:
            print(f"Error saving data for {symbol} on {record_date}: {e}")
    
    conn.commit()
    conn.close()
    return count


def fetch_stock_data(access_key, symbols, db_path='stock_data.db'):
    """
    Main function to fetch stock data from Marketstack API.
    
    Parameters:
        access_key: Marketstack API key
        symbols: List of stock ticker symbols (e.g., ['DAL', 'UAL', 'AAL', 'LUV'])
        db_path: Path to SQLite database file
    """
    # Initialize the database
    create_db_table(db_path)
    
    # Set the date range: full year of 2024
    start_date = date(2024, 1, 1)
    end_date = date(2024, 12, 31)
    
    base_url = "http://api.marketstack.com/v1/eod"
    batch_size = 25  # Number of days per API request
    
    # Convert symbols list to comma-separated string
    symbols_str = ','.join(symbols)
    
    print(f"Starting 2024 stock data fetch for {symbols_str} ...")
    
    # Get all date chunk pairs
    all_chunks = get_date_chunks(start_date, end_date, batch_size)
    
    # Loop through the list and fetch data
    for batch in all_chunks:
        batch_start = batch[0]
        batch_end = batch[1]
        
        # Convert date objects to strings like 2024-01-01
        str_start = batch_start.strftime("%Y-%m-%d")
        str_end = batch_end.strftime("%Y-%m-%d")
        
        print(f"Fetching {str_start} to {str_end} ...")
        
        params = {
            'access_key': access_key,
            'symbols': symbols_str,
            'date_from': str_start,
            'date_to': str_end,
            'limit': 1000  # Maximum records per request
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Check for API errors
            if 'error' in data:
                print(f"API error: {data.get('error')}")
                continue
            
            stock_records = data.get('data', [])
            if not stock_records:
                print("No stock data in response.")
                continue
            
            saved_count = save_to_db(db_path, stock_records)
            print(f"Saved {saved_count} stock records.")
            
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")

    print("--- Done ---")


if __name__ == "__main__":
    API_KEY = MARKETSTACK_API_KEY
    
    # Major US airline stock symbols
    AIRLINE_SYMBOLS = [
        'DAL',  # Delta Air Lines
        'UAL',  # United Airlines
        'AAL',  # American Airlines
        'LUV'   # Southwest Airlines
    ]
    
    fetch_stock_data(API_KEY, AIRLINE_SYMBOLS)

