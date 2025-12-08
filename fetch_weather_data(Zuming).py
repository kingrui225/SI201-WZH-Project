import requests
import sqlite3
import json
from datetime import date, timedelta
from config import WEATHERSTACK_API_KEY 

def create_db_table(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location TEXT,
            record_date TEXT,
            avg_temp REAL,
            min_temp REAL,
            max_temp REAL,
            full_data_json TEXT, 
            UNIQUE(location, record_date)
        )
    ''')
    conn.commit()
    conn.close()

def get_date_chunks(start_date, end_date, chunk_size):
    date_list = []
    current_start = start_date
    while current_start <= end_date:
        # Calculate the end date
        current_end = current_start + timedelta(days=chunk_size - 1)
        
        if current_end > end_date:
            current_end = end_date
            
        # add the (start date, end date) pair to the list
        date_list.append((current_start, current_end))
        
        # Move start date forward for the next loop
        current_start = current_end + timedelta(days=1)
        
    return date_list # Return the list

def save_to_db(db_path, location, weather_data):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    count = 0
    
    # weather_data is a dict keyed by date with each day's details
    for date_str in weather_data:
        details = weather_data[date_str]
        
        # Extract the fields we need
        avg_temp = details.get('avgtemp')
        min_temp = details.get('mintemp')
        max_temp = details.get('maxtemp')
        
        # Serialize the dict so it can be stored in the database
        full_json_str = json.dumps(details)

        try:
            cursor.execute('''
                INSERT OR REPLACE INTO weather_history 
                (location, record_date, avg_temp, min_temp, max_temp, full_data_json)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (location, date_str, avg_temp, min_temp, max_temp, full_json_str))
            count = count + 1
        except Exception as e:
            print(f"Error saving data for {date_str}: {e}")
    
    conn.commit()
    conn.close()
    return count

def fetch_weather_data(access_key, location, db_path='weather_data.db'):
    #Initialize the database
    create_db_table(db_path)
    
    # set the date range: full year of 2024
    start_date = date(2024, 1, 1)
    end_date = date(2024, 12, 31)
    
    base_url = "http://api.weatherstack.com/historical"
    batch_size = 25 
    
    print(f"Starting 2024 weather fetch for {location} ...")
    
    #get all date chunk pairs
    all_chunks = get_date_chunks(start_date, end_date, batch_size)
    
    # loop through the list and fetch data
    for batch in all_chunks:
        # Each batch is a tuple (start date, end date)
        batch_start = batch[0]
        batch_end = batch[1]
        
        # Convert date objects to strings like 2024-01-01
        str_start = batch_start.strftime("%Y-%m-%d")
        str_end = batch_end.strftime("%Y-%m-%d")
        
        print(f"Fetching {str_start} to {str_end} ...")
        
        params = {
            'access_key': access_key,
            'query': location,
            'historical_date_start': str_start,
            'historical_date_end': str_end,
            'hourly': 1,
            'units': 'm'
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data.get('success') is False:
                print(f"API error: {data.get('error')}")
                continue
            
            historical = data.get('historical')
            if not historical:
                print("No historical data in response.")
                continue

            saved_count = save_to_db(db_path, location, historical)
            print(f"Saved {saved_count} days.")
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")

    print("--- Done ---")

if __name__ == "__main__": 
    API_KEY = WEATHERSTACK_API_KEY 
    LOCATION = "New York"
    
    fetch_weather_data(API_KEY, LOCATION)
