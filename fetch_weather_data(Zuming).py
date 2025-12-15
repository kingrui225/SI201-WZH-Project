import requests
import sqlite3
import json
from datetime import date, timedelta, datetime
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

def save_to_db(db_path, location, weather_data):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    count = 0
    for date_str in weather_data:
        details = weather_data[date_str]
        avg_temp = details.get('avgtemp')
        min_temp = details.get('mintemp')
        max_temp = details.get('maxtemp')
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
    create_db_table(db_path)
    
    final_target_date = date(2025, 12, 12)
    batch_size = 25

    # check the latest date in the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(record_date) FROM weather_history WHERE location = ?", (location,))
    last_date_str = cursor.fetchone()[0]
    conn.close()

    # determine the start date for this run
    if last_date_str:
        last_date_obj = datetime.strptime(last_date_str, "%Y-%m-%d").date()
        start_date = last_date_obj + timedelta(days=1)
    else:
        start_date = date(2025, 1, 1)

    # check if we are already done
    if start_date > final_target_date:
        print(f"--- All tasks completed! Database updated to {final_target_date} ---")
        return

    # determine the end date for this run
    end_date = start_date + timedelta(days=batch_size - 1)
    if end_date > final_target_date:
        end_date = final_target_date

    str_start = start_date.strftime("%Y-%m-%d")
    str_end = end_date.strftime("%Y-%m-%d")

    # API request
    print(f"--- Fetching range: {str_start} to {str_end} ---")
    
    base_url = "http://api.weatherstack.com/historical"
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
            return
        
        historical = data.get('historical')
        if historical:
            saved_count = save_to_db(db_path, location, historical)
            
            # progress Report
            remaining_days = (final_target_date - end_date).days
            if remaining_days < 0: remaining_days = 0
            
            print(f"Successfully saved {saved_count} days of data.")
            print(f"Remaining days to target {final_target_date}: {remaining_days} days.")
        else:
            print("No historical data returned.")
            
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

if __name__ == "__main__": 
    API_KEY = WEATHERSTACK_API_KEY 
    LOCATION = "New York"
    fetch_weather_data(API_KEY, LOCATION)