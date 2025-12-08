# Scratch file for Zuming to quickly try code snippets

import requests
import sqlite3
import json
import time
from datetime import date, timedelta

def create_db_table(db_path):
    """Create the database table for storing weather data"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table with base fields and raw JSON for full details
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
    """Generate date chunks, each containing chunk_size days"""
    current_start = start_date
    while current_start <= end_date:
        current_end = current_start + timedelta(days=chunk_size - 1)
        # Ensure the final chunk does not exceed the end date
        if current_end > end_date:
            current_end = end_date
        yield current_start, current_end
        current_start = current_end + timedelta(days=1)

def save_to_db(db_path, location, weather_data):
    """Bulk save the data to the database"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    records_to_insert = []
    
    # weather_data is the 'historical' dict returned by the API, keyed by date
    for date_str, details in weather_data.items():
        records_to_insert.append((
            location,
            date_str,
            details.get('avgtemp'),
            details.get('mintemp'),
            details.get('maxtemp'),
            json.dumps(details) # Save full data (including hourly) as a JSON string
        ))
    
    # Use INSERT OR REPLACE to avoid duplicate-date errors
    cursor.executemany('''
        INSERT OR REPLACE INTO weather_history 
        (location, record_date, avg_temp, min_temp, max_temp, full_data_json)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', records_to_insert)
    
    conn.commit()
    count = cursor.rowcount
    conn.close()
    return count

def fetch_weather_data(access_key, location, db_path='weather_data.db'):
    """
    Fetch and save the full year of 2024 weather data.
    Each request pulls 25 days of data.
    """
    # 1. Initialize the database
    create_db_table(db_path)
    
    # 2. Set the time range: full year of 2024
    start_date = date(2024, 1, 1)
    end_date = date(2024, 12, 31)
    
    base_url = "http://api.weatherstack.com/historical"
    batch_size = 25 # Fetch 25 days per request
    
    print(f"--- 开始获取 {location} 2024年天气数据 ---")
    
    # 3. Loop to fetch data
    for batch_start, batch_end in get_date_chunks(start_date, end_date, batch_size):
        str_start = batch_start.strftime("%Y-%m-%d")
        str_end = batch_end.strftime("%Y-%m-%d")
        
        print(f"正在获取: {str_start} 到 {str_end} ...")
        
        params = {
            'access_key': access_key,
            'query': location,
            'historical_date_start': str_start,
            'historical_date_end': str_end,
            'hourly': 1,  # Fetch full hourly data
            'units': 'm'  # Use metric units
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Check if the API returned an error
            if 'success' in data and data['success'] is False:
                error_info = data.get('error', {})
                print(f"API 错误: {error_info.get('code')} - {error_info.get('type')}")
                break
            
            if 'historical' in data:
                saved_count = save_to_db(db_path, location, data['historical'])
                print(f"  -> 成功保存 {saved_count} 条记录到数据库。")
            else:
                print("  -> 警告: 返回数据中未找到 'historical' 字段。")
                
        except requests.exceptions.RequestException as e:
            print(f"  -> 请求失败: {e}")

    print("--- 任务完成 ---")

# --- Usage example ---
if __name__ == "__main__":
    # Please replace with your own Access Key
    MY_ACCESS_KEY = "1c71ada038989e8ac8968fa83d282c30" 
    LOCATION = "New York"
    
    fetch_weather_data(MY_ACCESS_KEY, LOCATION)
