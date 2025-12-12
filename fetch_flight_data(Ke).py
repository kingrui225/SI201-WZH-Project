import requests
import sqlite3
import json
from datetime import date, timedelta
from config import AVIATIONSTACK_API_KEY

def create_db_table(db_path):

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS flight_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            airport_code TEXT,
            record_date TEXT,
            total_flights INTEGER,
            delayed_flights INTEGER,
            cancelled_flights INTEGER,
            avg_delay_minutes REAL,
            full_data_json TEXT,
            UNIQUE(airport_code, record_date)
        )
    ''')
    conn.commit()
    conn.close()


def get_date_list(start_date, end_date):

    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def fetch_raw_flights_for_date(access_key, airport_code, record_date):

    if not access_key:
        print("Error: Missing AVIATIONSTACK_KEY in config or .env")
        return []

    base_url = "http://api.aviationstack.com/v1/flights"


    params = {
        "access_key": access_key,
        "dep_iata": airport_code,
        "limit": 100
    }

    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        flights = data.get("data", [])
        return flights

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return []

def save_to_db(db_path, airport_code, record_date, flights):

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    date_str = record_date.strftime("%Y-%m-%d")

    total_flights = len(flights)
    delayed_flights = 0
    cancelled_flights = 0
    delays = []

    for item in flights:
        status = item.get('flight_status')
        
        if status == 'cancelled':
            cancelled_flights += 1
        
        departure = item.get('departure', {})
        delay_min = departure.get('delay')
        if delay_min is not None:
            delays.append(delay_min)
            delayed_flights += 1

    avg_delay_minutes = sum(delays) / len(delays) if delays else None

    full_json_str = json.dumps(flights)

    try:
        cursor.execute('''
            INSERT OR REPLACE INTO flight_history
            (airport_code, record_date, total_flights, delayed_flights,
             cancelled_flights, avg_delay_minutes, full_data_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (airport_code, date_str, total_flights, delayed_flights,
              cancelled_flights, avg_delay_minutes, full_json_str))
    except Exception as e:
        print(f"Error saving flights for {date_str}: {e}")
    else:
        print(f"Saved {total_flights} flights for {date_str} "
              f"(delayed: {delayed_flights}, cancelled: {cancelled_flights})")

    conn.commit()
    conn.close()


def fetch_flight_data(access_key, airport_code, db_path='flight_data.db'):

    create_db_table(db_path)

    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 21)

    all_dates = get_date_list(start_date, end_date)

    print(f"Starting 2024 flight fetch for {airport_code} ...")

    for d in all_dates:
        print(f"Fetching flights for {airport_code} on {d.strftime('%Y-%m-%d')} ...")
        flights = fetch_raw_flights_for_date(access_key, airport_code, d)
        if not flights:
            print("No flights returned or error occurred.")
            continue
        save_to_db(db_path, airport_code, d, flights)

    print("--- Done ---")


if __name__ == "__main__":
    API_KEY = AVIATIONSTACK_API_KEY
    AIRPORT = "JFK"

    fetch_flight_data(API_KEY, AIRPORT)
