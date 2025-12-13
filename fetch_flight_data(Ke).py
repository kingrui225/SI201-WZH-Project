import requests
import sqlite3
import json
from datetime import date, timedelta
from config import AVIATIONSTACK_API_KEY
import time


def create_db_table(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS flight_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            airport_code TEXT NOT NULL,
            record_date TEXT NOT NULL,
            flight_iata TEXT,
            airline_name TEXT,
            flight_status TEXT,
            dep_delay_min INTEGER,
            dep_scheduled TEXT,
            dep_estimated TEXT,
            dep_actual TEXT,
            arr_iata TEXT,
            full_data_json TEXT,
            UNIQUE(airport_code, record_date, flight_iata)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS flight_fetch_progress (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            next_date TEXT NOT NULL,
            next_offset INTEGER NOT NULL
        )
    ''')
    cursor.execute('''
        INSERT OR IGNORE INTO flight_fetch_progress (id, next_date, next_offset)
        VALUES (1, '2025-09-20', 0)
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


def fetch_raw_flights_for_date(access_key, airport_code, record_date, offset=0, limit=25):
    if not access_key:
        print("Error: Missing AVIATIONSTACK_API_KEY")
        return []

    base_url = "https://api.aviationstack.com/v1/flights"
    date_str = record_date.strftime("%Y-%m-%d")

    params = {
        "access_key": access_key,
        "dep_iata": airport_code,
        "flight_date": date_str,
        "limit": limit,
        "offset": offset
    }

    try:
        resp = requests.get(base_url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        if isinstance(data, dict) and data.get("error"):
            print(f"API error on {date_str}: {data.get('error')}")
            return []

        flights = data.get("data", []) if isinstance(data, dict) else []
        print(f"[DEBUG] {date_str} offset={offset} pulled={len(flights)}")

        return flights

    except requests.exceptions.RequestException as e:
        print(f"Request failed on {date_str} offset={offset}: {e}")
        return []
    except ValueError:
        print(f"JSON decode failed on {date_str} offset={offset}. Raw text: {resp.text[:200] if 'resp' in locals() else ''}")
        return []




def save_to_db(db_path, airport_code, record_date, flights):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    date_str = record_date.strftime("%Y-%m-%d")
    rows = []
    for item in flights:
        flight_iata = (item.get("flight") or {}).get("iata")
        airline_name = (item.get("airline") or {}).get("name")
        status = item.get("flight_status")

        dep = item.get("departure") or {}
        arr = item.get("arrival") or {}

        rows.append((
            airport_code,
            date_str,
            flight_iata,
            airline_name,
            status,
            dep.get("delay"),
            dep.get("scheduled"),
            dep.get("estimated"),
            dep.get("actual"),
            arr.get("iata"),
            json.dumps(item)
        ))

    cursor.executemany('''
        INSERT OR IGNORE INTO flight_history
        (airport_code, record_date, flight_iata, airline_name, flight_status,
         dep_delay_min, dep_scheduled, dep_estimated, dep_actual, arr_iata, full_data_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', rows)

    inserted = cursor.rowcount
    conn.commit()
    conn.close()
    print(f"Inserted {inserted} new flights for {date_str} (pulled {len(flights)})")
    return inserted

def fetch_flight_data(access_key, airport_code, db_path='flight_data.db', items_per_run=25):
    create_db_table(db_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT next_date, next_offset FROM flight_fetch_progress WHERE id=1")
    next_date_str, next_offset = cursor.fetchone()
    conn.close()

    current_date = date.fromisoformat(next_date_str)
    offset = next_offset

    end_date = date(2025, 12, 10)

    print(f"Starting from {current_date.isoformat()}, offset={offset}, max={items_per_run} items...")

    max_date_rolls = 7
    rolls = 0

    while rolls <= max_date_rolls:
        if current_date > end_date:
            print(f"Reached end date {end_date.isoformat()}. Stop fetching.")
            return

        flights = fetch_raw_flights_for_date(
            access_key, airport_code, current_date, offset=offset, limit=items_per_run
        )

        if flights:
            inserted = save_to_db(db_path, airport_code, current_date, flights)

            next_day = current_date + timedelta(days=1)
            offset = 0

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE flight_fetch_progress SET next_date=?, next_offset=? WHERE id=1",
                (next_day.isoformat(), offset)
            )
            conn.commit()
            conn.close()

            print(f"Run done. Inserted {inserted}. Next: {next_day.isoformat()} offset=0. Re-run to continue.")
            return

        print(f"No flights for {current_date.isoformat()} at offset={offset}. Move to next day.")
        current_date += timedelta(days=1)
        offset = 0
        rolls += 1

    print("No flights returned after several date rollovers.")

if __name__ == "__main__":
    fetch_flight_data(AVIATIONSTACK_API_KEY, "JFK")