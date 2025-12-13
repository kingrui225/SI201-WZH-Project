import sqlite3


DB_PATH = "flight_data.db"


def process_flight_data(db_path='flight_data.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS flights_daily")
    cursor.execute('''
        CREATE TABLE flights_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            airport_code TEXT,
            total_flights INTEGER,
            delayed_flights INTEGER,
            cancelled_flights INTEGER,
            avg_delay_minutes REAL
        )
    ''')

    cursor.execute('''
        SELECT airport_code, record_date, total_flights, 
               delayed_flights, cancelled_flights, avg_delay_minutes
        FROM flight_history
    ''')
    rows = cursor.fetchall()

    cleaned_rows = []
    for airport_code, record_date, total_flights, delayed_flights, cancelled_flights, avg_delay in rows:
        cleaned_rows.append((
            record_date,
            airport_code,
            total_flights,
            delayed_flights,
            cancelled_flights,
            avg_delay
        ))

    cursor.executemany('''
        INSERT INTO flights_daily (
            date, airport_code, total_flights, delayed_flights,
            cancelled_flights, avg_delay_minutes
        ) VALUES (?, ?, ?, ?, ?, ?)
    ''', cleaned_rows)

    conn.commit()
    conn.close()
    print(f"Saved {len(cleaned_rows)} rows into flights_daily")

if __name__ == "__main__":
    process_flight_data()
    