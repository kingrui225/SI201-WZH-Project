import sqlite3
import json
from collections import Counter

def process_weather_data(db_path):
    """
    Read the raw data, aggregate it by day, and store it in a new table.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # create Daily Cleaned Table (1 row = 1 day)
    cursor.execute("DROP TABLE IF EXISTS daily_weather_summary")
    cursor.execute('''
        CREATE TABLE daily_weather_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location TEXT,
            date TEXT,
            
            -- temperatures
            avg_temp REAL,
            min_temp REAL,
            max_temp REAL,
            
            -- precipitation & humidity from Hourly)
            total_precip_mm REAL,  -- Total daily rainfall
            total_snow_cm REAL,    -- Total daily snowfall
            avg_humidity INTEGER,  -- Average daily humidity
            
            -- Wind
            max_wind_speed INTEGER, -- Maximum wind speed for the day
            
            -- Weather description
            weather_desc TEXT,
            
            -- astronomy
            sunrise TEXT,
            sunset TEXT,
            moon_phase TEXT,
            uv_index INTEGER
        )
    ''')
    
    # read the raw JSON data
    try:
        cursor.execute("SELECT location, full_data_json FROM weather_history")
        raw_rows = cursor.fetchall()
    except sqlite3.OperationalError:
        print("can't find 'weather_history'")
        return

    cleaned_rows = []

    # iterate and clean
    for location, json_str in raw_rows:
        if not json_str:
            continue
            
        try:
            data = json.loads(json_str)
            
            date_str = data.get('date')
            astro = data.get('astro', {})
            
            hourly_list = data.get('hourly', [])

            precip_values = []
            humidity_values = []
            wind_values = []
            desc_values = []
            
            for h in hourly_list:
                # values calculation
                precip_values.append(float(h.get('precip', 0)))
                humidity_values.append(int(h.get('humidity', 0)))
                wind_values.append(int(h.get('wind_speed', 0)))
                
                # descriptions
                descs = h.get('weather_descriptions', [])
                if descs:
                    desc_values.append(descs[0])

            # calc daily statistics
            total_precip = sum(precip_values)
            avg_humidity = round(sum(humidity_values) / len(humidity_values)) if humidity_values else 0
            max_wind = max(wind_values) if wind_values else 0
            most_common_desc = "Unknown"
            if desc_values:
                most_common_desc = Counter(desc_values).most_common(1)[0][0]

            # build data row
            row = (
                location,
                date_str,
                data.get('avgtemp'),
                data.get('mintemp'),
                data.get('maxtemp'),
                total_precip,
                data.get('totalsnow'),
                avg_humidity,
                max_wind,
                most_common_desc,
                astro.get('sunrise'),
                astro.get('sunset'),
                astro.get('moon_phase'),
                data.get('uv_index')
            )
            cleaned_rows.append(row)
            
        except json.JSONDecodeError:
            print(f"JSON analysis error: {location}")
            continue

    # save to database
    if cleaned_rows:
        cursor.executemany('''
            INSERT INTO daily_weather_summary (
                location, date, 
                avg_temp, min_temp, max_temp, 
                total_precip_mm, total_snow_cm, avg_humidity, 
                max_wind_speed, weather_desc, 
                sunrise, sunset, moon_phase, uv_index
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', cleaned_rows)
        
        conn.commit()
        print(f"saved {len(cleaned_rows)} rows into 'daily_weather_summary'")
    else:
        print("!!!no data generated!!!")

    conn.close()

if __name__ == "__main__":
    DB_FILE = "weather_data.db"
    process_weather_data(DB_FILE)
