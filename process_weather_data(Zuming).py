import sqlite3
import json
import os

def process_weather_data(db_path):
    # connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # create target table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_weather_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location TEXT,
            date TEXT,
            avg_temp REAL,
            min_temp REAL,
            max_temp REAL,
            total_precip_mm REAL,
            total_snow_cm REAL,
            avg_humidity INTEGER,
            max_wind_speed INTEGER,
            weather_desc TEXT,
            sunrise TEXT,
            sunset TEXT,
            moon_phase TEXT,
            uv_index INTEGER,
            UNIQUE(location, date)
        )
    ''')
    
    # get raw data
    try:
        cursor.execute("SELECT location, full_data_json FROM weather_history")
        raw_rows = cursor.fetchall()
    except sqlite3.OperationalError:
        print("Error: can't find 'weather_history' table.")
        conn.close()
        return

    cleaned_rows = []

    # process each row
    for row_data in raw_rows:
        location = row_data[0]
        json_str = row_data[1]
        
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
                p = float(h.get('precip', 0))
                hum = int(h.get('humidity', 0))
                w = int(h.get('wind_speed', 0))
                
                precip_values.append(p)
                humidity_values.append(hum)
                wind_values.append(w)
                
                descs = h.get('weather_descriptions', [])
                if len(descs) > 0:
                    desc_values.append(descs[0])

            # calculate statistics
            total_precip = sum(precip_values)
            
            avg_humidity = 0
            if len(humidity_values) > 0:
                avg_humidity = round(sum(humidity_values) / len(humidity_values))
                
            max_wind = 0
            if len(wind_values) > 0:
                max_wind = max(wind_values)
            
            # find most common weather
            counts = {}
            for desc in desc_values:
                if desc in counts:
                    counts[desc] = counts[desc] + 1
                else:
                    counts[desc] = 1
            
            most_common_desc = "Unknown"
            max_count = 0
            
            for desc in counts:
                if counts[desc] > max_count:
                    max_count = counts[desc]
                    most_common_desc = desc

            new_row = (
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
            cleaned_rows.append(new_row)
            
        except Exception:
            continue

    # save data to database
    count = 0
    if len(cleaned_rows) > 0:
        for row in cleaned_rows:
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO processed_weather_data (
                        location, date, 
                        avg_temp, min_temp, max_temp, 
                        total_precip_mm, total_snow_cm, avg_humidity, 
                        max_wind_speed, weather_desc, 
                        sunrise, sunset, moon_phase, uv_index
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', row)
                count = count + 1
            except Exception as e:
                print(f"Insert error: {e}")
        
        conn.commit()
        print(f"Successfully saved {count} rows into 'processed_weather_data'")
    else:
        print("No data generated.")

    conn.close()

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    DB_FILE = os.path.join(base_dir, "weather_data.db")
    process_weather_data(DB_FILE)