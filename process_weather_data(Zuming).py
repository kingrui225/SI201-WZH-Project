import sqlite3
import json
import os
from datetime import datetime

def process_weather_data(db_path):
    # connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # get raw data
    try:
        cursor.execute("SELECT record_date, full_data_json FROM weather_history ORDER BY record_date ASC")
        rows = cursor.fetchall()
    except Exception:
        conn.close()
        return

    # dictionaries to store data
    weekly_wind_speeds = {}
    weekly_dates = {}

    # process rows
    for row in rows:
        date_str = row[0]
        json_str = row[1]
        
        if not json_str:
            continue
            
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            week_key = date_obj.strftime("%Y-Week%U")
            
            # extract wind speeds
            data = json.loads(json_str)
            hourly_list = data.get('hourly', [])
            
            for h in hourly_list:
                wind_speed = int(h.get('wind_speed', 0))
                
                if week_key in weekly_wind_speeds:
                    weekly_wind_speeds[week_key].append(wind_speed)
                else:
                    weekly_wind_speeds[week_key] = [wind_speed]

            # record the date for this week
            if week_key in weekly_dates:
                weekly_dates[week_key].append(date_str)
            else:
                weekly_dates[week_key] = [date_str]
                    
        except Exception:
            continue

    # write to text file
    output_filename = "weekly_avg_wind_speed.txt"
    
    with open(output_filename, "w") as f:
        # header
        f.write(f"{'Week Range':<50} | {'Avg Wind Speed (km/h)':<20}\n")
        f.write("-" * 75 + "\n")
        
        sorted_weeks = sorted(weekly_wind_speeds.keys())
        
        for week in sorted_weeks:
            speeds = weekly_wind_speeds[week]
            dates = weekly_dates[week]
            
            if len(speeds) > 0:
                average_speed = sum(speeds) / len(speeds)
                
                # find start and end date for this week
                dates.sort()
                start_date = dates[0]
                end_date = dates[-1]
                
                week_label = f"{week} ({start_date} to {end_date})"
                
                f.write(f"{week_label:<50} | {average_speed:.2f}\n")

    conn.close()
    print(f"Done. Results saved to {output_filename}")

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    DB_FILE = os.path.join(base_dir, "weather_data.db")
    
    process_weather_data(DB_FILE)