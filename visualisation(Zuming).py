import sqlite3
import json
import matplotlib.pyplot as plt
import os

def visualize_weather_impact(db_path):
    # connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # query raw data directly from weather_history
    sql_query = """
        SELECT record_date, full_data_json 
        FROM weather_history 
        WHERE record_date BETWEEN '2025-09-20' AND '2025-12-10'
        ORDER BY record_date ASC
    """
    
    try:
        cursor.execute(sql_query)
        rows = cursor.fetchall()
    except Exception as e:
        print(f"Database error: {e}")
        conn.close()
        return

    conn.close()

    if not rows:
        print("No data found for the specified date range.")
        return

    dates = []
    wind_speeds = []
    severity_scores = []

    # process raw json data
    for row in rows:
        date_str = row[0]
        json_str = row[1]
        
        if not json_str:
            continue
            
        try:
            data = json.loads(json_str)
            hourly_list = data.get('hourly', [])
            
            # calculate max wind speed and total precip for this day
            daily_max_wind = 0
            daily_total_precip = 0.0
            
            for h in hourly_list:
                # get wind speed
                w = int(h.get('wind_speed', 0))
                if w > daily_max_wind:
                    daily_max_wind = w
                
                # get precipitation
                p = float(h.get('precip', 0.0))
                daily_total_precip = daily_total_precip + p
            
            # calculate severity score
            # Formula: (Wind * 0.5) + (Precip * 2.0)
            score = (daily_max_wind * 0.5) + (daily_total_precip * 2.0)
            
            dates.append(date_str)
            wind_speeds.append(daily_max_wind)
            severity_scores.append(score)
            
        except Exception:
            continue

    # create plot
    if not dates:
        print("No valid data to plot.")
        return

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

    # plot wind speed
    ax1.bar(dates, wind_speeds, color='skyblue', edgecolor='black', alpha=0.8)
    ax1.set_title('Daily Max Wind Speed')
    ax1.set_ylabel('Wind Speed (km/h)')
    ax1.grid(axis='y', linestyle='--', alpha=0.5)
    
    # hide x labels for the top plot
    ax1.set_xticklabels([]) 

    # plot severity index
    ax2.plot(dates, severity_scores, color='red', marker='o', linestyle='-', linewidth=2, markersize=4)
    ax2.fill_between(dates, severity_scores, color='red', alpha=0.2)
    ax2.set_title('Weather Severity Index')
    ax2.set_xlabel('Date')
    ax2.set_ylabel('Severity Score')
    ax2.grid(True, linestyle='--', alpha=0.5)

    # adjust layout
    plt.sca(ax2)
    plt.xticks(rotation=45)
    
    plt.tight_layout()

    # save figure
    output_filename = 'weather_severity_analysis.png'
    plt.savefig(output_filename)
    print(f"Chart saved to {output_filename}")

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    DB_FILE = os.path.join(base_dir, "weather_data.db")
    
    visualize_weather_impact(DB_FILE)