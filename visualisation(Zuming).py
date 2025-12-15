import sqlite3
import matplotlib.pyplot as plt
import os

def visualize_weather_impact(db_path):
    # connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # query data
    sql_query = """
        SELECT date, max_wind_speed, total_precip_mm 
        FROM processed_weather_data 
        WHERE date BETWEEN '2025-09-20' AND '2025-12-10'
        ORDER BY date ASC
    """
    
    try:
        cursor.execute(sql_query)
        rows = cursor.fetchall()
    except:
        conn.close()
        return

    conn.close()

    if not rows:
        return

    dates = []
    wind_speeds = []
    severity_scores = []

    # process data
    for row in rows:
        date_val = row[0]
        wind_val = row[1] if row[1] else 0
        precip_val = row[2] if row[2] else 0
        
        score = (wind_val * 0.5) + (precip_val * 2.0)
        
        dates.append(date_val)
        wind_speeds.append(wind_val)
        severity_scores.append(score)

    # create plot
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

    # plot wind speed
    ax1.bar(dates, wind_speeds, color='skyblue', edgecolor='black', alpha=0.8)
    ax1.set_title('Daily Max Wind Speed')
    ax1.set_ylabel('Wind Speed (km/h)')
    ax1.grid(axis='y', linestyle='--', alpha=0.5)
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

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    DB_FILE = os.path.join(base_dir, "weather_data.db")
    
    visualize_weather_impact(DB_FILE)