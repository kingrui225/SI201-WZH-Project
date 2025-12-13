import sqlite3
import matplotlib.pyplot as plt

DB_PATH = "flight_data.db"
def table_exists(conn, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name=?
    """, (table_name,))
    return cur.fetchone() is not None


def plot_wind_speed_vs_avg_delay(db_path=DB_PATH, output_file="wind_vs_delay.png"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    if not table_exists(conn, "flight_history"):
        print("Missing table: flight_history. Run fetch_flight_data first.")
        conn.close()
        return

    if not table_exists(conn, "daily_weather_summary"):
        print("No weather table found (daily_weather_summary).")
        print("You can still run this script; once weather data is added, it will generate the scatter plot.")
        conn.close()
        return

    query = """
        SELECT
            f.record_date AS date,
            AVG(CASE
                    WHEN f.dep_delay_min IS NULL THEN NULL
                    WHEN f.dep_delay_min < 0 THEN NULL
                    ELSE f.dep_delay_min
                END) AS avg_delay,
            w.max_wind_speed AS wind_speed
        FROM flight_history f
        JOIN daily_weather_summary w
            ON f.record_date = w.date
        WHERE f.dep_delay_min IS NOT NULL
          AND w.max_wind_speed IS NOT NULL
        GROUP BY f.record_date, w.max_wind_speed
    """

    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No joined data returned. Check that dates overlap and weather columns exist.")
        return

    wind = [r[2] for r in rows]
    avg_delay = [r[1] for r in rows]

    plt.figure(figsize=(6, 4))
    plt.scatter(wind, avg_delay, s=50)
    plt.xlabel("Max Wind Speed")
    plt.ylabel("Average Departure Delay (min)")
    plt.title("Wind Speed vs Average Flight Delay")
    plt.tight_layout()
    plt.savefig(output_file, dpi=150)
    plt.close()

    print(f"Saved chart: {output_file} (points: {len(rows)})")

def plot_avg_delay_by_date_bar(db_path=DB_PATH, output_file="avg_delay_by_date_bar.png", limit_days=30):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    if not table_exists(conn, "flight_history"):
        print("Missing table: flight_history.")
        conn.close()
        return

    query = """
        SELECT
            record_date,
            AVG(CASE
                    WHEN dep_delay_min IS NULL THEN NULL
                    WHEN dep_delay_min < 0 THEN NULL
                    ELSE dep_delay_min
                END) AS avg_delay
        FROM flight_history
        WHERE record_date IS NOT NULL
        GROUP BY record_date
        ORDER BY record_date ASC
    """

    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No flight-only data returned.")
        return


    dates = [r[0][5:] for r in rows]
    avg_delay = [r[1] if r[1] is not None else 0 for r in rows]

    plt.figure(figsize=(10, 4))
    plt.bar(dates, avg_delay)
    plt.xlabel("Date")
    plt.ylabel("Average Departure Delay (min)")
    plt.title("Flight-only: Average Departure Delay by Date")
    plt.xticks(rotation=60, ha="right")
    plt.tight_layout()
    plt.savefig(output_file, dpi=150)
    plt.close()

    print(f"Saved chart: {output_file} (days shown: {len(dates)})")

if __name__ == "__main__":
    # Flight-only bar chart
    plot_avg_delay_by_date_bar()

    # Weather vs Flight scatter
    plot_wind_speed_vs_avg_delay()