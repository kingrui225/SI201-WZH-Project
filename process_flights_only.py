import sqlite3
from collections import defaultdict

DB_PATH = "flight_data.db"
OUTPUT_FILE = "flight_delay_daily_results.txt"


def table_exists(conn, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name=?
    """, (table_name,))
    return cur.fetchone() is not None


def calculate_daily_flight_stats(db_path=DB_PATH, output_file=OUTPUT_FILE, limit_days=9999999):
    """
    Uses ONLY flight_history in flight_data.db.
    Selects data from SQLite, calculates:
      - number of flights per day
      - average departure delay per day (ignoring NULL and negative delays)
    Writes a clear text output file.

    Returns:
        list of tuples: (date, flight_count, avg_delay_min)
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    if not table_exists(conn, "flight_history"):
        conn.close()
        raise RuntimeError("Missing table: flight_history. Run fetch_flight_data first.")

    query = """
        SELECT record_date, dep_delay_min
        FROM flight_history
        WHERE record_date IS NOT NULL
    """
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

    counts = defaultdict(int)
    delay_sum = defaultdict(float)
    delay_n = defaultdict(int)

    for d, delay in rows:
        counts[d] += 1
        if delay is None:
            continue
        if isinstance(delay, (int, float)) and delay >= 0:
            delay_sum[d] += float(delay)
            delay_n[d] += 1

    dates_sorted = sorted(counts.keys())
    results = []
    for d in dates_sorted:
        avg = None
        if delay_n[d] > 0:
            avg = delay_sum[d] / delay_n[d]
        results.append((d, counts[d], avg))

    with open(output_file, "w", encoding="utf-8") as f:
        f.write("Flight Daily Stats (Flight-only)\n")
        f.write(f"Database: {db_path}\n")
        f.write("Source table: flight_history\n")
        f.write("Calculations:\n")
        f.write("  - flight_count per day\n")
        f.write("  - avg_delay_min per day (ignores NULL and negative delays)\n\n")

        f.write("Columns:\n")
        f.write("date\tflight_count\tavg_delay_min\n")
        f.write("-" * 50 + "\n")

        for d, cnt, avg in results[:limit_days]:
            avg_str = "NA" if avg is None else f"{avg:.2f}"
            f.write(f"{d}\t{cnt}\t{avg_str}\n")

        f.write("\n")
        f.write(f"Total unique days: {len(results)}\n")
        f.write(f"Total flights (rows in flight_history selected): {len(rows)}\n")
        f.write(f"Rows written (limit_days={limit_days}): {min(len(results), limit_days)}\n")

    print(f"Saved calculation file: {output_file}")
    print(f"Total flights selected: {len(rows)}")
    print(f"Total unique days: {len(results)}")
    return results


if __name__ == "__main__":
    calculate_daily_flight_stats()
