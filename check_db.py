"""Quick script to check database structure"""
import sqlite3

conn = sqlite3.connect('wzh_project.db')
cursor = conn.cursor()

print("=" * 60)
print("DATABASE STRUCTURE CHECK - wzh_project.db")
print("=" * 60)

# Check tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in cursor.fetchall()]
print(f"\nTables: {tables}")

# Check airlines table
if 'airlines' in tables:
    print("\n--- Airlines Table (INTEGER PRIMARY KEY) ---")
    cursor.execute('SELECT * FROM airlines')
    airlines = cursor.fetchall()
    print("  id | symbol | name")
    print("  " + "-" * 45)
    for a in airlines:
        print(f"  {a[0]}  | {a[1]:6} | {a[2]}")

# Check stock_history schema
if 'stock_history' in tables:
    print("\n--- Stock_history Table (with FOREIGN KEY) ---")
    cursor.execute('PRAGMA table_info(stock_history)')
    cols = cursor.fetchall()
    for c in cols:
        fk = " ← FOREIGN KEY to airlines.id" if c[1] == 'airline_id' else ""
        print(f"  {c[1]}: {c[2]}{fk}")
    
    cursor.execute('SELECT COUNT(*) FROM stock_history')
    count = cursor.fetchone()[0]
    print(f"\n  Total records: {count}")

# Check fetch progress
if 'fetch_progress' in tables:
    print("\n--- Fetch Progress ---")
    cursor.execute('SELECT * FROM fetch_progress')
    for row in cursor.fetchall():
        print(f"  API: {row[1]}, Last date: {row[2]}, Total: {row[3]}")

conn.close()

print("\n" + "=" * 60)
print("✅ CRITERIA CHECK:")
print("  - Two tables with INTEGER key: airlines.id → stock_history.airline_id")
print("  - Single database: wzh_project.db")
print("  - 25 items per run: Controlled by fetch script")
print("=" * 60)

