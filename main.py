# main programm
import sqlite3
from pathlib import Path
import sys

FINAL_DB = "wzh_project.db"
SOURCE_DBS = ["flight_data.db", "weather_data.db", "stock_data.db"]

def table_list(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
    """)
    return [r[0] for r in cur.fetchall()]

def table_exists(conn, name):
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM sqlite_master
        WHERE type='table' AND name=?
    """, (name,))
    return cur.fetchone() is not None

def merge_one(source_db, final_db):
    if not Path(source_db).exists():
        print(f"Skip (not found): {source_db}")
        return

    src = sqlite3.connect(source_db)
    dst = sqlite3.connect(final_db)

    src_tables = table_list(src)
    print(f"[{source_db}] tables: {src_tables}")

    for t in src_tables:
        if table_exists(dst, t):
            print(f"  - Skip (already exists in final): {t}")
            continue

        cur = src.cursor()
        cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (t,))
        row = cur.fetchone()
        if not row or not row[0]:
            print(f"  ! Skip (no CREATE sql): {t}")
            continue

        create_sql = row[0]
        dst.execute(create_sql)

        rows = src.execute(f"SELECT * FROM {t}").fetchall()
        if rows:
            placeholders = ",".join(["?"] * len(rows[0]))
            dst.executemany(f"INSERT INTO {t} VALUES ({placeholders})", rows)

        print(f"  + Copied table: {t} (rows={len(rows)})")

    dst.commit()
    src.close()
    dst.close()

def merge_databases():
    sqlite3.connect(FINAL_DB).close()
    for db in SOURCE_DBS:
        merge_one(db, FINAL_DB)
    print(f"Done. Final DB: {FINAL_DB}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "merge":
        merge_databases()
    else:
        print("Usage:")
        print("  python3 main.py merge")
