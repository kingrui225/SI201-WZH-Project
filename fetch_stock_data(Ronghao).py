"""
Fetch Stock Data - Ronghao Wang
SI 201 Final Project

GRADING REQUIREMENTS MET:
1. 100+ rows: Run this script multiple times (each run saves max 25 items)
2. Two tables with INTEGER key: airlines (id) -> stock_history (airline_id)
3. No duplicate strings: Airline names stored in airlines table, referenced by ID
4. 25 items per execution: Controlled by items_per_run parameter
5. SQLite database: stock_data.db
"""

import requests
import sqlite3
import json
from datetime import date, timedelta
from config import MARKETSTACK_API_KEY

# Database file
DATABASE_NAME = "stock_data.db"

# Airlines with JFK airport presence
AIRLINES = [
    {'symbol': 'JBLU', 'name': 'JetBlue Airways'},
    {'symbol': 'DAL', 'name': 'Delta Air Lines'},
    {'symbol': 'AAL', 'name': 'American Airlines'},
    {'symbol': 'UAL', 'name': 'United Airlines'}
]


def create_tables(db_path):
    """
    Create two tables that share an INTEGER KEY.
    
    airlines: id (INTEGER PRIMARY KEY), symbol, name
    stock_history: airline_id (INTEGER FOREIGN KEY) -> airlines.id
    
    This satisfies: "two tables that share an integer key"
    and "no duplicate string data" (airline names only stored once)
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # TABLE 1: airlines (stores airline info ONCE - no duplicate strings)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS airlines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL
        )
    ''')
    
    # Insert airlines if not exists
    for airline in AIRLINES:
        cursor.execute('''
            INSERT OR IGNORE INTO airlines (symbol, name) 
            VALUES (?, ?)
        ''', (airline['symbol'], airline['name']))
    
    # TABLE 2: stock_history (references airlines via INTEGER KEY)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            airline_id INTEGER NOT NULL,
            record_date TEXT NOT NULL,
            open_price REAL,
            close_price REAL,
            high_price REAL,
            low_price REAL,
            volume INTEGER,
            return_percentage REAL,
            price_range REAL,
            UNIQUE(airline_id, record_date),
            FOREIGN KEY (airline_id) REFERENCES airlines(id)
        )
    ''')
    
    # Progress tracker (to resume fetching)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fetch_progress (
            id INTEGER PRIMARY KEY,
            last_fetch_date TEXT,
            total_records INTEGER DEFAULT 0
        )
    ''')
    
    cursor.execute('''
        INSERT OR IGNORE INTO fetch_progress (id, last_fetch_date, total_records)
        VALUES (1, '2024-01-01', 0)
    ''')
    
    conn.commit()
    conn.close()


def get_airline_id(cursor, symbol):
    """Get airline_id (INTEGER) for a symbol - avoids duplicate strings."""
    cursor.execute('SELECT id FROM airlines WHERE symbol = ?', (symbol,))
    result = cursor.fetchone()
    return result[0] if result else None


def fetch_stock_data(access_key, db_path=DATABASE_NAME, items_per_run=25):
    """
    Fetch stock data - saves MAX 25 ITEMS per execution.
    
    Run this multiple times to get 100+ records.
    """
    create_tables(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get progress
    cursor.execute('SELECT last_fetch_date, total_records FROM fetch_progress WHERE id = 1')
    result = cursor.fetchone()
    last_date_str, total_records = result if result else ('2024-01-01', 0)
    
    print("=" * 60)
    print("STOCK DATA FETCH - Ronghao Wang")
    print("=" * 60)
    print(f"Database: {db_path}")
    print(f"Max items this run: {items_per_run}")
    print(f"Starting from: {last_date_str}")
    print(f"Total records so far: {total_records}")
    print("=" * 60)
    
    # Parse date
    year, month, day = map(int, last_date_str.split('-'))
    current_date = date(year, month, day)
    end_date = date(2024, 12, 31)
    
    if current_date > end_date:
        print("\n✅ All 2024 data has been fetched!")
        conn.close()
        return
    
    base_url = "http://api.marketstack.com/v1/eod"
    symbols_str = ','.join([a['symbol'] for a in AIRLINES])
    
    items_saved = 0
    
    while current_date <= end_date and items_saved < items_per_run:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"\nFetching {date_str}...")
        
        params = {
            'access_key': access_key,
            'symbols': symbols_str,
            'date_from': date_str,
            'date_to': date_str,
            'limit': 100
        }
        
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if 'error' in data:
                print(f"  API error: {data.get('error')}")
                current_date += timedelta(days=1)
                continue
            
            records = data.get('data', [])
            
            if not records:
                print(f"  No data for {date_str}")
                current_date += timedelta(days=1)
                continue
            
            for record in records:
                if items_saved >= items_per_run:
                    break
                
                symbol = record.get('symbol')
                airline_id = get_airline_id(cursor, symbol)
                
                if not airline_id:
                    continue
                
                rec_date = record.get('date', '')[:10]
                open_p = record.get('open')
                close_p = record.get('close')
                high_p = record.get('high')
                low_p = record.get('low')
                volume = record.get('volume')
                
                # Calculate metrics
                ret_pct = round(((close_p - open_p) / open_p) * 100, 4) if open_p and close_p else None
                price_rng = round(high_p - low_p, 4) if high_p and low_p else None
                
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO stock_history 
                        (airline_id, record_date, open_price, close_price, 
                         high_price, low_price, volume, return_percentage, price_range)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (airline_id, rec_date, open_p, close_p, high_p, low_p, 
                          volume, ret_pct, price_rng))
                    
                    if cursor.rowcount > 0:
                        items_saved += 1
                        total_records += 1
                        print(f"  ✓ {symbol} {rec_date} ({items_saved}/{items_per_run})")
                except Exception as e:
                    print(f"  Error: {e}")
            
        except requests.exceptions.RequestException as e:
            print(f"  Request failed: {e}")
            break
        
        current_date += timedelta(days=1)
    
    # Update progress
    cursor.execute('UPDATE fetch_progress SET last_fetch_date = ?, total_records = ? WHERE id = 1',
                   (current_date.strftime("%Y-%m-%d"), total_records))
    
    conn.commit()
    conn.close()
    
    print("\n" + "=" * 60)
    print(f"Items saved this run: {items_saved}")
    print(f"Total records: {total_records}")
    
    if total_records >= 100:
        print("✅ 100+ records requirement MET!")
    else:
        print(f"⚠️ Need {100 - total_records} more. Run this script again!")
    print("=" * 60)


if __name__ == "__main__":
    fetch_stock_data(MARKETSTACK_API_KEY)
