"""Test script to verify text file output works"""
import sqlite3
import sys
sys.path.insert(0, '.')

# Import from the file with parentheses
import importlib.util
spec = importlib.util.spec_from_file_location("process_stock", "process_stock_data(Ronghao).py")
process_stock = importlib.util.module_from_spec(spec)
spec.loader.exec_module(process_stock)

# Test with the old database that has data
db = 'stock_data.db'
conn = sqlite3.connect(db)
cursor = conn.cursor()

# Check if it has data
cursor.execute('SELECT COUNT(*) FROM stock_history')
count = cursor.fetchone()[0]
print(f'Records in stock_data.db: {count}')

if count > 0:
    # Run the comparison
    data = process_stock.compare_airlines_under_weather(conn)
    
    if data:
        print(f'Analysis generated for {len(data)} airlines!')
        # Write to text file
        process_stock.write_results_to_file(data, db, 'stock_analysis_results.txt')
        
        # Show the file contents
        print('\n--- TEXT FILE CONTENTS ---')
        with open('stock_analysis_results.txt', 'r') as f:
            print(f.read())
else:
    print('No stock data found!')

conn.close()

