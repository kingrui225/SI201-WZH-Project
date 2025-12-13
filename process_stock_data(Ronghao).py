import sqlite3
import json
import matplotlib.pyplot as plt
import os


def clean_stock_data(raw_stock_json):
    """
    Clean and transform raw Marketstack JSON data.
    
    BY: RONGHAO WANG
    
    Input: Raw Marketstack JSON (list of stock records)
    Output: Cleaned list of daily stock price records including return percentage and price range
    """
    cleaned_rows = []
    
    for record in raw_stock_json:
        try:
            symbol = record.get('symbol', '')
            record_date = record.get('date', '')[:10]  # Extract YYYY-MM-DD
            open_price = record.get('open')
            close_price = record.get('close')
            high_price = record.get('high')
            low_price = record.get('low')
            volume = record.get('volume')
            
            # Skip records with missing essential data
            if not symbol or not record_date:
                continue
            
            # Calculate return percentage: ((Close - Open) / Open) * 100
            if open_price and close_price and open_price != 0:
                return_percentage = round(((close_price - open_price) / open_price) * 100, 4)
            else:
                return_percentage = None
            
            # Calculate price range: High - Low
            if high_price is not None and low_price is not None:
                price_range = round(high_price - low_price, 4)
            else:
                price_range = None
            
            # Build cleaned row
            cleaned_row = {
                'symbol': symbol,
                'date': record_date,
                'open_price': open_price,
                'close_price': close_price,
                'high_price': high_price,
                'low_price': low_price,
                'volume': volume,
                'return_percentage': return_percentage,
                'price_range': price_range
            }
            cleaned_rows.append(cleaned_row)
            
        except Exception as e:
            print(f"Error cleaning record: {e}")
            continue
    
    return cleaned_rows


def insert_stock_data(connection, cleaned_stock_rows):
    """
    Insert cleaned stock data into the stocks_daily table.
    
    BY: RONGHAO WANG
    
    Input: Database connection, cleaned stock rows (list of dicts)
    Output: Inserts rows into daily stock price table
    """
    cursor = connection.cursor()
    
    # Create the stocks_daily table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stocks_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            symbol TEXT,
            open_price REAL,
            close_price REAL,
            high_price REAL,
            low_price REAL,
            volume INTEGER,
            return_percentage REAL,
            price_range REAL,
            UNIQUE(symbol, date)
        )
    ''')
    
    count = 0
    for row in cleaned_stock_rows:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO stocks_daily (
                    date, symbol, open_price, close_price, high_price,
                    low_price, volume, return_percentage, price_range
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['date'],
                row['symbol'],
                row['open_price'],
                row['close_price'],
                row['high_price'],
                row['low_price'],
                row['volume'],
                row['return_percentage'],
                row['price_range']
            ))
            count += 1
        except Exception as e:
            print(f"Error inserting row for {row.get('symbol')} on {row.get('date')}: {e}")
    
    connection.commit()
    print(f"Inserted {count} rows into stocks_daily table")
    return count


def compare_airlines_under_weather(connection):
    """
    Analyze how different airlines perform under similar weather conditions.
    
    BY: RONGHAO WANG
    
    Input: Database connection
    Output: A dataset showing how different airlines perform under similar weather conditions
    
    This function joins stock data with weather data to compare airline performance
    across different weather severity levels.
    """
    cursor = connection.cursor()
    
    # First, check which tables are available
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    available_tables = [row[0] for row in cursor.fetchall()]
    print(f"Available tables: {available_tables}")
    
    # Determine weather table name (different teammates may use different names)
    weather_table = None
    for table_name in ['daily_weather_summary', 'weather_daily', 'weather_history']:
        if table_name in available_tables:
            weather_table = table_name
            break
    
    # Determine stock table name
    stock_table = None
    for table_name in ['stocks_daily', 'stock_history']:
        if table_name in available_tables:
            stock_table = table_name
            break
    
    if not weather_table:
        print("Warning: No weather table found. Using stock data only.")
        # Return stock performance by airline without weather correlation
        cursor.execute(f'''
            SELECT 
                symbol,
                COUNT(*) as trading_days,
                ROUND(AVG(return_percentage), 4) as avg_return,
                ROUND(MIN(return_percentage), 4) as worst_day,
                ROUND(MAX(return_percentage), 4) as best_day,
                ROUND(AVG(price_range), 4) as avg_volatility
            FROM {stock_table}
            WHERE return_percentage IS NOT NULL
            GROUP BY symbol
            ORDER BY avg_return DESC
        ''')
        
        results = cursor.fetchall()
        columns = ['symbol', 'trading_days', 'avg_return', 'worst_day', 'best_day', 'avg_volatility']
        
        data = []
        for row in results:
            data.append(dict(zip(columns, row)))
        
        return data
    
    # If weather table exists, join with stock data
    # Determine weather severity based on available columns
    cursor.execute(f"PRAGMA table_info({weather_table})")
    weather_columns = [col[1] for col in cursor.fetchall()]
    
    # Build weather severity classification based on available columns
    if 'max_wind_speed' in weather_columns:
        severity_column = '''
            CASE 
                WHEN w.max_wind_speed >= 50 OR w.total_precip_mm >= 20 THEN 'Severe'
                WHEN w.max_wind_speed >= 30 OR w.total_precip_mm >= 10 THEN 'Moderate'
                ELSE 'Mild'
            END
        '''
    elif 'weather_description' in weather_columns:
        severity_column = '''
            CASE 
                WHEN w.weather_description LIKE '%storm%' OR w.weather_description LIKE '%snow%' THEN 'Severe'
                WHEN w.weather_description LIKE '%rain%' OR w.weather_description LIKE '%cloud%' THEN 'Moderate'
                ELSE 'Mild'
            END
        '''
    else:
        severity_column = "'Unknown'"
    
    # Get date column name from weather table
    date_col = 'date' if 'date' in weather_columns else 'record_date'
    
    # Join stock and weather data
    query = f'''
        SELECT 
            s.symbol,
            {severity_column} as weather_severity,
            COUNT(*) as trading_days,
            ROUND(AVG(s.return_percentage), 4) as avg_return,
            ROUND(AVG(s.price_range), 4) as avg_volatility
        FROM {stock_table} s
        LEFT JOIN {weather_table} w ON s.date = w.{date_col}
        WHERE s.return_percentage IS NOT NULL
        GROUP BY s.symbol, weather_severity
        ORDER BY s.symbol, weather_severity
    '''
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        columns = ['symbol', 'weather_severity', 'trading_days', 'avg_return', 'avg_volatility']
        
        data = []
        for row in results:
            data.append(dict(zip(columns, row)))
        
        return data
        
    except Exception as e:
        print(f"Error in join query: {e}")
        # Fallback: return basic stock performance
        cursor.execute(f'''
            SELECT 
                symbol,
                'All' as weather_severity,
                COUNT(*) as trading_days,
                ROUND(AVG(return_percentage), 4) as avg_return,
                ROUND(AVG(price_range), 4) as avg_volatility
            FROM {stock_table}
            WHERE return_percentage IS NOT NULL
            GROUP BY symbol
        ''')
        results = cursor.fetchall()
        columns = ['symbol', 'weather_severity', 'trading_days', 'avg_return', 'avg_volatility']
        
        data = []
        for row in results:
            data.append(dict(zip(columns, row)))
        
        return data


def plot_airline_comparison(data, output_file='airline_comparison.png'):
    """
    Create a visualization comparing airlines under different conditions.
    
    BY: RONGHAO WANG
    
    Input: Airline performance dataset (list of dicts)
    Output: A visualization comparing airlines saved to file
    """
    if not data:
        print("No data to plot!")
        return
    
    # Extract unique airlines and weather conditions
    airlines = list(set(row['symbol'] for row in data))
    airlines.sort()
    
    # Check if we have weather severity data
    has_weather = 'weather_severity' in data[0] and data[0]['weather_severity'] not in [None, 'All', 'Unknown']
    
    # Set up the figure
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle('Airline Stock Performance Comparison', fontsize=16, fontweight='bold')
    
    # Color palette for airlines
    colors = {'DAL': '#003366', 'UAL': '#0066CC', 'AAL': '#CC0000', 'LUV': '#FF6600'}
    
    if has_weather:
        # Plot 1: Average Return by Airline and Weather Severity
        severities = list(set(row['weather_severity'] for row in data if row['weather_severity']))
        severities.sort()
        
        x = range(len(airlines))
        width = 0.25
        
        for i, severity in enumerate(severities):
            returns = []
            for airline in airlines:
                val = next((row['avg_return'] for row in data 
                           if row['symbol'] == airline and row['weather_severity'] == severity), 0)
                returns.append(val if val else 0)
            
            offset = (i - len(severities)/2 + 0.5) * width
            axes[0].bar([xi + offset for xi in x], returns, width, label=severity, alpha=0.8)
        
        axes[0].set_xlabel('Airline', fontsize=12)
        axes[0].set_ylabel('Average Daily Return (%)', fontsize=12)
        axes[0].set_title('Returns by Weather Condition', fontsize=14)
        axes[0].set_xticks(x)
        axes[0].set_xticklabels(airlines)
        axes[0].legend(title='Weather')
        axes[0].axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        axes[0].grid(axis='y', alpha=0.3)
        
    else:
        # Plot 1: Simple bar chart of average returns
        returns = []
        bar_colors = []
        for airline in airlines:
            val = next((row['avg_return'] for row in data if row['symbol'] == airline), 0)
            returns.append(val if val else 0)
            bar_colors.append(colors.get(airline, '#888888'))
        
        bars = axes[0].bar(airlines, returns, color=bar_colors, alpha=0.8, edgecolor='black')
        axes[0].set_xlabel('Airline', fontsize=12)
        axes[0].set_ylabel('Average Daily Return (%)', fontsize=12)
        axes[0].set_title('Average Daily Stock Returns', fontsize=14)
        axes[0].axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        axes[0].grid(axis='y', alpha=0.3)
        
        # Add value labels on bars
        for bar, val in zip(bars, returns):
            height = bar.get_height()
            axes[0].annotate(f'{val:.2f}%',
                           xy=(bar.get_x() + bar.get_width() / 2, height),
                           xytext=(0, 3),
                           textcoords="offset points",
                           ha='center', va='bottom', fontsize=10)
    
    # Plot 2: Volatility comparison (price range)
    volatilities = []
    bar_colors = []
    for airline in airlines:
        # Get average volatility across all conditions for this airline
        airline_data = [row['avg_volatility'] for row in data if row['symbol'] == airline and row.get('avg_volatility')]
        avg_vol = sum(airline_data) / len(airline_data) if airline_data else 0
        volatilities.append(avg_vol)
        bar_colors.append(colors.get(airline, '#888888'))
    
    bars2 = axes[1].bar(airlines, volatilities, color=bar_colors, alpha=0.8, edgecolor='black')
    axes[1].set_xlabel('Airline', fontsize=12)
    axes[1].set_ylabel('Average Price Range ($)', fontsize=12)
    axes[1].set_title('Stock Volatility (Daily Price Range)', fontsize=14)
    axes[1].grid(axis='y', alpha=0.3)
    
    # Add value labels on bars
    for bar, val in zip(bars2, volatilities):
        height = bar.get_height()
        axes[1].annotate(f'${val:.2f}',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3),
                        textcoords="offset points",
                        ha='center', va='bottom', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"Chart saved to: {output_file}")
    return output_file


def process_stock_data(db_path='stock_data.db'):
    """
    Main function to process raw stock data and generate analysis.
    
    This orchestrates the full pipeline:
    1. Read raw data from stock_history table
    2. Clean the data
    3. Insert into stocks_daily table
    4. Run comparison analysis
    5. Generate visualization
    """
    print("=" * 50)
    print("PROCESSING STOCK DATA - Ronghao Wang")
    print("=" * 50)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Step 1: Read raw JSON data from stock_history
    print("\n[Step 1] Reading raw stock data...")
    try:
        cursor.execute("SELECT full_data_json FROM stock_history")
        raw_rows = cursor.fetchall()
        
        # Parse JSON and collect all records
        all_raw_records = []
        for (json_str,) in raw_rows:
            if json_str:
                try:
                    record = json.loads(json_str)
                    all_raw_records.append(record)
                except json.JSONDecodeError:
                    continue
        
        print(f"   Found {len(all_raw_records)} raw records")
        
    except sqlite3.OperationalError as e:
        print(f"   Error: {e}")
        print("   Make sure you've run fetch_stock_data first!")
        conn.close()
        return
    
    # Step 2: Clean the data
    print("\n[Step 2] Cleaning stock data...")
    cleaned_rows = clean_stock_data(all_raw_records)
    print(f"   Cleaned {len(cleaned_rows)} records")
    
    # Step 3: Insert into stocks_daily table
    print("\n[Step 3] Inserting into stocks_daily table...")
    insert_stock_data(conn, cleaned_rows)
    
    # Step 4: Run comparison analysis
    print("\n[Step 4] Running airline comparison analysis...")
    comparison_data = compare_airlines_under_weather(conn)
    
    print("\n   Analysis Results:")
    print("   " + "-" * 60)
    for row in comparison_data:
        print(f"   {row['symbol']}: Avg Return = {row['avg_return']}%, "
              f"Volatility = ${row.get('avg_volatility', 'N/A')}")
    
    # Step 5: Generate visualization
    print("\n[Step 5] Generating visualization...")
    plot_airline_comparison(comparison_data)
    
    conn.close()
    print("\n" + "=" * 50)
    print("PROCESSING COMPLETE!")
    print("=" * 50)


if __name__ == "__main__":
    process_stock_data()

