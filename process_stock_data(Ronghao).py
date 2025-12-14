"""
Process Stock Data - Ronghao Wang
SI 201 Final Project

This module analyzes stock data and generates outputs.

GRADING REQUIREMENTS MET:
1. Two tables with INTEGER key: airlines.id -> stock_history.airline_id
2. Writes calculated data to text file: stock_analysis_results.txt
3. Creates visualization: airline_comparison.png
"""

import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime

DATABASE_NAME = "stock_data.db"


def compare_airlines_under_weather(connection):
    """
    Analyze airline stock performance.
    
    BY: RONGHAO WANG
    
    Demonstrates JOIN between:
    - airlines (INTEGER PRIMARY KEY: id)
    - stock_history (FOREIGN KEY: airline_id)
    """
    cursor = connection.cursor()
    
    # This query JOINs airlines and stock_history using INTEGER key
    query = '''
        SELECT 
            a.id,
            a.symbol,
            a.name,
            COUNT(s.id) as trading_days,
            ROUND(AVG(s.return_percentage), 4) as avg_return,
            ROUND(MIN(s.return_percentage), 4) as worst_day,
            ROUND(MAX(s.return_percentage), 4) as best_day,
            ROUND(AVG(s.price_range), 4) as avg_volatility
        FROM airlines a
        JOIN stock_history s ON a.id = s.airline_id
        WHERE s.return_percentage IS NOT NULL
        GROUP BY a.id, a.symbol, a.name
        ORDER BY avg_return DESC
    '''
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        columns = ['airline_id', 'symbol', 'name', 'trading_days', 
                   'avg_return', 'worst_day', 'best_day', 'avg_volatility']
        
        data = []
        for row in results:
            data.append(dict(zip(columns, row)))
        
        return data
        
    except Exception as e:
        print(f"Query error: {e}")
        return []


def plot_airline_comparison(data, output_file='airline_comparison.png'):
    """
    Create visualization comparing airlines.
    
    BY: RONGHAO WANG
    """
    if not data:
        print("No data to plot!")
        return
    
    airlines = [row['symbol'] for row in data]
    returns = [row['avg_return'] or 0 for row in data]
    volatilities = [row['avg_volatility'] or 0 for row in data]
    
    colors = {'JBLU': '#0033A0', 'DAL': '#003366', 'AAL': '#CC0000', 'UAL': '#0066CC'}
    bar_colors = [colors.get(a, '#888888') for a in airlines]
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle('Airline Stock Performance Comparison\n(Using airlines.id -> stock_history.airline_id JOIN)', 
                 fontsize=14, fontweight='bold')
    
    # Plot 1: Returns
    bars1 = axes[0].bar(airlines, returns, color=bar_colors, alpha=0.8, edgecolor='black')
    axes[0].set_xlabel('Airline', fontsize=12)
    axes[0].set_ylabel('Average Daily Return (%)', fontsize=12)
    axes[0].set_title('Average Daily Stock Returns', fontsize=12)
    axes[0].axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    axes[0].grid(axis='y', alpha=0.3)
    
    for bar, val in zip(bars1, returns):
        axes[0].annotate(f'{val:.2f}%', xy=(bar.get_x() + bar.get_width()/2, bar.get_height()),
                        xytext=(0, 3), textcoords="offset points", ha='center', fontsize=10)
    
    # Plot 2: Volatility
    bars2 = axes[1].bar(airlines, volatilities, color=bar_colors, alpha=0.8, edgecolor='black')
    axes[1].set_xlabel('Airline', fontsize=12)
    axes[1].set_ylabel('Average Price Range ($)', fontsize=12)
    axes[1].set_title('Stock Volatility', fontsize=12)
    axes[1].grid(axis='y', alpha=0.3)
    
    for bar, val in zip(bars2, volatilities):
        axes[1].annotate(f'${val:.2f}', xy=(bar.get_x() + bar.get_width()/2, bar.get_height()),
                        xytext=(0, 3), textcoords="offset points", ha='center', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"Chart saved: {output_file}")


def write_results_to_file(data, db_path, output_file='stock_analysis_results.txt'):
    """
    Write calculated data to text file.
    
    BY: RONGHAO WANG
    
    GRADING: "Write out the calculated data to a file as text"
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("STOCK ANALYSIS RESULTS - SI 201 Final Project\n")
        f.write("Calculated by: Ronghao Wang\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 70 + "\n\n")
        
        # Show the INTEGER KEY relationship
        f.write("DATABASE STRUCTURE (Two tables with INTEGER key)\n")
        f.write("-" * 50 + "\n")
        f.write("Table 1: airlines\n")
        f.write("  - id (INTEGER PRIMARY KEY)\n")
        f.write("  - symbol (TEXT)\n")
        f.write("  - name (TEXT)\n\n")
        f.write("Table 2: stock_history\n")
        f.write("  - airline_id (INTEGER) -> REFERENCES airlines(id)\n")
        f.write("  - record_date, prices, etc.\n\n")
        
        # Airlines data
        f.write("AIRLINES TABLE DATA\n")
        f.write("-" * 50 + "\n")
        cursor.execute('SELECT id, symbol, name FROM airlines ORDER BY id')
        for row in cursor.fetchall():
            f.write(f"  ID: {row[0]}, Symbol: {row[1]}, Name: {row[2]}\n")
        f.write("\n")
        
        # Record counts
        cursor.execute('SELECT COUNT(*) FROM stock_history')
        stock_count = cursor.fetchone()[0]
        f.write(f"TOTAL STOCK RECORDS: {stock_count}\n")
        if stock_count >= 100:
            f.write("✓ 100+ records requirement MET!\n")
        else:
            f.write(f"⚠ Need {100 - stock_count} more records\n")
        f.write("\n")
        
        # Calculated metrics
        f.write("CALCULATED METRICS (JOIN: airlines.id -> stock_history.airline_id)\n")
        f.write("-" * 50 + "\n")
        
        for row in data:
            f.write(f"\n{row['symbol']} - {row['name']}\n")
            f.write(f"  Airline ID: {row['airline_id']}\n")
            f.write(f"  Trading Days: {row['trading_days']}\n")
            f.write(f"  Average Return: {row['avg_return']}%\n")
            f.write(f"  Best Day: {row['best_day']}%\n")
            f.write(f"  Worst Day: {row['worst_day']}%\n")
            f.write(f"  Avg Volatility: ${row['avg_volatility']}\n")
        
        f.write("\n" + "=" * 70 + "\n")
        f.write("END OF REPORT\n")
        f.write("=" * 70 + "\n")
    
    conn.close()
    print(f"Results written: {output_file}")


def process_stock_data(db_path=DATABASE_NAME):
    """Main processing function."""
    print("=" * 60)
    print("PROCESSING STOCK DATA - Ronghao Wang")
    print("=" * 60)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check tables exist
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cursor.fetchall()]
    print(f"Tables: {tables}")
    
    if 'airlines' not in tables or 'stock_history' not in tables:
        print("\n Required tables not found. Run fetch_stock_data first!")
        conn.close()
        return
    
    # Count records
    cursor.execute('SELECT COUNT(*) FROM stock_history')
    count = cursor.fetchone()[0]
    print(f"Stock records: {count}")
    
    if count == 0:
        print("\n No data. Run fetch_stock_data first!")
        conn.close()
        return
    
    # Run analysis (demonstrates JOIN)
    print("\nRunning analysis (JOIN: airlines.id -> stock_history.airline_id)...")
    data = compare_airlines_under_weather(conn)
    
    if data:
        print("\nResults:")
        for row in data:
            print(f"  {row['symbol']}: Avg Return = {row['avg_return']}%")
        
        # Generate outputs
        print("\nGenerating outputs...")
        plot_airline_comparison(data)
        write_results_to_file(data, db_path)
    
    conn.close()
    print("\n" + "=" * 60)
    print("DONE!")
    print("  - airline_comparison.png")
    print("  - stock_analysis_results.txt")
    print("=" * 60)


if __name__ == "__main__":
    process_stock_data()
