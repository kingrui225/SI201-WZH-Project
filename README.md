# SI 201 Final Project - WZH Project

## Group Information

| Name | Email | Role |
|------|-------|------|
| Zuming Hu | zuminghu@umich.edu | Weather Data & Stock-Flight Analysis |
| Ke Zhong | kzhong@umich.edu | Flight Data & Database Setup |
| Ronghao Wang | ronghao@umich.edu | Stock Data & Airline Comparison |

---

## Project Overview

This project analyzes the relationships between **weather conditions**, **flight delays/cancellations**, and **airline stock performance** using data from three APIs:

- **Aviationstack** - Flight information
- **Weatherstack** - Weather data
- **Marketstack** - Stock market data

---

## Data Sources

### 1. Aviationstack API
- **Base URL:** `http://api.aviationstack.com/v1`
- **Documentation:** https://aviationstack.com/documentation
- **Data Collected:**
  - Airline name
  - Flight number
  - Departure & arrival airports
  - Estimated schedule
  - Actual schedule
  - Flight status

### 2. Weatherstack API
- **Base URL:** `http://api.weatherstack.com`
- **Documentation:** https://weatherstack.com/documentation
- **Data Collected:**
  - City name
  - Local time
  - Wind speed
  - Humidity
  - Visibility
  - Precipitation
  - Weather description

### 3. Marketstack API
- **Base URL:** `http://api.marketstack.com/v1`
- **Documentation:** https://marketstack.com/documentation
- **Data Collected:**
  - Stock symbol
  - Date
  - Open price
  - Close price
  - High price
  - Low price
  - Trading volume

---

## Calculated Metrics

### From Flight Data (Aviationstack)
| Metric | Description |
|--------|-------------|
| Total flights per day | Count of all flights |
| Delayed flights per day | Count of flights with delays |
| Cancelled flights per day | Count of cancelled flights |
| Daily delay rate | (Delayed flights / Total flights) × 100 |
| Daily cancellation rate | (Cancelled flights / Total flights) × 100 |
| Average delay time | Mean delay in minutes |

### From Weather Data (Weatherstack)
| Metric | Description |
|--------|-------------|
| Daily average temperature | Mean temperature for the day |
| Daily maximum wind speed | Highest recorded wind speed |
| Weather severity indicator | Based on precipitation/weather description |

### From Stock Data (Marketstack)
| Metric | Description |
|--------|-------------|
| Daily stock return % | ((Close - Open) / Open) × 100 |
| Daily price range | High - Low |
| Trading activity level | Based on trading volume |

---

## Visualizations

Using **Matplotlib** to create:

| Chart Type | X-Axis | Y-Axis | Purpose |
|------------|--------|--------|---------|
| Line Chart | Visibility | Delay Time | Analyze visibility impact on delays |
| Line Chart | Wind Speed | Delay Time | Analyze wind impact on delays |
| Bar Chart | Weather Type | Avg Delay Minutes | Compare delays across weather conditions |
| Bar Chart | Delay Time | Stock Price Change | Explore delay-stock correlation |

---

## Project Structure

```
Final Project/
├── README.md                 # This file - project documentation
├── requirements.txt          # Python dependencies
├── config.py                 # API keys and configuration
├── .env.example              # Template for environment variables
├── data_collection.py        # API request functions
├── data_cleaning.py          # Data transformation functions
├── database.py               # SQLite database operations
├── analysis.py               # Data analysis and calculations
├── visualization.py          # Matplotlib chart generation
├── main.py                   # Main execution script
└── wzh_project.db            # SQLite database (generated)
```

---

## Function Assignments

### 1. Data Collection Functions

| Function | Responsible | Input | Output |
|----------|-------------|-------|--------|
| `get_flight_data()` | Ke Zhong | API key, airport code, date | Raw Aviationstack JSON |
| `get_weather_data()` | Zuming Hu | API key, city name, date | Raw Weatherstack JSON |
| `get_stock_data()` | Ronghao Wang | API key, ticker, start/end date | Raw Marketstack JSON |

### 2. Data Cleaning Functions

| Function | Responsible | Input | Output |
|----------|-------------|-------|--------|
| `clean_flight_data()` | Ke Zhong | Raw flight JSON | Cleaned flight rows |
| `clean_weather_data()` | Zuming Hu | Raw weather JSON | Cleaned weather rows |
| `clean_stock_data()` | Ronghao Wang | Raw stock JSON | Cleaned stock rows |

### 3. Database Functions

| Function | Responsible | Input | Output |
|----------|-------------|-------|--------|
| `create_tables()` | Ke Zhong | SQLite connection | Creates tables |
| `insert_flight_data()` | Ke Zhong | Connection, flight rows | Inserts flight data |
| `insert_weather_data()` | Zuming Hu | Connection, weather rows | Inserts weather data |
| `insert_stock_data()` | Ronghao Wang | Connection, stock rows | Inserts stock data |

### 4. Analysis Functions

| Function | Responsible | Input | Output |
|----------|-------------|-------|--------|
| `calculate_weather_flight_relationship()` | Ke Zhong | Connection | Weather-delay dataset |
| `calculate_flight_stock_relationship()` | Zuming Hu | Connection | Flight-stock dataset |
| `compare_airlines_under_weather()` | Ronghao Wang | Connection | Airline comparison dataset |

### 5. Visualization Functions

| Function | Responsible | Input | Output |
|----------|-------------|-------|--------|
| `plot_weather_vs_delays()` | Ke Zhong | Weather-delay data | Bar chart (saved to file) |
| `plot_delays_vs_stock_returns()` | Zuming Hu | Flight-stock data | Line/scatter plot |
| `plot_airline_comparison()` | Ronghao Wang | Airline data | Comparison chart |

---

## Function Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            DATA COLLECTION LAYER                             │
├─────────────────────┬─────────────────────┬─────────────────────────────────┤
│   get_flight_data() │  get_weather_data() │       get_stock_data()          │
│     (Ke Zhong)      │    (Zuming Hu)      │       (Ronghao Wang)            │
│         ↓           │         ↓           │              ↓                  │
│   Raw Flight JSON   │  Raw Weather JSON   │       Raw Stock JSON            │
└─────────────────────┴─────────────────────┴─────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA CLEANING LAYER                                │
├─────────────────────┬─────────────────────┬─────────────────────────────────┤
│  clean_flight_data()│ clean_weather_data()│      clean_stock_data()         │
│     (Ke Zhong)      │    (Zuming Hu)      │       (Ronghao Wang)            │
│         ↓           │         ↓           │              ↓                  │
│  Cleaned Flight Rows│ Cleaned Weather Rows│      Cleaned Stock Rows         │
└─────────────────────┴─────────────────────┴─────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            DATABASE LAYER                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                    create_tables() - (Ke Zhong)                              │
├─────────────────────┬─────────────────────┬─────────────────────────────────┤
│ insert_flight_data()│insert_weather_data()│      insert_stock_data()        │
│     (Ke Zhong)      │    (Zuming Hu)      │       (Ronghao Wang)            │
└─────────────────────┴─────────────────────┴─────────────────────────────────┘
                                    │
                                    ▼
                        ┌───────────────────┐
                        │   wzh_project.db  │
                        │  (SQLite Database)│
                        └───────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            ANALYSIS LAYER                                    │
├──────────────────────────┬──────────────────────┬───────────────────────────┤
│calculate_weather_flight_ │calculate_flight_stock│ compare_airlines_under_   │
│    relationship()        │   _relationship()    │      weather()            │
│      (Ke Zhong)          │     (Zuming Hu)      │    (Ronghao Wang)         │
└──────────────────────────┴──────────────────────┴───────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          VISUALIZATION LAYER                                 │
├──────────────────────────┬──────────────────────┬───────────────────────────┤
│  plot_weather_vs_delays()│plot_delays_vs_stock_ │  plot_airline_comparison()│
│      (Ke Zhong)          │     returns()        │    (Ronghao Wang)         │
│                          │     (Zuming Hu)      │                           │
│         ↓                │         ↓            │           ↓               │
│  weather_delays.png      │  delays_stock.png    │  airline_comparison.png   │
└──────────────────────────┴──────────────────────┴───────────────────────────┘
```

---

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure API Keys

1. Copy `.env.example` to create your own config:
   ```bash
   copy .env.example .env
   ```

2. Edit `config.py` and replace placeholder API keys:
   - Get Aviationstack key: https://aviationstack.com/signup/free
   - Get Weatherstack key: https://weatherstack.com/signup/free
   - Get Marketstack key: https://marketstack.com/signup/free

### 3. Run the Project

```bash
python main.py
```

---

## Database Schema

### flights_daily Table
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| date | TEXT | Date (YYYY-MM-DD) |
| airport_code | TEXT | Airport IATA code |
| total_flights | INTEGER | Total flights count |
| delayed_flights | INTEGER | Delayed flights count |
| cancelled_flights | INTEGER | Cancelled flights count |
| avg_delay_minutes | REAL | Average delay in minutes |

### weather_daily Table
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| date | TEXT | Date (YYYY-MM-DD) |
| city | TEXT | City name |
| avg_temperature | REAL | Average temperature |
| max_wind_speed | REAL | Maximum wind speed |
| humidity | REAL | Humidity percentage |
| visibility | REAL | Visibility |
| precipitation | REAL | Precipitation amount |
| weather_description | TEXT | Weather condition |
| severity_indicator | INTEGER | Weather severity (1-5) |

### stocks_daily Table
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| date | TEXT | Date (YYYY-MM-DD) |
| symbol | TEXT | Stock ticker symbol |
| open_price | REAL | Opening price |
| close_price | REAL | Closing price |
| high_price | REAL | Highest price |
| low_price | REAL | Lowest price |
| volume | INTEGER | Trading volume |
| return_percentage | REAL | Daily return % |
| price_range | REAL | High - Low |

---

## Implementation Timeline

| Phase | Tasks | Deadline |
|-------|-------|----------|
| Phase 1 | API setup, config, basic structure | Week 1 |
| Phase 2 | Data collection & cleaning functions | Week 2 |
| Phase 3 | Database setup & insertion functions | Week 3 |
| Phase 4 | Analysis functions | Week 4 |
| Phase 5 | Visualization & final testing | Week 5 |

---

## Notes

- Free tier API limits may apply - check each API's documentation
- Data is stored incrementally (25 items per run as per SI 201 requirements)
- All visualizations are saved as PNG files in the project directory

---

*SI 201 - Data Manipulation and Analysis*  
*University of Michigan - Fall 2024*

