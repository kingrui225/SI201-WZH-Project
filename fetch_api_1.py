import requests
from config import AVIATIONSTACK_KEY



def fetch_flight_data(airport_code, date):
    """
    Fetch raw flight data from Aviationstack API.

    Args:
        airport_code (str): IATA airport code (e.g., 'DTW' or 'LAX')
        date (str): Date in 'YYYY-MM-DD'

    Returns:
        dict: JSON response from the API OR {} if failed
    """
    base_url = "http://api.aviationstack.com/v1/flights"

    params = {
        "access_key": AVIATIONSTACK_KEY,
        "dep_iata": airport_code,
        "flight_date": date,
        "limit": 100
    }

    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        return data

    except requests.exceptions.RequestException as e:
        print("Error fetching flight data:", e)
        return {}