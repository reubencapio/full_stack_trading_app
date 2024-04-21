import requests
import config
import sqlite3
from datetime import datetime
import pandas as pd



# Define a function to decrement the date by one business day
def decrement_one_business_day(date):
    # Subtract one business day using pandas BDay offset
    new_date = date - pd.tseries.offsets.BDay(1)
    return new_date


#https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo

def convert_time(timestamp_ms: int) -> str:
    # Unix timestamp in milliseconds

    # Convert the timestamp from milliseconds to seconds
    timestamp_s = timestamp_ms / 1000

    # Convert the timestamp to a datetime object
    datetime_obj = datetime.fromtimestamp(timestamp_s)

    # Convert the datetime object to a string in the desired format
    date_string = datetime_obj.strftime("%Y-%m-%d")

    # Output the formatted date string
    return str(date_string)

connection = sqlite3.connect(config.DB_FILE)

connection.row_factory = sqlite3.Row

cursor = connection.cursor()
symbols = []
stock_dict = {}


def save_ids():
    cursor.execute("""
        SELECT id, symbol, company FROM stock
    """)

    rows = cursor.fetchall()


    for row in rows:
        symbol = row['symbol']
        symbols.append(symbol)
        stock_dict[symbol] = row['id']


def fetch_polygon_data(date, api_key):
    save_ids()

    # Base URL for the Polygon.io API endpoint
    #https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2023-01-09/2023-01-09?
    #adjusted=true&sort=asc&limit=120&apiKey=8tMLADmu1Cnj84p0DmsyKcwiihl7s4nu
    base_url = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"

    # Construct the full API URL with the provided date
    api_url = f"{base_url}{date}?adjusted=true&apiKey={api_key}"

    # Make the API request
    response = requests.get(api_url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        data_type = type(data)
        # Return the data
        return data
    else:
        # Handle request errors (if any)
        print(f"Failed to fetch data: {response.status_code} - {response.text}")
        return None

def str_to_datetime_obj(date: str):
    format = "%Y-%m-%d"
    # Convert the string to a datetime object
    date = datetime.strptime(date, format)
    return date

def datetime_obj_to_str(date: datetime):
    date = datetime.strptime(str(date), "%Y-%m-%d %H:%M:%S")
    date = date.strftime("%Y-%m-%d")
    return date

def main():

    current_date_time = datetime.now()
    previous_day = decrement_one_business_day(current_date_time)
    # Parse the date string
    date_obj = datetime.strptime(str(previous_day), "%Y-%m-%d %H:%M:%S.%f")
    date = date_obj.strftime("%Y-%m-%d")

    for i in range(1, 16):

        # Convert the initial date to a pandas Timestamp
        api_key = config.POLYGON_API_KEY
        # Fetch data from the API
        data = fetch_polygon_data(date, api_key)
        insert_data_to_db(data)

        date = str_to_datetime_obj(date)
        date = decrement_one_business_day(date)
        date = datetime_obj_to_str(date)

def insert_data_to_db(data):
    # Display the data
    if data:
        # Iterate through the results and extract the required fields
        result = data['results']
        for entry in result:
            ticker = entry['T']
            if ticker not in stock_dict:
                continue
            stock_id = stock_dict[ticker]
            volume = entry['v']  # Volume
            open = entry['o']  # Opening Price
            close = entry['c']  # Closing Price
            high = entry['h']  # Highest Price
            low = entry['l']  # lowest Price
            date = convert_time(entry['t'])  #

            cursor.execute("""
                INSERT INTO stock_price(stock_id, date, open, high, low, close, volume) VALUES(?, ?, ?, ?, ?, ?, ?)
            """, (stock_id, date, open, high, low, close, volume))


        connection.commit()


if __name__ == "__main__":
    main()