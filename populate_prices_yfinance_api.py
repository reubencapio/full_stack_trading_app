import config
import sqlite3
from datetime import datetime
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor


def convert_time(time: int) -> str:
    # Unix timestamp in milliseconds
    timestamp_ms = 1673298000000

    # Convert the timestamp from milliseconds to seconds
    timestamp_s = timestamp_ms / 1000

    # Convert the timestamp to a datetime object
    datetime_obj = datetime.fromtimestamp(timestamp_s)

    # Convert the datetime object to a string in the desired format
    date_string = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

    # Output the formatted date string
    return str(date_string)

connection = sqlite3.connect(config.DB_FILE)

connection.row_factory = sqlite3.Row

cursor = connection.cursor()
symbols = []
stock_dict = {}
historical_data = {}

def save_ids():
    cursor.execute("""
        SELECT id, symbol, company FROM stock
    """)

    rows = cursor.fetchall()


    for row in rows:
        symbol = row['symbol']
        symbols.append(symbol)
        stock_dict[symbol] = row['id']


def fetch_data():
    # Save IDs (assuming this is a predefined function)
    save_ids()

    # Initialize a dictionary to hold historical data for each symbol
    historical_data = {}

    # Function to fetch historical data for a single symbol
    def fetch_symbol_data(symbol):
        # Define the stock ticker symbol
        ticker_symbol = symbol  # Example: Apple Inc.

        # Create a ticker object for the specified stock symbol
        stock = yf.Ticker(ticker_symbol)

        # Fetch historical data for the specified period (15 days)
        historical_data = stock.history(period='5d')

        # Return the data
        return symbol, historical_data

    # Use ThreadPoolExecutor to fetch data concurrently with 8 threads
    with ThreadPoolExecutor(max_workers=8) as executor:
        # Submit tasks for each symbol to the thread pool and gather results
        futures = [executor.submit(fetch_symbol_data, symbol) for symbol in symbols]

        # Collect results as they become available
        for future in futures:
            symbol, data = future.result()
            historical_data[symbol] = data

    # Return the collected historical data
    return historical_data

def main():

    # Fetch data from the API
    data = fetch_data()

    # Display the data
    for k, v in stock_dict:

        data = data[k]
        # Iterate through the results and extract the required fields
        time_series = data["Time Series (Daily)"]

        # Iterate over each date in the time series data
        for date, row in time_series.items():
            # Extract data for each date
            open = row['Open']
            high = row['High']
            low = row['Low']
            close = row['Close']
            volume = row['Volume']

            stock_id = stock_dict[k]


            cursor.execute("""
                INSERT INTO stock_price(stock_id, date, open, high, low, close, volume) VALUES(?, ?, ?, ?, ?, ?, ?)
            """, (stock_id, date, open, high, low, close, volume))


        connection.commit()


if __name__ == "__main__":
    main()