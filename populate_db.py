import requests
import sqlite3
import config

connection = sqlite3.connect(config.DB_FILE)
cursor = connection.cursor()


# Define the query parameters including the API key
params = {
    "apikey": config.API_KEY
}


# Make the GET request
response = requests.get(config.BASE_URL, params=params)

def main():
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Convert the response to JSON format
        data = response.json()

        # Now you have the data, you can proceed with grouping it as before
        grouped_data = {}

        for item in data:
            exchange_short_name = item["exchangeShortName"]
            if exchange_short_name not in grouped_data:
                grouped_data[exchange_short_name] = []
            grouped_data[exchange_short_name].append(item)

        processed_stock = dict()
        for item in grouped_data['NYSE']:
            if item['symbol'] is not None:
                symbol = item['symbol']
            else:
                continue
            company_name = item['name'] if item['name'] is not None else "None"
            exchange = 'NYSE'
            processed_stock[symbol] = 'NYSE'
            cursor.execute("INSERT INTO stock (symbol, company, exchange) VALUES (?, ?, ?)", (symbol, company_name, exchange))

        for item in grouped_data['NASDAQ']:
            symbol = item['symbol'] if item['symbol'] is not None else "None"
            if symbol in processed_stock:
                continue
            processed_stock[symbol] = 'NASDAQ'
            company_name = item['name'] if item['name'] is not None else "None"
            exchange = 'NASDAQ'

            cursor.execute("INSERT INTO stock (symbol, company, exchange) VALUES (?, ?, ?)", (symbol, company_name, exchange))


        connection.commit()

    else:
        print("Failed to retrieve data. Status code:", response.status_code)

if __name__ == "__main__":
    main()