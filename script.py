import requests
import os
import csv
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000

example_ticker = {'ticker': 'HTB',
    'name': 'HomeTrust Bancshares,Inc.',
    'market': 'stocks', 
    'locale': 'us',
    'primary_exchange': 'XNYS',
    'type': 'CS',
    'active': True,
    'currency_name': 'usd',
    'cik': '0001538263',
    'composite_figi': 'BBG002CV5W70', 
    'share_class_figi': 'BBG002CV5WZ9',
    'last_updated_utc': '2025-09-20T06:05:17.34210598Z'}

def run_stock_job():
    print(POLYGON_API_KEY)
    
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
    response = requests.get(url)

    tickers = []

    data = response.json()
    #print(data.keys())
    for ticker in data['results']:
        tickers.append(ticker)

    while 'next_url' in data and data.get('status') != 'ERROR':
        print('requesting next page', data['next_url'])
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        data = response.json()
        print(data)
        for ticker in data['results']:
            tickers.append(ticker)

    print(len(tickers))

    # Write tickers to CSV file with the same schema as example_ticker
    csv_filename = 'tickers.csv'
    fieldnames = list(example_ticker.keys())

    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for ticker in tickers:
            # Ensure all fields from example_ticker schema are present
            row = {}
            for field in fieldnames:
                row[field] = ticker.get(field, '')  # Use empty string if field is missing
            
            writer.writerow(row)

    print(f"Successfully wrote {len(tickers)} tickers to {csv_filename}")

if __name__ == '__main__':
    run_stock_job()


