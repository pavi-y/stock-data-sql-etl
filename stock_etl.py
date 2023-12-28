import os
import pandas as pd
import yfinance as yf
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from datetime import datetime

# Function to upload a file to Azure Blob Storage
def save_to_azure_blob(csv_file_path, account_name, account_key, container_name, blob_name):
    blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    with open(csv_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f"File {csv_file_path} uploaded to Azure Storage container {container_name} as blob {blob_name}")

# Function to extract stock data and save to a CSV file
def get_stock_data_and_save(symbols, start_date, end_date, csv_file_path):
    data = pd.DataFrame()
    for symbol in symbols:
        ticker = yf.Ticker(symbol)
        stock_data = ticker.history(start=start_date, end=end_date)
        stock_data['symbol'] = symbol
        data = pd.concat([data, stock_data])
    data.to_csv(csv_file_path)
    print(f"Stock data saved to {csv_file_path}")
    save_to_azure_blob(csv_file_path, account_name, account_key, container_name, blob_name)

# Load environment variables from the .env file
load_dotenv()

# Define the stock symbols, date range, and CSV file path
symbols = ['AAPL', 'TSLA']
start_date = '2023-01-01'
end_date = '2023-12-31'
current_datetime = datetime.now()
datetime_str = current_datetime.strftime('%Y%m%d_%H%M')
csv_file_path = f'stock_data_{datetime_str}.csv'

# Retrieve Azure parameters from the environment variables
account_name = os.getenv('AZURE_ACCOUNT_NAME')
account_key = os.getenv('AZURE_ACCOUNT_KEY')
container_name = os.getenv('AZURE_CONTAINER_NAME')
blob_name = f'stock_data_{datetime_str}.csv'

# Call the function to extract stock data and save to a CSV file
get_stock_data_and_save(symbols, start_date, end_date, csv_file_path)