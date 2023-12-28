import os
import pandas as pd
from sqlalchemy import create_engine
import yfinance as yf
import pyodbc
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

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
    
    if data is not None:
        # Call the function to save data to Azure SQL Server database
        save_to_azure_sql_server(data)
    
        # Call the function to upload the CSV to Azure Blob Storage
        #save_to_azure_blob(csv_file_path, account_name, account_key, container_name, blob_name)

# Function to upload a file to Azure Blob Storage
def save_to_azure_blob(csv_file_path, account_name, account_key, container_name, blob_name):
    try:
        blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)

        with open(csv_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"File {csv_file_path} uploaded to Azure Storage container {container_name} as blob {blob_name}")

    except Exception as e:
        print(f"Failed to upload {csv_file_path} to Azure Blob Storage: {e}")    

# Function to save data to Azure SQL Server database using sqlalchemy
def save_to_azure_sql_server(data):
    try:
        # Define the connection URL
        connection_url = f"mssql+pymssql://{sql_username}:{sql_password}@{sql_server_name}/{sql_database_name}"

        # Create the database engine
        engine = create_engine(connection_url)

        # Use pandas to_sql method to save the dataframe
        data.to_sql(table_name, con=engine, if_exists='append', index=False)

        print(f"Data saved to Azure SQL Server database: {sql_database_name}, Table: {table_name}")
    except Exception as e:
        print(f"Failed to save data to Azure SQL Server database: {e}")

# Load environment variables from the .env file
load_dotenv()

# Define the stock symbols, date range, and CSV file path
symbols = ['AAPL', 'TSLA']
start_date = '2023-01-01'
end_date = '2023-12-31'
csv_file_path = 'stock_data.csv'

# Retrieve Azure parameters from the environment variables
account_name = os.getenv('AZURE_ACCOUNT_NAME')
account_key = os.getenv('AZURE_ACCOUNT_KEY')
container_name = os.getenv('AZURE_CONTAINER_NAME')
blob_name = 'stock_data.csv'

# Retrieve Azure SQL Server parameters from the environment variables
sql_server_name = os.getenv('SQL_SERVER_NAME')
sql_database_name = os.getenv('SQL_DATABASE_NAME')
sql_username = os.getenv('SQL_USERNAME')
sql_password = os.getenv('SQL_PASSWORD')
table_name = 'stock_info'

# Call the function to extract stock data and save to a CSV file
get_stock_data_and_save(symbols, start_date, end_date, csv_file_path)