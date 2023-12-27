import pandas as pd
import yfinance as yf

# Define the stock symbols and date range for data extraction
symbols = ['AAPL', 'TSLA']
start_date = '2023-01-01'
end_date = '2023-12-31'

# Extract data from Yahoo Finance
data = pd.DataFrame()
for symbol in symbols:
    ticker = yf.Ticker(symbol)
    stock_data = ticker.history(start=start_date, end=end_date)
    stock_data['symbol'] = symbol
    data = pd.concat([data, stock_data])

    print(data)
