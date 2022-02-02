import pandas as pd
from binance import Client
import config

import time

# Instantiate client with api keys
client = Client(config.api_key, config.api_secret)

print(client)

# Reads the predefined currencies, position and amount from the csv
pos_frame = pd.read_csv('data.csv')

# This function allows you to change the position values in the csv. 0 is sell, 1 is buy


def changePos(curr, buy=True):
    if buy:
        pos_frame.loc[pos_frame.Currency == curr, 'position'] = 1
    else:
        pos_frame.loc[pos_frame.Currency == curr, 'position'] = 0

    pos_frame.to_csv('data', index=False)

# This function gets hourly data from Binance and creates a dataframe with relevant columns.


def getHourlyData(symbol):
    frame = pd.DataFrame(client.get_historical_klines(
        symbol, '1h', '25 hours ago UTC'))
    frame = frame.iloc[:, :5]
    frame.columns = ['Time', 'Open', 'High', 'Low', 'Close']
    # Convert Time Open High Low and Close values to float
    frame[['Open', 'High', 'Low', 'Close']] = frame[[
        'Open', 'High', 'Low', 'Close']].astype(float)
    # Convert time from miliseconds to datetime
    frame.Time = pd.to_datetime(frame.Time, unit='ms')
    return frame

# This function caluclates the simple moving average (SMA) from the last 6 hours (FastSMA) and last 24 hours(SlowSMA).


def applyTechnicals(df):
    df['FastSMA'] = df.Close.rolling(7).mean()
    df['SlowSMA'] = df.Close.rolling(24).mean()


# This function calculates if a buy or sell order should be placed and executes the order
def trader(curr):
    # Get predefined quantity
    qty = pos_frame[pos_frame.Currency == curr].quantity.values[0]
    # Get the last 24 hours of data
    df = getHourlyData(curr)
    # Calcuate FastSMA and SlowSMA
    applyTechnicals(df)
    # Get the last row of the dataframe as SlowSMA only shows on the last row
    lastrow = df.iloc[-1]
    # If the currency is not in a buy position
    if not pos_frame[pos_frame.Currency == curr].position.values[0]:
        # If the FastSMA is bigger than the SlowSMA that indicates the price may be rising and the currency should be bought
        if lastrow.FastSMA > lastrow.SlowSMA:
            # Execute buy order with Binance API
            order = client.create_order(
                symbol=curr, side='BUY', type='MARKET', quantity=qty)
            print(order)
            # Change position in pos_frame to 1 (buy)
            changePos(curr, buy=True)
        else:
            print(f'Not In position for {curr} and condition not fulfilled')
    else:
        print(f"Already in {curr} position")
        # If the SlowSMA is bigger than the FastSMA that indicates the price may be falling and the currency should be sold
        if lastrow.SlowSMA > lastrow.FastSMA:
            # Execute sell order with Binance API
            order = client.create_order(
                symbol=curr, side='SELL', type='MARKET', quantity=qty)
            print(order)
            # Change position in pos_frame to 0 (buy)
            changePos(curr, buy=False)


# Loop script every 5 mins
# TODO: Change this to windows scheduler
while True:
    for coin in pos_frame.Currency:
        trader(coin)
    time.sleep(300)
