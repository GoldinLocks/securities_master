# https://medium.com/@peter.nistrup/full-historical-data-for-every-crypto-on-binance-using-the-python-api-de017de42c2f
# IMPORTS
import pandas as pd
import math
import os.path
import time
# import json
import requests
from bitmex import bitmex
from binance.client import Client
from datetime import timedelta, datetime
from dateutil import parser
from tqdm import tqdm_notebook #(Optional, used for progress-bars)

### API
bitmex_api_key = ''#Enter your own API-key here
bitmex_api_secret = ''#Enter your own API-secret here
binance_api_key = ''#Enter your own API-key here
binance_api_secret = ''#Enter your own API-secret here

### CONSTANTS
binsizes = {"1m": 1, "5m": 5, "1h": 60, "1d": 1440}
batch_size = 750
bitmex_client = bitmex(test=False, api_key=bitmex_api_key, api_secret=bitmex_api_secret)
binance_client = Client(api_key=binance_api_key, api_secret=binance_api_secret)


### FUNCTIONS
def minutes_of_new_data(symbol, kline_size, data, source):
    if len(data) > 0:  old = parser.parse(data["timestamp"].iloc[-1])
    elif source == "binance": old = datetime.strptime('1 Jan 2018', '%d %b %Y')
    elif source == "bitmex": old = bitmex_client.Trade.Trade_getBucketed(symbol=symbol, binSize=kline_size, count=1, reverse=False).result()[0][0]['timestamp']
    if source == "binance": new = pd.to_datetime(binance_client.get_klines(symbol=symbol, interval=kline_size)[-1][0], unit='ms')
    if source == "bitmex": new = bitmex_client.Trade.Trade_getBucketed(symbol=symbol, binSize=kline_size, count=1, reverse=True).result()[0][0]['timestamp']
    return old, new

def get_all_binance(symbol, kline_size, save = False):
    filename = '%s_%s_data.csv' % (symbol, kline_size)
    if os.path.isfile(filename): data_df = pd.read_csv(filename)
    else: data_df = pd.DataFrame()
    oldest_point, newest_point = minutes_of_new_data(symbol, kline_size, data_df, source = "binance")
    delta_min = (newest_point - oldest_point).total_seconds()/60
    available_data = math.ceil(delta_min/binsizes[kline_size])
    if oldest_point == datetime.strptime('1 Jan 2018', '%d %b %Y'): print('Downloading all available %s data for %s. Be patient..!' % (kline_size, symbol))
    else: print('Downloading %d minutes of new data available for %s, i.e. %d instances of %s data.' % (delta_min, symbol, available_data, kline_size))
    klines = binance_client.get_historical_klines(symbol, kline_size, oldest_point.strftime("%d %b %Y %H:%M:%S"), newest_point.strftime("%d %b %Y %H:%M:%S"))
    data = pd.DataFrame(klines, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore' ])
    data = data.drop('ignore', axis=1)
    data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
    TS = data['timestamp'] 
    if len(data_df) > 0:
        temp_df = pd.DataFrame(data)
        data_df = data_df.append(temp_df)
    else: data_df = data
    data_df.set_index('timestamp', inplace=True)
    if save: data_df.to_csv(filename)
    print('All caught up..!')
    return data_df

def get_all_bitmex(symbol, kline_size, save = False):
    filename = '%s_%s_data.csv' % (symbol, kline_size)
    if os.path.isfile(filename): data_df = pd.read_csv(filename)
    else: data_df = pd.DataFrame()
    oldest_point, newest_point = minutes_of_new_data(symbol, kline_size, data_df, source = "bitmex")
    delta_min = (newest_point - oldest_point).total_seconds()/60
    available_data = math.ceil(delta_min/binsizes[kline_size])
    rounds = math.ceil(available_data / batch_size)
    if rounds > 0:
        print('Downloading %d minutes of new data available for %s, i.e. %d instances of %s data in %d rounds.' % (delta_min, symbol, available_data, kline_size, rounds))
        for round_num in tqdm_notebook(range(rounds)):
            time.sleep(1)
            new_time = (oldest_point + timedelta(minutes = round_num * batch_size * binsizes[kline_size]))
            data = bitmex_client.Trade.Trade_getBucketed(symbol=symbol, binSize=kline_size, count=batch_size, startTime = new_time).result()[0]
            temp_df = pd.DataFrame(data)
            data_df = data_df.append(temp_df)
    data_df.set_index('timestamp', inplace=True)
    if save and rounds > 0: data_df.to_csv(filename)
    print('All caught up..!')
    return data_df

# For all Binance symbols
# https://steemit.com/money/@marketstack/visualizing-alt-coin-correlation
# symbols = json.loads(requests.get("https://api.binance.com/api/v1/exchangeInfo").text)
# symbols = [symbol['symbol'] for symbol in symbols['symbols'] if symbol['quoteAsset'] == 'BTC']
binance_symbols = ["BTCUSDT", "ETHUSDT", "ETHBTC", "XRPBTC", "ADABTC", "EOSBTC", "TRXBTC", "XLMBTC", "LTCBTC"]
for symbol in binance_symbols:
    get_all_binance(symbol, '1m', save = False)

df0 = get_all_binance("BTCUSDT", '1m', save = False)
print(df0.head(5))
df1 = get_all_binance("ETHUSDT", '1m', save = False)
df2 = get_all_binance("ETHBTC", '1m', save = False)
df3 = get_all_binance("XRPBTC", '1m', save = False)
df4 = get_all_binance("ADABTC", '1m', save = False)
df5 = get_all_binance("EOSBTC", '1m', save = False)
df6 = get_all_binance("TRXBTC", '1m', save = False)
df7 = get_all_binance("XLMBTC", '1m', save = False)
df8 = get_all_binance("LTCBTC", '1m', save = False)

# For BitMex
bitmex_symbols = ["XBTUSD", "ETHUSD"]
for symbol in bitmex_symbols:
    get_all_bitmex(symbol, '1m', save = False)

df9 = get_all_bitmex("XBTUSD", '1m', save = False)
df10 = get_all_bitmex("ETHUSD", '1m', save = False)

# https://kanoki.org/2019/01/12/how-to-merge-multiple-csv-files-and-upload-to-mysql-or-postgressql-using-python/
import sqlalchemy  
from sqlalchemy import create_engine

engine = create_engine("mysql+mysqldb://USERNAME:"+'PASSWORD'+"@YOURHOST:PORT#/DB_NAME")

print('Uploading Binance symbols to DB...!')

df0.to_sql(name='Binance_BTCUSDT_1m', con=engine, if_exists='append',index=True)
df1.to_sql(name='Binance_ETHUSDT_1m', con=engine, if_exists='append',index=True)
df2.to_sql(name='Binance_ETHBTC_1m', con=engine, if_exists='append',index=True)
df3.to_sql(name='Binance_XRPBTC_1m', con=engine, if_exists='append',index=True)
df4.to_sql(name='Binance_ADABTC_1m', con=engine, if_exists='append',index=True)
df5.to_sql(name='Binance_EOSBTC_1m', con=engine, if_exists='append',index=True)
df6.to_sql(name='Binance_TRXBTC_1m', con=engine, if_exists='append',index=True)
df7.to_sql(name='Binance_XLMBTC_1m', con=engine, if_exists='append',index=True)
df8.to_sql(name='Binance_LTCBTC_1m', con=engine, if_exists='append',index=True)

print('Uploading BitMex symbols to DB...!')

df9.to_sql(name='BitMex_XBTUSD_1m', con=engine, if_exists='append',index=True)
df10.to_sql(name='BitMex_ETHUSD_1m', con=engine, if_exists='append',index=True)

print('All caught up..!')