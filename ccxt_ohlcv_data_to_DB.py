 # https://backtest-rookies.com/2018/03/08/download-cryptocurrency-data-with-ccxt/

import ccxt
from datetime import datetime, timedelta, timezone
import math
import argparse
import pandas as pd
import sqlalchemy  
from sqlalchemy import create_engine
 
def parse_args():
    parser = argparse.ArgumentParser(description='CCXT Market Data Downloader')
 
 
    parser.add_argument('-s','--symbol',
                        type=str,
                        required=True,
                        help='The Symbol of the Instrument/Currency Pair To Download')
 
    parser.add_argument('-e','--exchange',
                        type=str,
                        required=True,
                        help='The exchange to download from')
 
    parser.add_argument('-t','--timeframe',
                        type=str,
                        default='1d',
                        choices=['1m', '5m','15m', '30m','1h', '2h', '3h', '4h', '6h', '12h', '1d', '1M', '1y'],
                        help='The timeframe to download')
 
 
    parser.add_argument('--debug',
                            action ='store_true',
                            help=('Print Sizer Debugs'))
 
    return parser.parse_args()
 
# Get our arguments
args = parse_args()
 
# Get our Exchange
try:
    exchange = getattr (ccxt, args.exchange) ()
except AttributeError:
    print('-'*36,' ERROR ','-'*35)
    print('Exchange "{}" not found. Please check the exchange is supported.'.format(args.exchange))
    print('-'*80)
    quit()
 
# Check if fetching of OHLC Data is supported
if exchange.has["fetchOHLCV"] != True:
    print('-'*36,' ERROR ','-'*35)
    print('{} does not support fetching OHLC data. Please use another exchange'.format(args.exchange))
    print('-'*80)
    quit()
 
# Check requested timeframe is available. If not return a helpful error.
if (not hasattr(exchange, 'timeframes')) or (args.timeframe not in exchange.timeframes):
    print('-'*36,' ERROR ','-'*35)
    print('The requested timeframe ({}) is not available from {}\n'.format(args.timeframe,args.exchange))
    print('Available timeframes are:')
    for key in exchange.timeframes.keys():
        print('  - ' + key)
    print('-'*80)
    quit()
 
# Check if the symbol is available on the Exchange
exchange.load_markets()
if args.symbol not in exchange.symbols:
    print('-'*36,' ERROR ','-'*35)
    print('The requested symbol ({}) is not available from {}\n'.format(args.symbol,args.exchange))
    print('Available symbols are:')
    for key in exchange.symbols:
        print('  - ' + key)
    print('-'*80)
    quit()
 
# Get data
data = exchange.fetch_ohlcv(args.symbol, args.timeframe)
header = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
df = pd.DataFrame(data, columns=header)
df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms')
df = df.set_index('Timestamp')

# Save it
print('Saving symbol O-H-L-C-V csv to DB...!')
symbol_out = args.symbol.replace("/","")
filename = '{}-{}-{}.csv'.format(args.exchange, symbol_out,args.timeframe)
filenameDB = '{}-{}-{}'.format(args.exchange, symbol_out,args.timeframe)
# df.to_csv(filename)

# Upload data to SQL DB
# https://kanoki.org/2019/01/12/how-to-merge-multiple-csv-files-and-upload-to-mysql-or-postgressql-using-python/
engine = create_engine("mysql+mysqldb://USERNAME:"+'PASSWORD'+"@YOURHOST:PORT#/DB_NAME")

print('Uploading symbol to DB...!')

df.to_sql(name=filenameDB, con=engine, if_exists='append',index=True)

# Linux command line example: python ccxt_ohlcv_data_to_DB.py -s NEO/ETH -e binance -t 1d