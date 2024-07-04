import requests , json, hashlib,hmac
import time
import datetime
import pandas as pd
import os
import glob2
import numpy as np

BINANCE_API = 'https://api.binance.com'
BINANCE_FAPI = 'https://fapi.binance.com'
BINANCE_DAPI = 'https://dapi.binance.com'
API_KEY = API_KEY
API_SECRET = API_SECRET

test = requests.get("https://api.binance.com/api/v3/ping")

def sign(params):
    params['timestamp'] = int(time.time() * 1000)
    query = '&'.join([f"{k}={v}" for k, v in params.items()])
    m = hmac.new(API_SECRET.encode('utf-8'), query.encode('utf-8'), hashlib.sha256)
    query += f'&signature={m.hexdigest()}'
    return query

def make_signed_req_kwargs(endpoint, method, params):
    "Return dictionary as kwargs for requests.request(**kwargs)."
    return {
        'url': BINANCE_FAPI + endpoint,
        'method': method,
        'params': params,
        'headers': {"X-MBX-APIKEY": API_KEY},
    }

def make_signed_req_kwargs_spot(endpoint, method, params):
    "Return dictionary as kwargs for requests.request(**kwargs)."
    return {
        'url': BINANCE_API + endpoint,
        'method': method,
        'params': params,
        'headers': {"X-MBX-APIKEY": API_KEY},
    }

def make_signed_req_kwargs_dapi(endpoint, method, params):
    "Return dictionary as kwargs for requests.request(**kwargs)."
    return {
        'url': BINANCE_DAPI + endpoint,
        'method': method,
        'params': params,
        'headers': {"X-MBX-APIKEY": API_KEY},
    }

def get(endpoint, params):
    kwargs = make_signed_req_kwargs(endpoint, 'GET', params)
    print(kwargs)
    return requests.request(**kwargs).json()

def get_spot(endpoint, params):
    kwargs = make_signed_req_kwargs_spot(endpoint, 'GET', params)
    print(kwargs)
    return requests.request(**kwargs).json()

def get_dapi(endpoint, params):
    kwargs = make_signed_req_kwargs_dapi(endpoint, 'GET', params)
    print(kwargs)
    return requests.request(**kwargs).json()

df = pd.read_csv(r'G:\Algo_trade\Algo_Log\Crypto\symbol_list.csv')
symbol_list = list(df['symbol'])
required_list = []
for symbol in symbol_list:
    located = symbol.find('USDT')
    if located != -1:
        required_list.append(symbol)
    else:
        'Do nothing'

#Section to call historcial Data
# Data_date = '2021-8-20'
#eDate = '2021-09-01 00:00:00'
# start_date = datetime.datetime.strptime(Data_date, '%Y-%m-%d')
#start_date = datetime.datetime.strptime(eDate, '%Y-%m-%d %H:%M:%S')
#end_date = start_date + datetime.timedelta(hours = 528)
# # end_date = datetime.datetime.strptime(eDate, '%Y-%m-%d')
#start_time = int(start_date.timestamp() * 1000)
#end_time = int(end_date.timestamp() * 1000)
#
# params = {'symbol' : 'TRXUSDT',
#           'interval' : '1h',
#           'startTime' : start_time,
#           'endTime' : end_time,}
#
# get_data = get('/api/v3/klines',params)
# klines_col = ['Open_Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close_Time', 'Quote_asset_volume', 'Number_of_trades', 'Taker_buy_base_asset_volume', 'Taker_buy_quote_asset_volume', 'Ignore']
# df = pd.DataFrame(get_data)
# df.columns = klines_col
# print(list(df['Open_Time'])[-1])
# df.to_csv(r'G:\Algo_trade\Crypto_data\TRXUSDT_6.csv', index = False)

# params = {'symbol' : 'TRXUSDT',
#           'period' : '1h',
#           'startTime' : start_time,
#           'endTime' : end_time,}

# params = {'symbol' : symbol,
#          'period' : '1h',
#         'startTime' : start_time,
#          'endTime' : end_time,}
def ADD_CONTRACT_TYPE(symbol_list , TYPE):
    temp_list = []
    for symbol in symbol_list:
        value = symbol + TYPE
        temp_list.append(value)
    return temp_list

def CALL_SPOT_DATA(symbol, S_Date, E_Date, period):
    #i.e. S_Date = '2021-09-01 00:00:00'
    start_date = datetime.datetime.strptime(S_Date, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(E_Date, '%Y-%m-%d %H:%M:%S')
    start_time = int(start_date.timestamp() * 1000)
    end_time = int(end_date.timestamp() * 1000)
    params = {'symbol' : symbol,
                'interval' : period,
                'startTime' : start_time,
                'endTime' : end_time,}
    get_data = get_spot('/api/v3/klines',params)
    klines_col = ['Open_Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close_Time', 'Quote_asset_volume', 'Number_of_trades', 'Taker_buy_base_asset_volume', 'Taker_buy_quote_asset_volume', 'Ignore']
    df = pd.DataFrame(get_data)
    df.columns = klines_col
    # df.to_csv(r'G:\Algo_trade\Crypto_data\Spot_Data\\' + symbol + '_' + S_Date.replace('-','').replace(' ', '').replace(':', '') +'_' + period + '.csv', index = False)
    return df

def CALL_OPEN_INEREST(symbol, S_Date, E_Date,period):
    #i.e E_Date = '2021-09-09 00:00:00'
    start_date = datetime.datetime.strptime(S_Date, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(E_Date, '%Y-%m-%d %H:%M:%S')
    start_time = int(start_date.timestamp() * 1000)
    end_time = int(end_date.timestamp() * 1000)
    params = {'symbol': symbol,
              'period': period,
              'startTime': start_time,
              'endTime': end_time, }
    get_fu_openinterest = get('/futures/data/openInterestHist', params)
    df = pd.DataFrame(get_fu_openinterest)
    # df.to_csv(r'G:\Algo_trade\Crypto_data\Open_Interest_Data\\' + symbol + '_' + S_Date.replace('-','').replace(' ', '').replace(':', '') + '_' + period + '_OpenInterest.csv', index=False)
    return df

def CALL_FUTURE_SYMBOL():
    get_future_symbol = get('/fapi/v1/ticker/price')
    get_future_symbol.to_csv(r'G:\Algo_trade\Crypto_data\FIles\Future_symbol_list_20220326.csv', index = False)
# get('/api/v3/historicalTrades')

def CALL_FUNDING_RATE(symbol, S_Date, E_Date):
    #i.e E_Date = '2021-09-09 00:00:00'
    start_date = datetime.datetime.strptime(S_Date, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(E_Date, '%Y-%m-%d %H:%M:%S')
    start_time = int(start_date.timestamp() * 1000)
    end_time = int(end_date.timestamp() * 1000)
    params = {'symbol': symbol,
              'startTime': start_time,
              'endTime': end_time, }
    get_funding_rate = get('/fapi/v1/fundingRate', params)
    get_funding_rate_df = pd.DataFrame(get_funding_rate)
    get_funding_rate_df.to_csv(r'G:\Algo_trade\Crypto_data\Funding_Rate_Data\\' + symbol + '_' + S_Date.replace('-','').replace(' ', '').replace(':', '') +'_Funding_rate.csv', index=False)

def CALL_TOP_LS_RATIO_ACCT(symbol, S_Date, E_Date, period):
    start_date = datetime.datetime.strptime(S_Date, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(E_Date, '%Y-%m-%d %H:%M:%S')
    start_time = int(start_date.timestamp() * 1000)
    end_time = int(end_date.timestamp() * 1000)
    params = {'symbol': symbol,
              'period': period,
              'startTime': start_time,
              'endTime': end_time, }
    # params = {'symbol': symbol}
    get_top_lsrationacct = get('/futures/data/topLongShortAccountRatio', params)
    df = pd.DataFrame(get_top_lsrationacct)
    # df.to_csv(r'G:\Algo_trade\Crypto_data\Long_Short_Data\\' + symbol + '_' + S_Date.replace('-','').replace(' ', '').replace(':', '') + '_' + period + '_Top_LS_RATIO_ACCT.csv', index=False)
    return df

def CALL_TOP_LS_RATIO_POS(symbol, S_Date, E_Date, period):
    start_date = datetime.datetime.strptime(S_Date, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(E_Date, '%Y-%m-%d %H:%M:%S')
    start_time = int(start_date.timestamp() * 1000)
    end_time = int(end_date.timestamp() * 1000)
    params = {'symbol': symbol,
              'period': period,
              'startTime': start_time,
              'endTime': end_time, }
    # params = {'symbol': symbol}
    get_top_lsrationacct = get('/futures/data/topLongShortPositionRatio', params)
    df = pd.DataFrame(get_top_lsrationacct)
    # df.to_csv(r'G:\Algo_trade\Crypto_data\Long_Short_Data\\' + symbol + '_' + S_Date.replace('-','').replace(' ', '').replace(':', '') + '_' + period + '_Top_LS_RATIO_POS.csv',index=False)
    return df

def CALL_LS_RATIO(symbol, S_Date, E_Date, period):
    start_date = datetime.datetime.strptime(S_Date, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(E_Date, '%Y-%m-%d %H:%M:%S')
    start_time = int(start_date.timestamp() * 1000)
    end_time = int(end_date.timestamp() * 1000)
    params = {'symbol': symbol,
              'period': period,
              'startTime': start_time,
              'endTime': end_time, }
    # params = {'symbol': symbol}
    get_top_lsrationacct = get('/futures/data/globalLongShortAccountRatio', params)
    df = pd.DataFrame(get_top_lsrationacct)
    # df.to_csv(r'G:\Algo_trade\Crypto_data\Long_Short_Data\\' + symbol + '_' + S_Date.replace('-','').replace(' ', '').replace(':', '') + '_' + period + '_LS_RATIO.csv',index=False)
    return df

def CALL_TAKER_BS_VOLUME(symbol, S_Date, E_Date, period):
    start_date = datetime.datetime.strptime(S_Date, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(E_Date, '%Y-%m-%d %H:%M:%S')
    start_time = int(start_date.timestamp() * 1000)
    end_time = int(end_date.timestamp() * 1000)
    params = {'symbol': symbol,
              'period': period,
              'startTime': start_time,
              'endTime': end_time, }
    # params = {'symbol': symbol}
    get_Taker_BS_vol = get('/futures/data/takerlongshortRatio', params)
    get_Taker_BS_vol_df = pd.DataFrame(get_Taker_BS_vol)
    get_Taker_BS_vol_df['symbol'] = symbol
    # get_Taker_BS_vol_df.to_csv(r'G:\Algo_trade\Crypto_data\Taker_BuySell_Data\\' + symbol + '_' + S_Date.replace('-','').replace(' ', '').replace(':', '') + '_' + period + '_Taker_BS.csv', index= False )
    return get_Taker_BS_vol_df

def CALL_MARK_PRICE(symbol, S_Date, E_Date, Interval):
    # Not ready
    start_date = datetime.datetime.strptime(S_Date, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(E_Date, '%Y-%m-%d %H:%M:%S')
    start_time = int(start_date.timestamp() * 1000)
    end_time = int(end_date.timestamp() * 1000)
    params = {'symbol' : symbol,
              'interval' : Interval,
              'startTime' : start_time,
              'endTime' : end_time}
    try:
        get_MARK_PRICE_DF = get_dapi(r'/dapi/v1/markPriceKlines', params)
        Mark_Price_DF = pd.DataFrame(np.array(get_MARK_PRICE_DF), columns = ['Open_Time', 'Open', 'High', 'Low', 'Close', 'Ignore', ' Close_Time', 'Ignore_1', 'number_of_basic_data', 'Ignore_2', 'Ignore_3', 'Ignore_4'])
        Mark_Price_DF['Symbol'] = symbol
        Mark_Price_DF.to_csv(r'G:\Algo_trade\Crypto_data\Mark_Price_Data\\' + symbol + '_' + S_Date.replace('-','').replace(' ','').replace(':', '') + '_' + Interval + '_MarkPrice.csv', index = False )
    except:
        print('No such contract')
    return

def CALL_BASIS_DATA():
    # symbol, contractType, period, S_Date, E_Date
    # 'BTCUSD', 'ALL', '1h', '2022-03-01 00:00:00', '2022-03-28 00:00:00'
    symbol = 'BTCUSD'
    contractType = 'ALL'
    period = '1h'
    S_Date = '2022-03-01 00:00:00'
    E_Date = '2022-03-28 00:00:00'
    start_date = datetime.datetime.strptime(S_Date, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(E_Date, '%Y-%m-%d %H:%M:%S')
    start_time = int(start_date.timestamp() * 1000)
    end_time = int(end_date.timestamp() * 1000)
    params = {'symbol': symbol,
              'contractType' : contractType,
              'period': period,
              'startTime': start_time,
              'endTime': end_time}
    get_CALL_BASIS = get(r'/futures/data/basis', params)
    get_CALL_BASIS_DF = pd.DataFrame(get_CALL_BASIS)
    get_CALL_BASIS_DF.to_csv(r'G:\Algo_trade\Crypto_data\BASIS_DATA\\' + symbol + '_' + contractType + '_' + S_Date.replace('-','').replace(' ', '').replace(':', '') + period + '_basis.csv', index = False)

def CALL_TICKER_PRICE_CHANGE():
    # Not Ready
    params = {'pair' : 'SOLUSDT'}
    get_TICKER_PRICE_CHANGE_DF = get('/dapi/v1/ticker/24hr', params)
    TICKER_PRICE_DF = pd.DataFrame(get_TICKER_PRICE_CHANGE_DF)
    TICKER_PRICE_DF.to_csv(r'G:\Algo_trade\Crypto_data\TICKERPRICE.csv')
    return

def main(Target_list):
    Target_list = Target_list
    # Create spring function to capture data from Binance API
    lastest_file = glob2.glob(r'G:\Algo_trade\Crypto_data\Spot_Data\\*.csv')[-1]
    # Get the DateTime from the latest file
    datetime_str = os.path.basename(lastest_file)[8:-11]
    date_str = datetime_str[:4] + '-' + datetime_str[4:6] + '-' + datetime_str[6:8]
    time_str = datetime_str[8:]
    today = datetime.datetime.today()
    last_data_date = date_str + ' ' + time_str + ':00:00'
    # last_data_date = '2023-03-25 00:00:00'
    last_data_date = datetime.datetime.strptime(last_data_date, '%Y-%m-%d %H:%M:%S')
    time_delta = today - last_data_date
    hr_diff = int(time_delta.total_seconds()//3600)
    # hr_diff = 26
    if hr_diff > 0 :
        for i in range(1, hr_diff + 1):
            DateTime = last_data_date + datetime.timedelta(hours=i)
            DateTime = DateTime.strftime('%Y-%m-%d %H:%M:%S')
            for symbol in Target_list:
                Start_Date = DateTime
                End_Date = DateTime
                period = '1h'
                TOP_LS_RATIO_ACCT_DF = CALL_TOP_LS_RATIO_ACCT(symbol, Start_Date, End_Date, period)
                TOP_LS_RATIO_ACCT_DF.to_csv(r'G:\Algo_trade\Crypto_data\Long_Short_Data\\' + symbol + '_' + Start_Date.replace('-', '').replace(' ','').replace(':', '') + '_' + period + '_Top_LS_RATIO_ACCT.csv', index=False)
                TOP_LS_RATIO_POS_DF = CALL_TOP_LS_RATIO_POS(symbol, Start_Date, End_Date, period)
                TOP_LS_RATIO_POS_DF.to_csv(r'G:\Algo_trade\Crypto_data\Long_Short_Data\\' + symbol + '_' + Start_Date.replace('-', '').replace(' ','').replace(':', '') + '_' + period + '_Top_LS_RATIO_POS.csv', index=False)
                LS_RATIO_DF = CALL_LS_RATIO(symbol, Start_Date, End_Date, period)
                LS_RATIO_DF.to_csv(r'G:\Algo_trade\Crypto_data\Long_Short_Data\\' + symbol + '_' + Start_Date.replace('-', '').replace(' ','').replace(':', '') + '_' + period + '_LS_RATIO.csv', index=False)
                OPEN_INTEREST_DF = CALL_OPEN_INEREST(symbol, Start_Date, End_Date, period)
                OPEN_INTEREST_DF.to_csv(r'G:\Algo_trade\Crypto_data\Open_Interest_Data\\' + symbol + '_' + Start_Date.replace('-', '').replace(' ', '').replace(':', '') + '_' + period + '_OpenInterest.csv', index=False)
                SPOT_DATA_DF = CALL_SPOT_DATA(symbol, Start_Date, End_Date, period)
                SPOT_DATA_DF.to_csv(r'G:\Algo_trade\Crypto_data\Spot_Data\\' + symbol + '_' + Start_Date.replace('-', '').replace(' ','').replace(':', '') + '_' + period + '.csv', index=False)
                TAKER_BS_VOLUME_DF = CALL_TAKER_BS_VOLUME(symbol, Start_Date, End_Date, period)
                TAKER_BS_VOLUME_DF.to_csv(r'G:\Algo_trade\Crypto_data\Taker_BuySell_Data\\' + symbol + '_' + Start_Date.replace('-', '').replace(' ', '').replace(':', '') + '_' + period + '_Taker_BS.csv', index=False)
                time.sleep(0.1)
    else:
        "Data Up to Date"
    return

def main_min_record(Target_list):
    return


if __name__ == "__main__":

    Target_list = ['AVAXUSDT', 'MANAUSDT', 'RAYUSDT','SOLUSDT', 'TRXUSDT','BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'NEOUSDT', 'LTCUSDT', 'QTUMUSDT', 'ADAUSDT', 'XRPUSDT', 'EOSUSDT', 'IOTAUSDT', 'XLMUSDT', 'ONTUSDT', 'ETCUSDT','VETUSDT', 'DOTUSDT']
    main(Target_list)
