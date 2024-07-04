from futu import *
import pandas as pd
import time
import datetime
import sqlalchemy
import logging

log_date = datetime.datetime.today().date()
logging.basicConfig(filename = r'E:\Schedule_tasks\Log\\' + str(log_date) + 'FUTU_API_DATA.log', level = logging.DEBUG)
logger = logging.getLogger(__name__)


def READ_PI_DB(query):
    user_name = user_name
    password = password
    db = table
    connection_string = f'mysql+pymysql://{user_name}:{password}@{ip}/{db}'
    engine = sqlalchemy.create_engine(connection_string)

    connection = engine.connect()
    DF = pd.read_sql(query, con = engine)
    return DF

def LOAD_PI_ALGO_DB(DF, TABLE):
    user_name = user_name
    password = password
    db = table
    connection_string = f'mysql+pymysql://{user_name}:{password}@{ip}/{db}'
    engine = sqlalchemy.create_engine(connection_string)
    connection = engine.connect()
    DF.to_sql(name=TABLE, con=engine, if_exists='append', index=False)
    return DF

def LOAD_PI_ALGO_DB_REPLACE(DF, TABLE):
    user_name = user_name
    password = password
    db = table
    connection_string = f'mysql+pymysql://{user_name}:{password}@{ip}/{db}'
    engine = sqlalchemy.create_engine(connection_string)
    connection = engine.connect()
    DF.to_sql(name=TABLE, con=engine, if_exists='replace', index=False)
    return DF


def CALL_ALL_STOCK_DATA():
    # to collect All stock data
    Plate_List_DF = pd.read_csv(file_path)
    Plate_Code_List = list(Plate_List_DF['code'])

    US_Stock_List = []

    for plate_code in Plate_Code_List:
        quote_ctx = OpenQuoteContext(host = ip, port = 11111, is_encrypt= None)
        ret, data = quote_ctx.get_plate_stock(plate_code)
        if ret == RET_OK:
            print(data)
            US_Stock_List.append(data)
        else:
            print('error -', data)
            quote_ctx.close()
            print('wait 30s and resume scrapping')
            time.sleep(30)
            print('after 30s programe resume')
            quote_ctx = OpenQuoteContext(host=ip, port=11111, is_encrypt=None)
            ret, data = quote_ctx.get_plate_stock(plate_code)
            print(data)
            US_Stock_List.append(data)
        quote_ctx.close()
    US_Stock_Full_DF = pd.concat(US_Stock_List)
    US_Stock_Full_DF.to_csv(r'G:\Algo_trade\FUTU_Data\FUTU_US_STOCK_FULL_DF.csv', index = False)
    return

def CALL_ALL_STOCK_DATA_HK():
    # to collect All stock data
    Plate_List_DF = pd.read_csv(r'G:\Algo_trade\FUTU_Data\FUTU_Plate_Record_HK.csv')
    Plate_Code_List = list(Plate_List_DF['code'])

    HK_Stock_List = []

    for plate_code in Plate_Code_List:
        quote_ctx = OpenQuoteContext(host = ip, port = 11111, is_encrypt= None)
        ret, data = quote_ctx.get_plate_stock(plate_code)
        if ret == RET_OK:
            print(data)
            HK_Stock_List.append(data)
        else:
            print('error -', data)
            quote_ctx.close()
            print('wait 30s and resume scrapping')
            time.sleep(30)
            print('after 30s programe resume')
            quote_ctx = OpenQuoteContext(host=ip, port=11111, is_encrypt=None)
            ret, data = quote_ctx.get_plate_stock(plate_code)
            print(data)
            HK_Stock_List.append(data)
        quote_ctx.close()
    HK_Stock_Full_DF = pd.concat(HK_Stock_List)
    HK_Stock_Full_DF.to_csv(r'G:\Algo_trade\FUTU_Data\FUTU_HK_STOCK_FULL_DF.csv', index = False)
    return

def CALL_ALL_PLATE_DATA():
    # get plate list
    quote_ctx = OpenQuoteContext(host=ip, port=11111)
    ret, data = quote_ctx.get_owner_plate(Market.HK, Plate.ALL)
    # ret, data = quote_ctx.get_plate_list(Market.HK, Plate.ALL)
    # ret, data = quote_ctx.get_plate_list(Market.US, Plate.ALL)
    if ret == RET_OK:
        print(data)
        data.to_csv(r'G:\Algo_trade\FUTU_Data\FUTU_Plate_Record_HK.csv', index = False)
    else:
        print('error:', data)
    quote_ctx.close()


# to get all option chain
def CALL_OPTION_CHAIN_DATA():
    Stock_List_DF = pd.read_csv(r'G:\Algo_trade\FUTU_Data\FUTU_US_STOCK_FULL_DF.csv')
    Stock_Code_List = list(Stock_List_DF['code'])
    for Stock_Code in Stock_Code_List:
        quote_ctx = OpenQuoteContext(host=ip, port=11111)
        ret1, data1 = quote_ctx.get_option_expiration_date(code=Stock_Code)
        if ret1 == RET_OK:
            print(Stock_Code)
            if data1.shape[0] != 0 :
                print(data1)
                expiration_date_list = data1['strike_time'].values.tolist()
                Stock_Option_Chain_DF_List = []
                for date in expiration_date_list:
                    ret2, data2 = quote_ctx.get_option_chain(code=Stock_Code, start=date, end=date)
                    if ret2 == RET_OK:
                        print(data2)
                        Stock_Option_Chain_DF_List.append(data2)
                    else:
                        print('error:', data2)
                        print('wait 30s and resume scrapping')
                        time.sleep(30)
                        print('after 30s programe resume')
                        ret2, data2 = quote_ctx.get_option_chain(code=Stock_Code, start=date, end=date)
                        print(data2)
                        Stock_Option_Chain_DF_List.append(data2)
                Stock_Option_Chain_DF = pd.concat(Stock_Option_Chain_DF_List)
                Stock_Option_Chain_DF.to_csv(r'G:\Algo_trade\FUTU_Data\FUTU_Stock_Option_Chain_' + Stock_Code + '.csv', index = False)
            else:
                print(Stock_Code, ' no option chain and go to next symbol')
        else:
            print('error while checking expiry date chain:', data1)
            print('wait 30s and resume scrapping')
            time.sleep(30)
            print('after 30s programe resume')
            ret1, data1 = quote_ctx.get_option_expiration_date(code=Stock_Code)
            print(Stock_Code)
            if data1.shape[0] != 0 :
                print(data1)
                expiration_date_list = data1['strike_time'].values.tolist()
                Stock_Option_Chain_DF_List = []
                for date in expiration_date_list:
                    ret2, data2 = quote_ctx.get_option_chain(code=Stock_Code, start=date, end=date)
                    if ret2 == RET_OK:
                        print(data2)
                        Stock_Option_Chain_DF_List.append(data2)
                    else:
                        print('error:', data2)
                        print('wait 30s and resume scrapping')
                        time.sleep(30)
                        print('after 30s programe resume')
                        ret2, data2 = quote_ctx.get_option_chain(code=Stock_Code, start=date, end=date)
                        print(data2)
                        Stock_Option_Chain_DF_List.append(data2)
                Stock_Option_Chain_DF = pd.concat(Stock_Option_Chain_DF_List)
                Stock_Option_Chain_DF.to_csv(r'G:\Algo_trade\FUTU_Data\FUTU_Stock_Option_Chain_' + Stock_Code + '.csv', index = False)
            else:
                print(Stock_Code, ' no option chain and go to next symbol')
    quote_ctx.close()
    return


def CALL_OPTION_CHAIN_DATA_HK(Stock_Code, Expiration_Date_List):
    Stock_Option_Chain_DF_List = []
    quote_ctx = OpenQuoteContext(host=ip, port=11111)
    for date in Expiration_Date_List:
        ret, data = quote_ctx.get_option_chain(code=Stock_Code, start=date, end=date)
        if ret == RET_OK:
            Stock_Option_Chain_DF_List.append(data)
        else:
            print('error:', data)
            print('wait 30s and resume scrapping')
            time.sleep(30)
            print('after 30s programe resume')
            ret, data = quote_ctx.get_option_chain(code=Stock_Code, start=date, end=date)
            Stock_Option_Chain_DF_List.append(data)
    quote_ctx.close()
    Stock_Option_Chain_DF = pd.concat(Stock_Option_Chain_DF_List)
    Stock_Option_Chain_DF.to_csv(r'G:\Algo_trade\FUTU_Data\FUTU_Option_Data_HK\FUTU_Stock_Option_Chain_' + Stock_Code + '.csv', index=False)
    return

def STOCK_HISTORY_KLINE(stock_symbol, start_date , end_date):
    quote_ctx = OpenQuoteContext(host=ip, port=11111)
    ret, data, page_req_key = quote_ctx.request_history_kline(stock_symbol, start = start_date, end = end_date, max_count = 5)
    if ret == RET_OK:
        print(data)
        print(data['code'][0])
    else:
        print('error:', data)
    quote_ctx.close()
    return

def HK_STOCK_HISTORY_KLINE(stock_symbol, start_date , end_date):
    quote_ctx = OpenQuoteContext(host=ip, port=11111)
    ret, data, page_req_key = quote_ctx.request_history_kline(stock_symbol, start = start_date, end = end_date, max_count = 1000)
    HK_STOCK_HISTORY_KLINE = []
    if ret == RET_OK and not data.empty:
        print(data)
        print(data['code'][0])
        HK_STOCK_HISTORY_KLINE.append(data)
    else:
        print('error:', data)
        print('wait 30s and resume scrapping')
        time.sleep(30)
        print('after 30s programe resume')
        ret2, data2, page_req_key = quote_ctx.request_history_kline(stock_symbol, start=start_date, end=end_date, max_count=1000)
        HK_STOCK_HISTORY_KLINE.append(data2)
    while page_req_key != None:
        ret, data, page_req_key = quote_ctx.request_history_kline(stock_symbol, start=start_date, end=end_date, max_count=1000, page_req_key = page_req_key)
        if ret == RET_OK:
            HK_STOCK_HISTORY_KLINE.append(data)
        else:
            print('error in taking next page data')
    quote_ctx.close()
    HK_Stock_DF = pd.concat(HK_STOCK_HISTORY_KLINE)
    HK_Stock_DF.to_csv(r'G:\Algo_trade\FUTU_Data\FUTU_Spot_Data\\' + stock_symbol + '_' + start_date + '_' + end_date + '.csv', index = False)
    return

def CHECK_API_USAGE():
    quote_ctx = OpenQuoteContext(host=ip, port=11111)
    quote_ctx.subscribe(['HK.00700'], [SubType.QUOTE])
    ret, data = quote_ctx.query_subscription()
    if ret == RET_OK:
        print(data)
    else:
        print('error:', data)
    quote_ctx.close()
    return
#Should relied on the function from this point and beyond
#refined script on 3 Feb 2024 for replacing yfinance datasource
#Breakdown the scripts into difference function
def CHECK_OPTION_EXPIRATION_DATES(Code):
    quote_ctx = OpenQuoteContext(host=ip, port=11111)
    # To Get the expiration date list of that stock
    ret, data = quote_ctx.get_option_expiration_date(code=Code)
    try:
        if ret == RET_OK:
            data['strike_time'] = pd.to_datetime(data['strike_time'])
            # Tag the required column
            data.loc[0, 'expiration_cycle'] = 'T'
            # Loop thru the df to locate the expiration date we need to check
            for i in range(1, len(data)):
                if (data.loc[i, 'strike_time'] - data.loc[i - 1, 'strike_time']).days >= 30:
                    data.loc[i, 'expiration_cycle'] = 'T'
                else:
                    data.loc[i, 'expiration_cycle'] = 'F'
            data['Stock_Code'] = Code
            data['Query_Date'] = datetime.datetime.today().date()
            data['Query_Time'] = datetime.datetime.today().time()
            data['strike_time'] = data['strike_time'].dt.date
            data['strike_time'] = data['strike_time'].astype(str)
            expiration_df = data
        else:
            print('error:', data)
        quote_ctx.close()
    except:
        print('error')
        quote_ctx.close()
        expiration_df = data
    return expiration_df

def CHECK_CODE_LIST(df):
    #df = expiration check list df
    Expiration_Date_List = list(df.loc[df['expiration_cycle'] == 'T', 'strike_time'])
    Code = list(df['Stock_Code'])[0]
    DF_LIST = []
    quote_ctx = OpenQuoteContext(host=ip, port=11111)

    for expiration_date in Expiration_Date_List:
        print(expiration_date)
        ret , data = quote_ctx.get_option_chain(code = Code, start = expiration_date)
        if ret == RET_OK:
            DF_LIST.append(data)
        else:
            print('error:', data)
    CODE_DF = pd.concat(DF_LIST)
    CODE_DF['Query_Date'] = datetime.datetime.today().date()
    CODE_DF['Query_Time'] = datetime.datetime.today().time()
    quote_ctx.close()
    return CODE_DF

def LOAD_FUTU_OPTION_CHAIN_DATA(CODE_DF):
    Code_list = list(CODE_DF['code'])

    grouped_data = []
    api_limit_tag = 0
    # add limit tag to prevent exceed the api limit
    for i in range(0, len(Code_list), 400):
        group = Code_list[i:i + 400]
        grouped_data.append(group)
        if len(grouped_data) % 60 == 0:
            api_limit_tag += 1

    # Create a DataFrame with the grouped data and API limit tags
    df = pd.DataFrame(columns=['group', 'code', 'api_limit_tag'])

    for i, group in enumerate(grouped_data):
        df = df.append({'group': i + 1, 'code': group, 'api_limit_tag': api_limit_tag}, ignore_index=True)

        if (i + 1) % 60 == 0:
            api_limit_tag += 1

    Snapshot_DF_LIST = []
    # Print the DataFrame
    quote_ctx = OpenQuoteContext(host=ip, port=11111)
    for group in list(df['group']):
        code_list = list(df.loc[df['group'] == group, 'code'])[0]
        api_limit_tag = list(df.loc[df['group'] == group, 'api_limit_tag'])[0]

        ret, data = quote_ctx.get_market_snapshot(code_list)

        if ret == RET_OK:
            if api_limit_tag == 0:
                data['Query_Date'] = datetime.datetime.today().date()
                data['Query_Time'] = datetime.datetime.today().time()
                print('Loading FUTU data into DB :', group)
                LOAD_PI_ALGO_DB(data, 'FUTU_SNAPSHOT_TABLE')
                print('Done')
                # data.to_csv(r'G:\Algo_trade\FUTU_Data\Testing\NVDA_snapshot.csv', index = False)
            elif api_limit_tag == 1:
                print("Reach API limit")
                data['Query_Date'] = datetime.datetime.today().date()
                data['Query_Time'] = datetime.datetime.today().time()
                print('Loading FUTU data into DB :', group)
                LOAD_PI_ALGO_DB(data, 'FUTU_SNAPSHOT_TABLE')
                print('Done')
                print('wait api recover from api limit')
                time.sleep(30)
                print('resume')
        else:
            print('error:', data)
            print('In error loop')

    quote_ctx.close()


def UPDATE_FUTU_STATIS_US_STOCK_TABLE():
    quote_ctx = OpenQuoteContext(host=ip, port=11111)
    Stock_Plate = 'US.USAALL'
    ret, data = quote_ctx.get_plate_stock(Stock_Plate)
    if ret == RET_OK:
        print(data)
        LOAD_PI_ALGO_DB_REPLACE(data, 'FUTU_STATIC_US_STOCK_LIST')
    else:
        print('error:', data)
    quote_ctx.close()
    return

def CHECK_TRADING_DAYS(start_date,end_date):
    #start_date = '2024-01-01'
    quote_ctx = OpenQuoteContext(host=ip, port=11111)

    ret, data = quote_ctx.request_trading_days(market=TradeDateMarket.US, start= start_date, end= end_date)
    if ret == RET_OK:
        print('US market calendar:', data)
        df = pd.DataFrame(data)
        df['Market'] = 'US'
    else:
        print('error:', data)
    quote_ctx.close()
    return df

if __name__ == "__main__":
    # Procedure check the option chain first
    #change the workflow for multi stock code
    logger.debug('RUNNING_FUTU_API_DATA')
    weekday_list = [1,2,3,4,5]

    if datetime.datetime.today().weekday() in weekday_list:
        Code = 'US.NVDA'
        Stock_LIST = ['US.NVDA','US.AAPL', 'US.MSFT', 'US.AMZN', 'US.TSLA', 'US.AMD', 'US.SPY','US.IBM','US.TSM', 'US.TREX', 'US.PANW', 'US.NET', 'US.MKSI', 'US.ITA', 'US.HUBS','US.AVGO','US.GOOG','US.GOOGL','US.COIN']
        for Code in Stock_LIST:
            logger.debug('GETTING ' + Code)
            df = CHECK_OPTION_EXPIRATION_DATES(Code)
            CODE_DF = CHECK_CODE_LIST(df)
            logger.debug('API_Break')
            time.sleep(30)
            logger.debug('API_Break resume')
            LOAD_FUTU_OPTION_CHAIN_DATA(CODE_DF)
            time.sleep(30)
    else:
        logger.debug("it is Sunday/monday")
        time.sleep(5)
        "Do Nothing"
