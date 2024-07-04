import datetime
import pandas as pd
import sqlalchemy
import numpy as np
import matplotlib.pyplot as plt
import mplcursors
import streamlit as st
from mpl_toolkits.mplot3d import Axes3D


def READ_PI_DB(query):
    user_name = user_name
    password = password
    db = table
    connection_string = f'mysql+pymysql://{user_name}:{password}@{ip}/{db}'
    engine = sqlalchemy.create_engine(connection_string)

    connection = engine.connect()
    DF = pd.read_sql(query, con=engine)
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


def LOAD_STOCK_DATA(DF):
    user_name = user_name
    password = password
    db = table
    connection_string = f'mysql+pymysql://{user_name}:{password}@{ip}/{db}'
    engine = sqlalchemy.create_engine(connection_string)
    DF.to_sql(name='US_STOCK', con=engine, if_exists='replace', index=False)
    print("Uploaded US Stock data")
    return


# Define function to adjust report date based on day of week
def adjust_report_date(date):
    if date.weekday() in [1, 2, 3]:  # Tuesday to Friday
        return (date - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    else:  # Monday, Saturday, Sunday
        days_to_friday = (4 - date.weekday()) % 7
        return (date - datetime.timedelta(days=days_to_friday)).strftime('%Y-%m-%d')


def us_report_date(date):
    return date.strftime('%Y-%m-%d')


def reading_yahoo_data():
    query = """Select * from PROC_US_OPTION where underlying_ticker = 'NVDA' """

    DF = READ_PI_DB(query)
    # DF.to_csv(r'G:\Algo_trade\yfinance_Data\20231216_NVDA_OPTION.csv', index = False)
    # The Dataset group
    # First Level - by option type (Call / Put) level to group Open Interest
    grouped_sum = DF.groupby(['underlying_ticker', 'option_type', 'Trade_Date'])['openInterest'].sum()

    # Merge the summed values back to the original DataFrame
    merged_DF = pd.merge(DF, grouped_sum, on=['underlying_ticker', 'option_type', 'Trade_Date'],
                         suffixes=('', '_sum'))

    # Calculate the relative value by dividing openInterest by the sum of openInterest
    merged_DF['OI_RV_L1'] = merged_DF['openInterest'] / merged_DF['openInterest_sum']

    # Print the weighted average

    # Second Level - by expiry to weight Open interest
    grouped_L2_sum = DF.groupby(['underlying_ticker', 'option_type', 'Trade_Date', 'expiry_datetime'])[
        'openInterest'].sum()
    merged_DF = pd.merge(merged_DF, grouped_L2_sum,
                         on=['underlying_ticker', 'option_type', 'Trade_Date', 'expiry_datetime'],
                         suffixes=('', '_sum_L2'))
    merged_DF['OI_RV_L2'] = merged_DF['openInterest'] / merged_DF['openInterest_sum_L2']
    # merged_DF.to_csv(r'G:\Algo_trade\yfinance_Data\merged_DF.csv', index = False)
    merged_DF['OI_RV_L1_Hightest'] = merged_DF.groupby(['Trade_Date', 'option_type'])['OI_RV_L1'].transform(max)
    merged_DF['OI_RV_L2_Hightest'] = merged_DF.groupby(['Trade_Date', 'option_type', 'expiry_datetime'])[
        'OI_RV_L2'].transform(max)
    # Add Highest L1 tag
    merged_DF['OI_RV_L1_Hightest_TAG'] = merged_DF['OI_RV_L1'] == merged_DF['OI_RV_L1_Hightest']
    merged_DF['OI_RV_L1_Hightest_TAG'] = merged_DF['OI_RV_L1_Hightest_TAG'].map({True: 'T', False: 'F'})
    # Add Highest L2 Tag
    merged_DF['OI_RV_L2_Hightest_TAG'] = merged_DF['OI_RV_L2'] == merged_DF['OI_RV_L2_Hightest']
    merged_DF['OI_RV_L2_Hightest_TAG'] = merged_DF['OI_RV_L2_Hightest_TAG'].map({True: 'T', False: 'F'})
    merged_DF['Trade_Date'] = merged_DF['Trade_Date'].astype(str)

    # merged_DF.to_csv(r'G:\Algo_trade\yfinance_Data\merged_DF.csv', index = False)

    Stock_query = """Select * from US_STOCK"""
    STOCK_DF = READ_PI_DB(Stock_query)
    STOCK_DF['Trade_Date'] = STOCK_DF['Date'].astype(str)
    STOCK_DF['Trade_Date'] = STOCK_DF['Trade_Date'].str[:10]

    STOCK_DF['Trade_Date'] = STOCK_DF['Trade_Date'].astype(str)
    output_DF = pd.merge(merged_DF, STOCK_DF, how='left', left_on=['underlying_ticker', 'Trade_Date'],
                         right_on=['Symbol', 'Trade_Date'])
    output_DF.to_csv(r'G:\Algo_trade\yfinance_Data\output_DF_revised.csv', index=False)
    return


if __name__ == '__main__':
    query = """Select `Query_Date`,	`Query_Time`,	`code`,	`name`,	`update_time`,	`last_price`,	`open_price`,	`high_price`,	`low_price`,	`prev_close_price`,	`volume`,	`turnover`,	`turnover_rate`,	`suspension`,	`listing_date`,	`lot_size`,	`price_spread`,	`stock_owner`,	`ask_price`,	`bid_price`,	`ask_vol`,	`bid_vol`,	`amplitude`,	`avg_price`,	`bid_ask_ratio`,	`volume_ratio`,	`highest52weeks_price`,	`lowest52weeks_price`,	`highest_history_price`,	`lowest_history_price`,	`close_price_5min`,	`after_volume`,	`after_turnover`,	`sec_status`,	`equity_valid`,	`option_valid`,	`option_type`,	`strike_time`,	`option_strike_price`,	`option_contract_size`,	`option_open_interest`,	`option_implied_volatility`,	`option_premium`,	`option_delta`,	`option_gamma`,	`option_vega`,	`option_theta`,	`option_rho`,	`option_net_open_interest`,	`option_expiry_date_distance`,	`option_contract_nominal_value`,`option_owner_lot_multiplier`,	`option_area_type`,	`option_contract_multiplier`,`index_option_type` from FUTU_SNAPSHOT_TABLE where stock_owner = 'US.NVDA' and sec_status = 'NORMAL' """
    trade_data = READ_PI_DB(query)
    print(trade_data)
    # Convert option_expiry_date_distance to timedelta
    trade_data['option_expiry_date_distance'] = pd.to_timedelta(trade_data['option_expiry_date_distance'], unit='D')
    # Filter the trade data for sec_status == 'NORMAL'
    # filtered_trade_data = trade_data[trade_data['sec_status'] == 'NORMAL']

    trade_data['trade_date'] = trade_data['strike_time'] - trade_data['option_expiry_date_distance']

    # First Level - by option type (Call / Put) level to group Open Interest
    grouped_sum = trade_data.groupby(['code', 'option_type', 'trade_date'])['option_open_interest'].sum()

    # Merge the summed values back to the original DataFrame
    merged_DF = pd.merge(trade_data, grouped_sum, on=['code', 'option_type', 'trade_date'], suffixes=('', '_sum'))

    # Calculate the relative value by dividing option_open_interest by the sum of option_open_interest
    merged_DF['OI_RV_L1'] = merged_DF['option_open_interest'] / merged_DF['option_open_interest_sum']

    # Print the weighted average

    # Second Level - by expiry to weight Open interest
    grouped_L2_sum = trade_data.groupby(['code', 'option_type', 'trade_date', 'strike_time'])[
        'option_open_interest'].sum()
    merged_DF = pd.merge(merged_DF, grouped_L2_sum, on=['code', 'option_type', 'trade_date', 'strike_time'],
                         suffixes=('', '_sum_L2'))
    merged_DF['OI_RV_L2'] = merged_DF['option_open_interest'] / merged_DF['option_open_interest_sum_L2']
    # merged_DF.to_csv(r'G:\Algo_trade\yfinance_Data\merged_DF.csv', index = False)
    merged_DF['OI_RV_L1_Hightest'] = merged_DF.groupby(['trade_date', 'option_type'])['OI_RV_L1'].transform(max)
    merged_DF['OI_RV_L2_Hightest'] = merged_DF.groupby(['trade_date', 'option_type', 'strike_time'])[
        'OI_RV_L2'].transform(max)
    # Add Highest L1 tag
    merged_DF['OI_RV_L1_Hightest_TAG'] = merged_DF['OI_RV_L1'] == merged_DF['OI_RV_L1_Hightest']
    merged_DF['OI_RV_L1_Hightest_TAG'] = merged_DF['OI_RV_L1_Hightest_TAG'].map({True: 'T', False: 'F'})
    # Add Highest L2 Tag
    merged_DF['OI_RV_L2_Hightest_TAG'] = merged_DF['OI_RV_L2'] == merged_DF['OI_RV_L2_Hightest']
    merged_DF['OI_RV_L2_Hightest_TAG'] = merged_DF['OI_RV_L2_Hightest_TAG'].map({True: 'T', False: 'F'})
    merged_DF['trade_date'] = merged_DF['trade_date'].astype(str)

    # Create features
    # price diff
    merged_DF['option_premium_change'] = merged_DF.groupby('code')['option_premium'].diff()
    merged_DF['last_price_change'] = merged_DF.groupby('code')['last_price'].diff()
    merged_DF['option_open_interest_change'] = merged_DF.groupby('code')['option_open_interest'].diff()

    check_date = datetime.datetime(2024, 6, 11)
    merged_DF = merged_DF.astype('str')

    merged_DF = merged_DF[merged_DF['Query_Date'] == '2024-06-11']

    merged_DF = merged_DF[['Query_Date', 'option_strike_price', 'option_open_interest', 'strike_time', 'option_type',
                           'option_open_interest_change', 'option_premium_change', 'last_price_change', 'code']]
    merged_DF['option_strike_price'] = pd.to_numeric(merged_DF['option_strike_price'], errors='coerce')
    merged_DF['option_open_interest'] = pd.to_numeric(merged_DF['option_open_interest'], errors='coerce')
    merged_DF['option_open_interest_change'] = pd.to_numeric(merged_DF['option_open_interest_change'], errors='coerce')
    merged_DF['option_premium_change'] = pd.to_numeric(merged_DF['option_premium_change'], errors='coerce')
    merged_DF['last_price_change'] = pd.to_numeric(merged_DF['last_price_change'], errors='coerce')
    merged_DF['strike_time'] = pd.to_datetime(merged_DF['strike_time'])
    merged_DF['strike_time'] = merged_DF['strike_time'].apply(datetime.datetime.toordinal)

    merged_DF = merged_DF.fillna(0)

    CALL_Merged_DF = merged_DF[merged_DF['option_type'] == 'CALL']
    PUT_Merged_DF = merged_DF[merged_DF['option_type'] == 'PUT']
    print(CALL_Merged_DF)
    # merged_DF[merged_DF['option_open_interest_change']<= 0].to_csv(r'G:\Algo_trade\FUTU_Data\Testing\checking.csv', index = False)
    # Create 3D plot
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')

    # set width ,depth and height of the bars
    # width = 5
    # depth = 5

    # ax.bar3d(list(CALL_Merged_DF['option_strike_price']), list(CALL_Merged_DF['option_open_interest']), [0] * len(list(CALL_Merged_DF['option_strike_price'])), width, depth, list(CALL_Merged_DF['strike_time']), color = 'b')
    # ax.bar3d(list(PUT_Merged_DF['option_strike_price']), list(PUT_Merged_DF['option_open_interest']), [0] * len(list(PUT_Merged_DF['option_strike_price'])), width, depth, list(PUT_Merged_DF['strike_time']), color = 'r')

    ax.scatter(list(CALL_Merged_DF['option_strike_price']), list(CALL_Merged_DF['option_open_interest']),
               list(CALL_Merged_DF['strike_time']), color='b')
    ax.scatter(list(PUT_Merged_DF['option_strike_price']), list(PUT_Merged_DF['option_open_interest']),
               list(PUT_Merged_DF['strike_time']), color='r')

    # Set labels for each axis
    ax.set_xlabel('Strike')
    ax.set_ylabel('Open Interest Change')
    ax.set_zlabel('Expiry Date')

    plt.show()
