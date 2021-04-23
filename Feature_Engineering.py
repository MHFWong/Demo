# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 22:19:30 2021

@author: marti
"""

import pandas as pd
import numpy as np
from datetime import datetime

import seaborn as sns
import matplotlib.pyplot as plt

import talib as tb
#Import Data
#path = r'E:\python\USMK Data\SQ.csv'
#path = r'D:\Testing\GOOG.csv'
#Stock_Data = pd.read_csv(path, index_col = 'Date')
#Stock_Data = pd.read_csv(path)
#Stock_Data_Featured = Stock_Data.copy()
#Function
def BooleanConver(df,string):
    Boollist = []
    for i in df[string]:
        i = int(i == True)
        Boollist.append(i)
    return Boollist

def DTconvert(dt):
    dt = datetime.strptime(dt, '%Y-%m-%d')
    return dt

def Daychange(df, str1, str2):
    DayChangelist = [0]
    for i in range(1,len(df)):
        delta = df[str1][i] - df[str2][i-1]
        PerChange = delta/df[str2][i-1]
        DayChangelist.append(PerChange)
    return DayChangelist
#Plot Function
def correlation_of_Xy(dataset, title):
    f,ax = plt.subplots(figsize=(24,20))
    corr = dataset.corr()
    sns.heatmap(corr, cmap='coolwarm_r', annot=True, annot_kws={'size':10}, ax=ax)
    ax.set_title(title, fontsize = 6)
    plt.show()

def CompareBoolean_2D(df, str1, str2, direction):
    
    Boolean_list = [0]
    #Greater
    if direction == 0:
        for i in range(1,len(df)):
            delta = df[str1][i] > df[str2][i-1]
            delta = int(delta == True)
            Boolean_list.append(delta)
        return Boolean_list
    #Lesser
    if direction == 1:
        for i in range(1, len(df)):
            delta = df[str1][i] < df[str2][i-1]
            delta = int(delta == True)
            Boolean_list.append(delta)
        return Boolean_list
    #Equal to
    if direction == 2:
        for i in range(1, len(df)):
            delta = df[str1][i] == df[str2][i-1]
            delta = int(delta == True)
            Boolean_list.append(delta)
        return Boolean_list
            
def CompareBoolean(df, str1, str2, direction):
    Boolean_list = [0]
    #Greater
    if direction == 0:
        for i in range(1,len(df)):
            delta = df[str1][i] > df[str2][i]
            delta = int(delta == True)
            Boolean_list.append(delta)
        return Boolean_list
    #Lesser
    if direction == 1:
        for i in range(1, len(df)):
            delta = df[str1][i] < df[str2][i]
            delta = int(delta == True)
            Boolean_list.append(delta)
        return Boolean_list
    #Equal to
    if direction == 2:
        for i in range(1, len(df)):
            delta = df[str1][i] == df[str2][i]
            delta = int(delta == True)
            Boolean_list.append(delta)
        return Boolean_list
    
def Labeling(df, str1, str2):
    Label_list = []
    for i in range(0, len(df)):
        if df[str1][i] > df[str2][i]:
            Label_list.append(-1)
            continue
        if df[str1][i] < df[str2][i]:
            Label_list.append(1)
            continue
        if df[str1][i] == df[str2][i]:
            Label_list.append(0)
            continue
    return Label_list

def Labeling_2D(df, str1, str2):
    Label_list = [0]
    for i in range(1, len(df)):
        if df[str1][i] > df[str2][i-1]:
            Label_list.append(1)
            continue
        if df[str1][i] < df[str2][i-1]:
            Label_list.append(-1)
            continue
        if df[str1][i] == df[str2][i-1]:
            Label_list.append(0)
            continue
    return Label_list

def ATR(df):
    ATR_list = []
    for i in range(1, len(df)):
        T_PreCvH = df['High'][i] - df['Close'][i-1]
        T = df['High'][i] - df['Low'][i]
        T_PreCvL = df['Close'][i-1] - df['Low'][i]
        List_ATR = [T_PreCvH, T, T_PreCvL]
        ATR_value = max(List_ATR)
        ATR_list.append(ATR_value)
    return ATR_list

def RSI_TALIB(df, value, timeperiod):
    Temp_Value = np.array(list(df[value]))
    TA_LIST = tb.RSI(Temp_Value, timeperiod)
    TA_LIST = TA_LIST.tolist()
    return TA_LIST

def MACD_TALIB(df, value, timeperiod, timeperiod2, Signal_Period):
    Temp_Value = np.array(list(df[value]))
    MACD_1226, MACD_Signal, MACD_Hist = tb.MACD(Temp_Value, 
                                         fastperiod = timeperiod,
                                         slowperiod = timeperiod2,
                                         signalperiod = Signal_Period)
    return MACD_1226.tolist(), MACD_Signal.tolist(), MACD_Hist.tolist()

def Feature_Generate(df):    
    Stock_Data_Featured = df
    #Adding ATR
    #Stock_Data_Featured['ATR'] = ATR(Stock_Data_Featured)
    #Adding 
    #Adding DayRange
    Stock_Data_Featured['DayRange'] = Stock_Data_Featured['High'] - Stock_Data_Featured['Low']
    #Adding ATR % (DayRange/Close)
    Stock_Data_Featured['DayRange%'] = Stock_Data_Featured['DayRange'] / Stock_Data_Featured['Close']
    #Adding Price related features
    #percentage change %(open - pre. close/ pre. close )
    Stock_Data_Featured['P%_preCvO'] = Daychange(Stock_Data_Featured, 'Open', 'Close')
    Stock_Data_Featured['P%_preCvC'] = Daychange(Stock_Data_Featured, 'Close', 'Close')
    Stock_Data_Featured['P%_preOvO'] = Daychange(Stock_Data_Featured, 'Open', 'Open')
    Stock_Data_Featured['P%_preHvO'] = Daychange(Stock_Data_Featured, 'Open', 'High')
    Stock_Data_Featured['P%_preLvO'] = Daychange(Stock_Data_Featured, 'Open', 'Low')
    #Adding Volumn related Features
    Stock_Data_Featured['P%_preVol'] = Daychange(Stock_Data_Featured, 'Volume', 'Volume')
    #Adding Rise and Drop
    Stock_Data_Featured['Rise'] = CompareBoolean(Stock_Data_Featured, 'Open', 'Close', 1)
    Stock_Data_Featured['Drop'] = CompareBoolean(Stock_Data_Featured, 'Open', 'Close', 0)
    Stock_Data_Featured['UNCH'] = CompareBoolean(Stock_Data_Featured, 'Open', 'Close', 2)
    #Label For Rise and Drop
    Stock_Data_Featured['Status_Label'] = Labeling(Stock_Data_Featured, 'Open', 'Close')
    #Adding average price and volumn
    #Adding Historical High and Low
    Stock_Data_Featured['Week_High'] = Stock_Data_Featured['High'].rolling(window=5).max()
    Stock_Data_Featured['4Week_High'] = Stock_Data_Featured['High'].rolling(window=20).max()
    Stock_Data_Featured['12Week_High'] = Stock_Data_Featured['High'].rolling(window=60).max()
    Stock_Data_Featured['52Week_High'] = Stock_Data_Featured['High'].rolling(window=260).max()
    
    Stock_Data_Featured['Week_Low'] = Stock_Data_Featured['Low'].rolling(window=5).min()
    Stock_Data_Featured['4Week_Low'] = Stock_Data_Featured['Low'].rolling(window=20).min()
    Stock_Data_Featured['12Week_Low'] = Stock_Data_Featured['Low'].rolling(window=60).min()
    Stock_Data_Featured['52Week_Low'] = Stock_Data_Featured['Low'].rolling(window=260).min()
    #Adding % difference between closing price and High / Low
    Stock_Data_Featured['Divation%_wk_High'] = Stock_Data_Featured['Close'] / Stock_Data_Featured['Week_High']
    Stock_Data_Featured['Divation%_4wk_High'] = Stock_Data_Featured['Close'] / Stock_Data_Featured['4Week_High']
    Stock_Data_Featured['Divation%_12wk_High'] = Stock_Data_Featured['Close'] / Stock_Data_Featured['12Week_High']
    Stock_Data_Featured['Divation%_52wk_High'] = Stock_Data_Featured['Close'] / Stock_Data_Featured['52Week_High']
    
    Stock_Data_Featured['Divation%_wk_Low'] = Stock_Data_Featured['Close'] / Stock_Data_Featured['Week_Low']
    Stock_Data_Featured['Divation%_4wk_Low'] = Stock_Data_Featured['Close'] / Stock_Data_Featured['4Week_Low']
    Stock_Data_Featured['Divation%_12wk_Low'] = Stock_Data_Featured['Close'] / Stock_Data_Featured['12Week_Low']
    Stock_Data_Featured['Divation%_52wk_Low'] = Stock_Data_Featured['Close'] / Stock_Data_Featured['52Week_Low']
    #Adding moving average DayRange
    Stock_Data_Featured['MA_7_DayRange'] = Stock_Data_Featured['DayRange'].rolling(window=7).mean()
    Stock_Data_Featured['MA_12_DayRange'] = Stock_Data_Featured['DayRange'].rolling(window=12).mean()
    #Adding moving average
    Stock_Data_Featured['MA_20_P'] = Stock_Data_Featured['Close'].rolling(window=20).mean()
    Stock_Data_Featured['MA_50_P'] = Stock_Data_Featured['Close'].rolling(window=50).mean()
    Stock_Data_Featured['MA_150_P'] = Stock_Data_Featured['Close'].rolling(window=150).mean()
    Stock_Data_Featured['MA_200_P'] = Stock_Data_Featured['Close'].rolling(window=200).mean()
    #Adding EMA
    Stock_Data_Featured['EMA_9_P'] = Stock_Data_Featured['Close'].ewm(span=9, adjust = False).mean()
    Stock_Data_Featured['EMA_16_P'] = Stock_Data_Featured['Close'].ewm(span=16, adjust = False).mean()
    #Adding Moving average volumn
    Stock_Data_Featured['MA_7_V'] = Stock_Data_Featured['Volume'].rolling(window=7).mean()
    Stock_Data_Featured['MA_12_V'] = Stock_Data_Featured['Volume'].rolling(window=12).mean()
    #Adding Moving average for P% change
    Stock_Data_Featured['MA_7_preCvO'] = Stock_Data_Featured['P%_preCvO'].rolling(window=7).mean()
    Stock_Data_Featured['MA_14_preCvO'] = Stock_Data_Featured['P%_preCvO'].rolling(window=14).mean()
    
    Stock_Data_Featured['MA_7_preCvC'] = Stock_Data_Featured['P%_preCvC'].rolling(window=7).mean()
    Stock_Data_Featured['MA_14_preCvC'] = Stock_Data_Featured['P%_preCvC'].rolling(window=14).mean()
    
    Stock_Data_Featured['MA_7_preOvO'] = Stock_Data_Featured['P%_preOvO'].rolling(window=7).mean()
    Stock_Data_Featured['MA_14_preOvO'] = Stock_Data_Featured['P%_preOvO'].rolling(window=14).mean()
    
    Stock_Data_Featured['MA_7_preHvO'] = Stock_Data_Featured['P%_preHvO'].rolling(window=7).mean()
    Stock_Data_Featured['MA_14_preHvO'] = Stock_Data_Featured['P%_preHvO'].rolling(window=14).mean()
    
    Stock_Data_Featured['MA_7_preHvO'] = Stock_Data_Featured['P%_preHvO'].rolling(window=7).mean()
    Stock_Data_Featured['MA_14_preHvO'] = Stock_Data_Featured['P%_preHvO'].rolling(window=14).mean()
    
    Stock_Data_Featured['MA_7_preLvO'] = Stock_Data_Featured['P%_preLvO'].rolling(window=7).mean()
    Stock_Data_Featured['MA_14_preLvO'] = Stock_Data_Featured['P%_preLvO'].rolling(window=14).mean()
    #Adding RSI
    Stock_Data_Featured['RSI_7'] = RSI_TALIB(Stock_Data_Featured, 'Close', 7)
    Stock_Data_Featured['RSI_14'] = RSI_TALIB(Stock_Data_Featured, 'Close', 14)
    #Adding MACD
    Stock_Data_Featured['MACD_1226'], Stock_Data_Featured['MACD_Signal'], Stock_Data_Featured['MACD_Hist'] = MACD_TALIB(Stock_Data_Featured, 'Close', 12, 26, 9)
    #Adding Flags
    #Adding Gap up indicator
    Stock_Data_Featured['Pre_mkt_up'] = CompareBoolean_2D(Stock_Data_Featured, 'Open', 'Close', 0)
    Stock_Data_Featured['Pre_mkt_drop'] = CompareBoolean_2D(Stock_Data_Featured, 'Open', 'Close', 1)
    Stock_Data_Featured['Gap_up'] = CompareBoolean_2D(Stock_Data_Featured, 'Open', 'High', 0)
    Stock_Data_Featured['Gap_drop'] = CompareBoolean_2D(Stock_Data_Featured, 'Open', 'Low', 1)
    #label For Gap up indicator
    Stock_Data_Featured['Pre_mkt_labeling'] = Labeling_2D(Stock_Data_Featured, 'Open', 'Close')
    Stock_Data_Featured['Gap_Up/Drop'] = Labeling_2D(Stock_Data_Featured, 'Open', 'Close')
    return Stock_Data_Featured
#Adding Check Point A Flag

#Adding Check Point B Flag

#Adding Check Point 
def Warm_Up(df):
    #Slice Data Set
    Slice_Stock_Data_set = df[50:]
    #Change DateTime Object into datetime64
    Slice_Stock_Data_set['Date'] = Slice_Stock_Data_set['Date'].apply(DTconvert)
    return Slice_Stock_Data_set
#Debug
#df = Feature_Generate(Stock_Data_Featured)
#df.to_csv(r'G:\Algo_trade\Algo_Log\debug.csv')