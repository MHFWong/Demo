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


#Import Data
path = r'E:\python\USMK Data\SQ.csv'
#path = r'D:\Testing\GOOG.csv'
#Stock_Data = pd.read_csv(path, index_col = 'Date')
Stock_Data = pd.read_csv(path)
Stock_Data_Featured = Stock_Data.copy()
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
    
#Adding ATR
Stock_Data_Featured['ATR'] = Stock_Data_Featured['High'] - Stock_Data_Featured['Low']
#Adding ATR % (ATR/Close)
Stock_Data_Featured['ATR%'] = Stock_Data_Featured['ATR'] / Stock_Data_Featured['Close']
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
#Adding average price and volumn
#Adding moving average ATR
Stock_Data_Featured['MA_7_ATR'] = Stock_Data_Featured['ATR'].rolling(window=7).mean()
Stock_Data_Featured['MA_12_ATR'] = Stock_Data_Featured['ATR'].rolling(window=12).mean()
#Adding moving average
Stock_Data_Featured['MA_20_P'] = Stock_Data_Featured['Close'].rolling(window=20).mean()
Stock_Data_Featured['MA_50_P'] = Stock_Data_Featured['Close'].rolling(window=50).mean()
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
#Adding Flags
#Adding Gap up indicator
Stock_Data_Featured['Pre_mkt_up'] = CompareBoolean_2D(Stock_Data_Featured, 'Open', 'Close', 0)
Stock_Data_Featured['Pre_mkt_drop'] = CompareBoolean_2D(Stock_Data_Featured, 'Open', 'Close', 1)
Stock_Data_Featured['Gap_up'] = CompareBoolean_2D(Stock_Data_Featured, 'Open', 'High', 0)
Stock_Data_Featured['Gap_drop'] = CompareBoolean_2D(Stock_Data_Featured, 'Open', 'Low', 1)
#Adding Check Point A Flag

#Adding Check Point B Flag

#Adding Check Point 

#Slice Data Set
Slice_Stock_Data_set = Stock_Data_Featured[50:]
#Change DateTime Object into datetime64
Slice_Stock_Data_set['Date'] = Slice_Stock_Data_set['Date'].apply(DTconvert)
print(Slice_Stock_Data_set['Gap_up'].sum())
print(Slice_Stock_Data_set.info())

correlation_of_Xy(Slice_Stock_Data_set, 'Corelation matrix')

Slice_Stock_Data_set.to_csv(r'G:\Algo_trade\Algo_Log\Slice_Stock_Data_Set.csv', index= False)
#Inventory checking
#MA 20 / 100
