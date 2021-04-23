# -*- coding: utf-8 -*-
"""
Created on Sat Mar  6 11:17:28 2021

@author: marti
"""

import pandas as pd
import Feature_Engineering as FE
import math
import datetime as dt

#ML model
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split

forest_model = RandomForestClassifier(random_state=1)


path = r'E:\python\USMK Data\SQ.csv'
path2 = r'G:\Algo_trade\Algo_Log\TSLA.csv'
#split raw data
df = pd.read_csv(path)

#Convert datetime data to numeric

Featured_DF = FE.Feature_Generate(df)
Featured_Train_DF = FE.Warm_Up(Featured_DF)
Featured_Train_DF['Date'] = Featured_Train_DF['Date'].map(dt.datetime.toordinal)

Featured_Train_DF = Featured_Train_DF.drop(columns = ['Rise', 'Drop', 'UNCH', 'Status_Label', 'Pre_mkt_up', 'Pre_mkt_drop', 'Gap_up', 'Gap_drop', 'Pre_mkt_labeling'])
X = Featured_Train_DF.drop(columns = ['Gap_Up/Drop'])
y = Featured_Train_DF['Gap_Up/Drop']

print(X)
print(y)
#X = Data Feature Y= target
X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.33)

#Fit in RandomForestGregressor
forest_model.fit(X_train, y_train)
Gap_up_predict = forest_model.predict(X_test)

print(mean_absolute_error(y_test, Gap_up_predict))
print("Prediction :", Gap_up_predict)
print("Actual :", y_test)
#Basic Back Test
#Strategy - Buy in the after market session if the label = 1
#Sell at the open price on the next trade day
#Status 100000 cash
#with the Model, insert data row by row
#import new data for testing (cut the lastest data and test for 3 month)
real_data = pd.read_csv(path2)
Featured_real_data = FE.Feature_Generate(real_data)
Featured_real_data = FE.Warm_Up(Featured_real_data)
Featured_real_data['Date'] = Featured_real_data['Date'].map(dt.datetime.toordinal)
Featured_real_data = Featured_real_data.drop(columns = ['Rise', 'Drop', 'UNCH', 'Status_Label', 'Pre_mkt_up', 'Pre_mkt_drop', 'Gap_up', 'Gap_drop', 'Pre_mkt_labeling'])
T1_list = [0]
T1_list = T1_list + Featured_real_data['Gap_Up/Drop']
T1_list = T1_list[:-1]
Featured_real_data['Gap_Up/Drop'] = T1_list
Featured_real_data = Featured_real_data[2482:]
Featured_real_data = Featured_real_data.reset_index()
#Input Data
Featured_real_data_X = Featured_real_data.drop(columns = ['Gap_Up/Drop','index'])
Featured_real_data_y = Featured_real_data['Gap_Up/Drop']

#Ledger
#Cash Account
Cash = 100000
Position = 0

#Limited Trade Book
TradeDate = []
Cash_Position = []
Security_Position = []
print(Featured_real_data_X)

for i in range(0, 59):
    Featured_real_data_test = Featured_real_data_X.iloc[i]
    DataFeed = Featured_real_data_test.to_frame()
    DataFeed = DataFeed.T
    prediction = forest_model.predict(DataFeed)
    
    #clean up position
    if Position > 0:
        #sell all at the open hour
        Cash = Cash + Position * list(DataFeed['Open'])[0]
        TradeDate.append(list(DataFeed['Date'])[0])
        Position = Position - Position
        Security_Position.append(Position)
        Cash_Position.append(Cash)
    if prediction == 1:
        Position = Position + 100
        Cash = Cash - list(DataFeed['Close'])[0] * 100
        TradeDate.append(list(DataFeed['Date'])[0])
        Security_Position.append(Position)
        Cash_Position.append(Cash)
    continue

TradeBook_detail = {'TradeDate':TradeDate, 'Cash_Position':Cash_Position, 'Security_Position':Security_Position}
TradeBook = pd.DataFrame(data=TradeBook_detail)
TradeBook.to_csv(r'G:\Algo_trade\Algo_Log\TradeBook.csv')