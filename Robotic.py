import pyautogui
import time
import datetime

import shutil, os
import glob2

#Open Browser
pyautogui.click(59, 9)
time.sleep(10)
#Input in search bar
pyautogui.write('https://www.nasdaq.com/market-activity/stocks/screener')
pyautogui.press('enter')
time.sleep(6)
#Select Nasdaq
pyautogui.click(183, 513)
pyautogui.scroll(-15)
time.sleep(2)
#Press Apply
pyautogui.click(191, 953)
#Scroll back
time.sleep(5)
pyautogui.scroll(15)
#Download Nasdaq csv
time.sleep(3)
pyautogui.click(1122, 579)
time.sleep(8)
#Select NYSE
pyautogui.click(182, 657)
pyautogui.scroll(-15)
#Press Apply
pyautogui.click(191, 953)
time.sleep(5)
pyautogui.scroll(15)
time.sleep(3)
#Download NYSE csv
pyautogui.click(1122, 579)
time.sleep(8)

#Select AMEX
pyautogui.click(182, 691)
pyautogui.scroll(-15)
#Press Apply
pyautogui.click(191, 953)
time.sleep(5)
pyautogui.scroll(15)
time.sleep(3)
#Download AMEX csv
pyautogui.click(1122, 579)

#look up file names
files = glob2.glob(r'/home/pi/Downloads/*.csv')
#create folder in Stock_Symbol folder
path = r'/home/pi/Desktop/Stock_Symbol'
os.chdir(path)
today = datetime.date.today()
folder_name = str(today)+ "_Stock_Master_Record"
#Create Folder
os.makedirs(folder_name)
#move file to new folder
for file in files:
    shutil.move(file, path + '/' + folder_name)
