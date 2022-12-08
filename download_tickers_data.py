import numpy as np
import pandas as pd
from pandas_datareader import data as pdr
import pathlib
import os
import yfinance as yf
import csv
import json
import sys
import time
from dateutil.parser import parse
from confluent_kafka import Producer
import socket
import pymysql


def download_stock_data(ticker,start,end):
    df = yf.download(tickers=ticker, start=start, end=end, period='1d', interval='1d')
    return df


def geteSqlConnection():
    # Connect to the database
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='12345678',
                                 db='thales')
    return connection
def store_data_sql(connection,data,threadsleep):
    # create cursor
    cursor = connection.cursor()
    data.reset_index(inplace=True)
    # creating column list for insertion
    cols = "`,`".join([str(i) for i in data.columns.tolist()])
    print(cols)
    # Insert DataFrame recrds one by one.
    for i, row in data.iterrows():
        sql = "INSERT INTO `tickers` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        time.sleep(threadsleep)
        print(tuple(row))
        cursor.execute(sql, tuple(row))
        # the connection is not autocommitted by default, so we must commit to save our changes
        connection.commit()
def load_ticker_list(path):
    return pd.read_csv(path)
def load_data_sql(tickers_df,connection, start_date,end_date,threadsleep):
    for i, row in tickers_df.iterrows():
        print(row['Ticker'],row['Name'])
        ticker_id = row['Ticker']
        data = download_stock_data(ticker_id, start_date, end_date)
        data['ticker'] = ticker_id
        data['ticker_name'] = row['Name']
        store_data_sql(connection,data,threadsleep)

if __name__ == '__main__':
    tickers_df = load_ticker_list('tickers_list.csv')
    connection = geteSqlConnection()
    load_data_sql(tickers_df,connection,'2017-01-01','2022-02-02',5)




