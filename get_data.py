import json
import os
import datetime
import pandas as pd
import mysql.connector
from mysql.connector import Error

from binance.client import Client


class Dataset():
    def __init__(self, interval='hour', coin='BTCUSDT'):
        self.coin = coin
        self.file_name = f'{self.coin}_kline'
        self.client = Client()

        self.set_config()
        self.db_connect()
        self.create_table()

        if interval == 'day':
            self.kline_interval = Client.KLINE_INTERVAL_1DAY
        elif interval == 'hour':
            self.kline_interval = Client.KLINE_INTERVAL_1HOUR

        if not os.path.exists(f'data/{self.file_name}.csv'):
            self.get_historical_data()
            self.insert_to_db(self.dataset)
        else:
            self.dataset = pd.read_csv(f'data/{self.file_name}.csv')
            self.update_data()


def set_config(self):
    float_col = ['Open', 'High', 'Low', 'Close', 'Volume', 'QuoteAssetVolume',
                 'TakerBuyBaseAssetVolume', 'TakerBuyQuoteAssetVolume']
    dtypes = {'NumberOfTrades': 'int32',
              'OpenTime': 'object',
              'CloseTime': 'object'}
    for col in float_col:
        dtypes[col] = 'float32'
    self.dtypes = dtypes


Dataset.set_config = set_config


def get_historical_data(self):
    hist_data = pd.DataFrame(columns=['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume',
                                      'CloseTime', 'QuoteAssetVolume', 'NumberOfTrades',
                                      'TakerBuyBaseAssetVolume', 'TakerBuyQuoteAssetVolume'])

    klines_2017_2022 = self.client.get_historical_klines(
        self.coin, self.kline_interval, start_str="1 Jan, 2017")

    for kline in klines_2017_2022[:-1]:
        hist_data.loc[len(hist_data)] = kline[:-1]

    hist_data = hist_data.astype(self.dtypes)
    self.dataset = hist_data
    hist_data.to_csv(f'data/{self.file_name}.csv', index=False)


Dataset.get_historical_data = get_historical_data


def update_data(self):
    df = self.dataset

    newest_data = self.client.get_historical_klines(self.coin, self.kline_interval,
                                                    start_str=int(df.iloc[-1, 0]))

    if len(newest_data[1:-1]) != 0:
        for data in newest_data[1:-1]:
            df.loc[len(df)] = data[:-1]
        df = df.astype(self.dtypes)

        df.to_csv(f'data/{self.file_name}.csv', index=False)
        self.insert_to_db(df.iloc[-len(newest_data[1:-1]):])

    self.dataset = df


Dataset.update_data = update_data


def db_connect(self):
    with open('data/info.json', 'r') as file:
        db = json.load(file)
    try:
        connection = mysql.connector.connect(host=db['mysql']['host'],
                                             database='Bitcoin',
                                             user=db['mysql']['user'],
                                             password=db['mysql']['password'])
        self.connection = connection
    except Error as e:
        print("Error while connecting to MySQL", e)


Dataset.db_connect = db_connect


def create_table(self):
    create_table_command = (f"""CREATE TABLE IF NOT EXISTS `{self.coin}` (
                                `ID` int NOT NULL AUTO_INCREMENT,
                                `OpenTime` varchar(255) NOT NULL,
                                `Open` float NOT NULL,`High` float NOT NULL,
                                `Low` float NOT NULL,`Close` float NOT NULL,
                                `Volume` float NOT NULL,
                                `CloseTime` varchar(255) NOT NULL,
                                `QuoteAssetVolume` float NOT NULL,
                                `NumberOfTrades` int NOT NULL,
                                `TakerBuyBaseAssetVolume` float NOT NULL,
                                `TakerBuyQuoteAssetVolume` float NOT NULL,
                                PRIMARY KEY (ID));""")
    conn = self.connection
    curr = conn.cursor()
    curr.execute(create_table_command)
    conn.commit()


Dataset.create_table = create_table


def insert_to_db(self, df_to_insert):
    conn = self.connection
    curr = conn.cursor()
    for i, row in df_to_insert.iterrows():
        self.insert(curr, row)
    conn.commit()


Dataset.insert_to_db = insert_to_db


def insert(self, curr, row):
    insert_command = (f"""INSERT INTO `{self.coin}` (
                        `OpenTime`, `Open`, `High`, `Low`, `Close`, `Volume`, 
                        `CloseTime`, `QuoteAssetVolume`, `NumberOfTrades`, 
                        `TakerBuyBaseAssetVolume`, `TakerBuyQuoteAssetVolume`) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""")

    values_to_insert = (row['OpenTime'], row['Open'], row['High'], row['Low'],
                        row['Close'], row['Volume'], row['CloseTime'], row['QuoteAssetVolume'],
                        row['NumberOfTrades'], row['TakerBuyBaseAssetVolume'],
                        row['TakerBuyQuoteAssetVolume'])
    curr.execute(insert_command, values_to_insert)


Dataset.insert = insert


def drop_table(self):
    drop_command = (f"""DROP TABLE `{self.coin}`;""")
    conn = self.connection
    curr = conn.cursor()
    curr.execute(drop_command)
    conn.commit()


Dataset.drop_table = drop_table
