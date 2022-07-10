import pandas as pd
import os

from binance.client import Client


class Dataset():
    def __init__(self, interval='hour', coin='BTCUSDT'):
        self.coin = coin
        self.file_name = f'{self.coin}_kline'
        self.client = Client()

        self.set_config()

        if interval == 'day':
            self.kline_interval = Client.KLINE_INTERVAL_1DAY
        elif interval == 'hour':
            self.kline_interval = Client.KLINE_INTERVAL_1HOUR

        if not os.path.exists(f'data/{self.file_name}.csv'):
            self.get_historical_data()
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

    self.dataset = df


Dataset.update_data = update_data
