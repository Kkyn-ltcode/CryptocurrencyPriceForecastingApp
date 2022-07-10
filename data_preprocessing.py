import datetime
import numpy as np
import pandas as pd


class data_preprocessing():
    def __init__(self, coin='BTCUSDT'):
        self.coin = coin
        self.file_name = f'{self.coin}_kline'

        self.read_data()
        self.feature_engineering()


def read_data(self):
    data = pd.read_csv(f'data/{self.file_name}.csv')
    data['OpenTime'] = data['OpenTime'].apply(
        lambda x: datetime.datetime.fromtimestamp(int(x / 1000)))
    close_time = data.pop('CloseTime').apply(
        lambda x: datetime.datetime.fromtimestamp(int(x / 1000)))
    data = data.set_index('OpenTime')
    data['hour'] = data.index.hour
    data['day'] = data.index.day
    data['month'] = data.index.month
    data['year'] = data.index.year

    self.dataset = data


data_preprocessing.read_data = read_data


def feature_engineering(self):
    df = self.dataset
    agg_func = ['sum', 'max', 'min', 'mean', 'count']
    columns = df.columns

    group_by_day = df.groupby(['year', 'month', 'day'])[
        columns[:9]].agg(agg_func).drop(columns='Close')
    group_by_day.columns = [
        f'{col[0]}_{col[1]}(day)' for col in group_by_day.columns]

    df = df.merge(group_by_day, left_on=[
                  'year', 'month', 'day'], right_index=True, how='left')

    trades_bins = np.append(np.linspace(0, 199000, 200),
                            np.linspace(200000, 400000, 5))
    df['NumberOfTrades_iterval'] = pd.cut(
        df['NumberOfTrades'], trades_bins).values
    trades_group = df.groupby('NumberOfTrades_iterval')[
        columns[3:9].drop('NumberOfTrades')].agg(agg_func)
    trades_group.columns = [
        f'Trades_{col[0]}_{col[1]}(interval)' for col in trades_group.columns]

    df = df.merge(trades_group, left_on='NumberOfTrades_iterval',
                  right_index=True, how='left')
    df.drop(columns='NumberOfTrades_iterval', inplace=True)

    df['High_Close_diff'] = df['High'] - df['Close']
    df['Close_Low_diff'] = df['Close'] - df['Low']
    df['Open_Close_diff'] = df['Close'] - df['Open']

    self.dataset = df


data_preprocessing.feature_engineering = feature_engineering
