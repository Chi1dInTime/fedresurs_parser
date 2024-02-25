import pandas as pd


class DataProcessor:
    def __init__(self, df):
        self.df = df

    def change_date_format(self):
        self.df['datePublish'] = pd.to_datetime(self.df['datePublish'], format='mixed')
        self.df['datePublish'] = self.df['datePublish'].dt.strftime('%Y-%m-%d %H:%M:%S')

    def add_correct_vin_id(self):
        pass

    def get_correct_data(self):
        self.change_date_format()
        self.add_correct_vin_id()
        return self.df
