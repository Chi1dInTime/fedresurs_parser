import pandas as pd

FINAL_DATA_FORMAT = '%Y-%m-%d %H:%M:%S'


class DataProcessor:
    """
    Класс DataProcessor отвечает за преобразование данных после парсинга
    """
    def __init__(self, df):
        self.df = df

    def change_date_format(self):
        """
        Метод преобразует дату в поле datePublish в необходимый формат, находящийся в константе FINAL_DATA_FORMAT
        """
        self.df['datePublish'] = pd.to_datetime(self.df['datePublish'], format='mixed')
        self.df['datePublish'] = self.df['datePublish'].dt.strftime(FINAL_DATA_FORMAT)

    @staticmethod
    def update_vin_id_corrected(row):
        """
        Метод возвращает корректный vin_id:
        он находится либо в поле description (если длина 17 или 18), если нет, то в поле identifier
        """
        if len(row['description']) in [17, 18]:
            return row['description']
        elif row['identifier'] != 'Н/Д':
            return row['identifier']
        else:
            return ''

    def add_correct_vin_id(self):
        """
        Метод заполняет поле vin_id_corrected
        """
        self.df['description'] = self.df['description'].str.strip()
        self.df['vin_id_corrected'] = self.df.apply(self.update_vin_id_corrected, axis=1)

    def get_correct_data(self):
        self.change_date_format()
        self.add_correct_vin_id()
        return self.df
