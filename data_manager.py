from datetime import datetime, timedelta
import os
import requests
import time
from requests.exceptions import ConnectionError, RequestException

JSON_DATA_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"


class DataManager:
    """
    В классе DataManager находятся вспомогательные методы для парсинга данных, такие как работа с http,
    формирование списка дат, экспорт в csv и xslx
    """
    def __init__(self, company_guid, start_date):
        self.company_guid = company_guid or ""
        self.start_date = start_date or ""

    @staticmethod
    def get_month_date_ranges(start_date):
        end_date = datetime.now()
        date_ranges = []
        current_date = start_date

        while current_date <= end_date:
            first_day_of_month = current_date.replace(day=1)
            next_month = first_day_of_month + timedelta(days=32)
            last_day_of_month = next_month.replace(day=1) - timedelta(days=1)
            date_ranges.append((first_day_of_month.strftime(JSON_DATA_FORMAT),
                                last_day_of_month.strftime(JSON_DATA_FORMAT)))
            current_date = last_day_of_month + timedelta(days=1)

        return date_ranges

    @staticmethod
    def get_daily_date_ranges(msg_start_date, msg_end_date):
        start_date = datetime.strptime(msg_start_date, JSON_DATA_FORMAT)
        end_date = datetime.strptime(msg_end_date, JSON_DATA_FORMAT)
        current_date = start_date
        date_ranges = []
        while current_date <= end_date:
            date_ranges.append((current_date.strftime(JSON_DATA_FORMAT), current_date.strftime(JSON_DATA_FORMAT)))
            current_date += timedelta(days=1)
        return date_ranges

    @staticmethod
    def export_csv(df, filename):
        filename = filename.replace('"', "").replace("'", "")
        current_directory = os.getcwd()
        csv_file_path = os.path.join(current_directory, filename)
        df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')

    @staticmethod
    def export_xlsx(df, filename):
        filename = filename.replace('"', "").replace("'", "")
        current_directory = os.getcwd()
        xlsx_file_path = os.path.join(current_directory, filename)
        df.to_excel(xlsx_file_path, index=False, engine='openpyxl')

    @staticmethod
    def fetch(url, headers, timeout=15, attempt_limit=20):
        attempt_count = 0
        while attempt_count < attempt_limit:
            try:
                response = requests.get(url, headers=headers, timeout=timeout)
                response.raise_for_status()
                return response.json()
            except (ConnectionError, RequestException) as e:
                attempt_count += 1
                print(f"Error: {e}. Attempt {attempt_count} of {attempt_limit}")
                time.sleep(timeout)
            except Exception as e:
                print(f"Неожиданная ошибка: {e}. Попытка {attempt_count} из {attempt_limit}")
                time.sleep(timeout)
                attempt_count += 1
        if attempt_count == attempt_limit:
            print("Превышено максимальное количество попыток.")
        return None
