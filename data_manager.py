from datetime import datetime, timedelta
import os

class DataManager:
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
            date_ranges.append((first_day_of_month.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                                last_day_of_month.strftime("%Y-%m-%dT%H:%M:%S.000Z")))
            current_date = last_day_of_month + timedelta(days=1)

        return date_ranges

    @staticmethod
    def export_csv(df, filename):
        filename = filename.replace('"', "").replace("'", "")
        current_directory = os.getcwd()
        csv_file_path = os.path.join(current_directory, filename)
        df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')
