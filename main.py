from data_parser import Downloader
from data_processing import DataProcessor
from datetime import datetime, timedelta
from data_manager import DataManager

ORG_GUIDS = {'АО "КАРШЕРИНГ"': "872c7dff-8ef5-4a40-8f95-7aeec0bc3a80"}
START_DATE = datetime(2020, 11, 12)


def get_orgs_data(org_guids, start_date):

    data = {}
    corrected_data = {}
    for org, guid in org_guids.items():
        downloader = Downloader(guid, start_date)
        data[org] = downloader.get_company_data()
        dataprocessor = DataProcessor(data[org])
        corrected_data[org] = dataprocessor.get_correct_data()
        DataManager.export_csv(corrected_data[org], org + '.csv')
    return corrected_data


def run_pipeline():
    data = get_orgs_data(ORG_GUIDS, START_DATE)


if __name__ == "__main__":
    run_pipeline()
