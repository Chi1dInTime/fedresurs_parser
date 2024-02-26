from data_parser import Downloader
from data_processing import DataProcessor
from datetime import datetime
from data_manager import DataManager

# Константа, хранящая название и guid организаций, данные которых необходимо скачать
ORG_GUIDS = {'АО "КАРШЕРИНГ"': "872c7dff-8ef5-4a40-8f95-7aeec0bc3a80"}
# ORG_GUIDS = {'ПАО "КАРШЕРИНГ РУССИЯ"': "fb11b39c-01f2-418a-b730-4ec071e65908"}
# ORG_GUIDS = {'ПАО "КАРШЕРИНГ РУССИЯ"': "c9910f79-0a51-4810-937d-8d8dd76ca2b3"}

# Дата, начиная с которой идёт поиск сообщений
START_DATE = datetime(2019, 11, 26)


def get_orgs_data(org_guids, start_date):
    """
    Функция достаёт дату по организациям, преобразует и экспортирует в csv и xlsx

    Возвращает словарь датасетов со скорректированными данными по всем организациям в org_guids
    """
    data = {}
    corrected_data = {}
    for org, guid in org_guids.items():
        downloader = Downloader(guid, start_date)
        data[org] = downloader.get_company_data()
        dataprocessor = DataProcessor(data[org])
        corrected_data[org] = dataprocessor.get_correct_data()
        DataManager.export_csv(corrected_data[org], org + '.csv')
        DataManager.export_xlsx(corrected_data[org], org + '.xlsx')
    return corrected_data


def run_pipeline():
    data = get_orgs_data(ORG_GUIDS, START_DATE)


if __name__ == "__main__":
    run_pipeline()
