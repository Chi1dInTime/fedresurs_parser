from itertools import chain
from data_manager import DataManager
import pandas as pd
import time
import random


class Downloader:
    """
    Основной класс для парсинга данных с сайта
    """
    def __init__(self, company_guid, start_date):
        self.company_guid = company_guid or ""
        self.start_date = start_date or ""
        self.headers = {"Referer": "https://fedresurs.ru/company/" + self.company_guid}

    def get_msg_url(self, msg_start_date, msg_end_date, offset):
        """
        Метод возвращает url странички компании со списком сообщений, фильтруя по дате сообщений
        offset необходим так как сайт выдаёт данные в json только по 15 за раз
        """
        url = "https://fedresurs.ru/backend/companies/" + self.company_guid + "/publications?limit=15&offset=" + str(
            offset) + \
              "&startDate=" + msg_start_date + "&endDate=" + msg_end_date + \
              "&searchCompanyEfrsb=true&searchAmReport=true&searchFirmBankruptMessage=true" \
              "&searchFirmBankruptMessageWithoutLegalCase=false&searchSfactsMessage=true&searchSroAmMessage=true" \
              "&searchTradeOrgMessage=true "
        return url

    def get_msg_guids_for_date(self, msg_start_date, msg_end_date, is_daily=False):
        """
        метод возвращает список guid сообщений, с даты msg_start_date по msg_end_date
        is_daily - означает что функция вызвана вытащить сообщения одного дня
        """
        i = 0
        url = self.get_msg_url(msg_start_date, msg_end_date, i)
        msg_json = DataManager.fetch(url, self.headers)
        msg_count = msg_json["found"]
        msg_guids = []
        print(msg_start_date + " - " + msg_end_date)
        print(msg_count)
        if msg_count >= 500:
            if is_daily:
                print("В день более 500 записей, не получится вытащить таким способом из-за ограничения")
                return []
            return self.get_msg_guids(daily=True, msg_start_date=msg_start_date, msg_end_date=msg_end_date)
        for i in range(0, msg_count, 15):  # Можно доставать из бэкэнда только по 15 записей за раз
            url = self.get_msg_url(msg_start_date, msg_end_date, i)
            msg_json = DataManager.fetch(url, self.headers)
            guids_count = len(msg_json["pageData"])
            for j in range(0, guids_count):
                msg_guids.append(msg_json["pageData"][j]["guid"])
        return msg_guids

    def get_msg_guids(self, daily=False, msg_start_date="", msg_end_date=""):
        """
        метод формирует списки дат по которым будут искаться сообщения и вызывает метод get_msg_guids_for_date
        для формирования списка guid сообщений.
        параметр daily означает что функция вызвана для работы внутри месяца (если количество сообщений в месяце было больше 500)
        функция возвращает списко guid сообщений
        """
        if daily:
            dates = DataManager.get_daily_date_ranges(msg_start_date, msg_end_date)
        else:
            dates = DataManager.get_month_date_ranges(self.start_date)
        msg_guids = []
        for msg_start_date, msg_end_date in dates:
            msg_guids.extend(self.get_msg_guids_for_date(msg_start_date, msg_end_date, daily))
            time.sleep(random.uniform(0, 0.1))  # слип на всякий случай, чтобы запросы выглядели человеческими)

        return msg_guids

    @staticmethod
    def get_msg_data(url, hdrs):
        """
        Метод вытаскивает json с данными по сообщению и парсит его в датасет с необходимыми данными
        """
        msg_json = DataManager.fetch(url, hdrs)
        column_names = ["guid", "lessors", "lessors_inn", "lessors_ogrn", "identifier", "classifier", "description",
                        "vin_id_corrected", "datePublish"]
        df = pd.DataFrame(columns=column_names)
        guid = msg_json["guid"]
        datePublish = msg_json["datePublish"]
        content = msg_json.get("content", {})
        if "lessors" in content:
            lessors = content["lessors"][0].get("fullName", "")
            lessors_inn = content["lessors"][0].get("inn", "")
            lessors_ogrn = content["lessors"][0].get("ogrn", "")
        elif "lessorsCompanies" in content:
            lessors = content["lessorsCompanies"][0].get("fullName", "")
            lessors_inn = content["lessorsCompanies"][0].get("inn", "")
            lessors_ogrn = content["lessorsCompanies"][0].get("ogrn", "")
        else:
            lessors = ""
            lessors_inn = ""
            lessors_ogrn = ""

        for subject in chain(content.get("changedSubjects", []), content.get("subjects", [])):
            identifier = subject.get("identifier", subject.get('subjectId', ""))
            classifier_dict = subject.get("classifier")
            if classifier_dict is None:
                classifier = subject.get("classifierName", "")
            else:
                classifier = classifier_dict.get("description", "")
            description_list = [line for line in subject["description"].split('\n') if line]
            for vin_id in description_list:
                description = vin_id
                vin_id_corrected = ''
                df.loc[len(df)] = [guid, lessors, lessors_inn, lessors_ogrn, identifier, classifier, description,
                                   vin_id_corrected, datePublish]
        return df

    @staticmethod
    def get_data_url_headers(msg_guid):
        """
        метод формирует url и headers по сообщению для дальнейшего обращения к серверу в методе get_msg_data
        """
        url = "https://fedresurs.ru/backend/sfactmessages/" + msg_guid
        hdrs = {"Referer": "https://fedresurs.ru/sfactmessage/" + msg_guid}
        return url, hdrs

    def get_company_data(self):
        """
        Метод формирует датасет с необходимыми данными по списку guid'ов сообщений
        """
        all_msg_data = []
        guid_counter = 0
        msg_guids = self.get_msg_guids()
        guid_len = len(msg_guids)
        for msg_guid in msg_guids:
            guid_counter += 1
            url, hdrs = self.get_data_url_headers(msg_guid)
            print(str(guid_counter) + "/" + str(guid_len) + " " + str(msg_guid))
            all_msg_data.append(self.get_msg_data(url, hdrs))
        df_all_msg = pd.concat(all_msg_data, ignore_index=True)
        return df_all_msg
