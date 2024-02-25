from itertools import chain
from http_client import HTTPClient
from data_manager import DataManager
import pandas as pd
import time
import random
import os


class Downloader:
    def __init__(self, company_guid, start_date):
        self.company_guid = company_guid or ""
        self.start_date = start_date or ""

    def get_msg_guids_for_date(self, msg_start_date, msg_end_date):
        url = "https://fedresurs.ru/backend/companies/" + self.company_guid + "/publications?limit=15&offset=0&startDate=" + \
              msg_start_date + "&endDate=" + msg_end_date + \
              "&searchCompanyEfrsb=true&searchAmReport=true&searchFirmBankruptMessage=true" \
              "&searchFirmBankruptMessageWithoutLegalCase=false&searchSfactsMessage=true&searchSroAmMessage=true" \
              "&searchTradeOrgMessage=true "
        hdrs = {"Referer": "https://fedresurs.ru/company/" + self.company_guid}
        msg_json = HTTPClient.fetch(url, hdrs)
        msg_count = msg_json["found"]
        msg_guids = []
        print(msg_start_date + " - " + msg_end_date)
        print(msg_count)
        for i in range(0, msg_count, 15):  # Можно доставать из бэкэнда только по 15 записей за раз
            url = "https://fedresurs.ru/backend/companies/" + self.company_guid + "/publications?limit=15&offset=" + str(
                i) + \
                  "&startDate=" + msg_start_date + "&endDate=" + msg_end_date + "&searchCompanyEfrsb=true&searchAmReport=true" \
                                                                                "&searchFirmBankruptMessage=true&searchFirmBankruptMessageWithoutLegalCase=false" \
                                                                                "&searchSfactsMessage=true&searchSroAmMessage=true&searchTradeOrgMessage=true"
            msg_json = HTTPClient.fetch(url, headers=hdrs)
            guids_count = len(msg_json["pageData"])
            for j in range(0, guids_count):
                msg_guids.append(msg_json["pageData"][j]["guid"])
        return msg_guids

    def get_msg_guids(self):
        dates = DataManager.get_month_date_ranges(self.start_date)
        msg_guids = []
        for msg_start_date, msg_end_date in dates:
            msg_guids.extend(self.get_msg_guids_for_date(msg_start_date, msg_end_date))
            time.sleep(random.uniform(0, 0.1))  # слип на всякий случай, чтобы сервер не подумал что мы его ддосим)

        return msg_guids

    @staticmethod
    def get_msg_data(url, hdrs):
        msg_json = HTTPClient.fetch(url, hdrs)
        column_names = ["guid", "lessors", "lessors_inn", "lessors_ogrn", "identifier", "classifier", "description",
                        "vin_id_corrected", "datePublish"]
        df = pd.DataFrame(columns=column_names)
        guid = msg_json["guid"]
        datePublish = msg_json["datePublish"]
        content = msg_json.get("content", {})
        if "lessors" in content:
            lessors = msg_json["content"]["lessors"][0].get("fullName", "")
            lessors_inn = msg_json["content"]["lessors"][0].get("inn", "")
            lessors_ogrn = msg_json["content"]["lessors"][0].get("ogrn", "")
        elif "lessorsCompanies" in content:
            lessors = msg_json["content"]["lessorsCompanies"][0].get("fullName", "")
            lessors_inn = msg_json["content"]["lessorsCompanies"][0].get("inn", "")
            lessors_ogrn = msg_json["content"]["lessorsCompanies"][0].get("ogrn", "")
        else:
            lessors = ""
            lessors_inn = ""
            lessors_ogrn = ""

        for subject in chain(msg_json["content"].get("changedSubjects", []), msg_json["content"].get("subjects", [])):
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

    def get_company_data(self):
        all_msg_data = []
        guid_counter = 0
        msg_guids = self.get_msg_guids()
        guid_len = len(msg_guids)
        for msg_guid in msg_guids:
            guid_counter += 1
            url = "https://fedresurs.ru/backend/sfactmessages/" + msg_guid
            hdrs = {"Referer": "https://fedresurs.ru/sfactmessage/" + msg_guid}
            print(str(guid_counter) + "/" + str(guid_len) + " " + str(msg_guid))
            all_msg_data.append(self.get_msg_data(url, hdrs))
        df_all_msg = pd.concat(all_msg_data, ignore_index=True)
        return df_all_msg
