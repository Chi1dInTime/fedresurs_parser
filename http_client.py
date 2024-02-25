import requests
import time

from requests.exceptions import ConnectionError, RequestException


class HTTPClient:
    def __init__(self):
        self.headers = headers or {}

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

