import requests
from requests.adapters import HTTPAdapter, Retry


def get_session(tries: int = 3, backoff: float = 0.1) -> requests.Session:
    session = requests.Session()
    retries = Retry(total=tries, backoff_factor=backoff, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session
