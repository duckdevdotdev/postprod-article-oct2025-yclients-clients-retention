import json
import time

import pandas as pd
from scheduler import Scheduler
import datetime as dt
from datetime import datetime, timedelta
from yclients import YClientsAPI
import base64
import os
from dataclasses import dataclass, field
import requests

resp = {
  "success": True,
  "data": [
    {
      "name": "Петров",
      "id": 2,
      "phone": "79101166438",
      "last_visit_date": '2014-09-21T23:00:00.000+03:00'
    },
    {
      "name": "Сидоров",
      "id": 3,
      "phone": "79101166438",
      "last_visit_date": '2014-09-21T23:00:00.000+03:00'
    },
    {
      "name": "Иванов",
      "id": 1,
      "phone": "79101166438",
      "last_visit_date": '2014-09-21T23:00:00.000+03:00'
    }
  ],
  "meta": {
    "total_count": 908
  }
}


class Filter:
    critical_date = "2025-01-01"
    filter: dict | None = None

    '''field(default_factory=lambda:  {
        "type": "last_visit_date",
        "state":
        {
            "from": "2000-01-01",
            "to": "2000-01-01"
        }
    })'''

    def update_filter(self):
        crit_date = datetime.today() - timedelta(days=5) + timedelta(days=25)
        self.critical_date = datetime.date(crit_date).isoformat()
        self.filter = {
        "type": "last_visit_date",
        "state":
        {
            "from": "2000-01-01",
            "to": "2000-01-01"
        }
    }
        self.filter['state']['to'] = self.critical_date


filt = Filter()
CID = 1568331
ust = '8c99dac7720ab85ea80d74b59cd71c63'
partn_tok = 'c8gnk5n4s4z44423zk96'
url = "https://api.yclients.com/api/v1/clients/{}".format(CID)
'''api = YClientsAPI(token=partn_tok, company_id=CID, form_id=0)
h = api.headers
h['Authorization'] = f'Bearer {partn_tok}, User {ust}' '''
h = {
    "Authorization": f"Bearer {partn_tok}, User {ust}",
    "Accept": "application/vnd.yclients.v2+json",
    "Content-Type": "application/json"
}
querystring = {}
querystring.update({"count": 10})
querystring.update({"page": 1})
# response = requests.get(url, headers=h, params=querystring)
PER_PAGE = 100
DAYS_NOT_VISITED = 100
send_str = 'Приходите в ближайшие 7 дней и получите скидку 10% на любую услугу.'
SMS_NUMBER = 3
exolve_phone = '79300649972'

exolve_api_key = os.environ['EXOLVE_API_KEY']
application_phone = os.environ['APPLICATION_PHONE']
manager_phone = os.environ['MANAGER_PHONE']


# ==YCLIENTS==

def query_clients(pages_counter: int):
    url = "https://api.yclients.com/api/v1/company/{}/clients/search".format(CID)
    querystring = {"page": 1, "page_size": PER_PAGE}
    response = requests.post(url, headers=h, json=querystring)
    assert response.status_code == 200
    response = response.json()
    total_clients = response['meta']['total_count']
    clients = response['data']
    return clients, total_clients


def not_visited(clid: str):
    url = "https://api.yclients.com/api/v1/company/{}/clients/visits/search".format(CID)
    querystring = {
        "client_id": clid,
        "client_phone": None,
        "from": "2000-01-01",
        "to": filt.critical_date,
        "payment_statuses": None,
        "attendance": None
    }
    response = requests.post(url, headers=h, json=querystring)
    ans = response.json()
    data = ans['data']['records']
    return len(data) == 0


def retrieve_client(client_id):
    url = f"https://api.yclients.com/api/v1/client/{CID}/{client_id}"
    response = requests.get(url, headers=h)
    assert response.status_code == 200
    return response.json()['data']


def filter_clients(clients: list[dict]):
    filtered_clients = []
    for c in clients:
        idx = c['id']
        if not_visited(idx):
            full_client_info = retrieve_client(idx)
            filtered_clients.append(full_client_info)
    return filtered_clients


def retrieve_by_visits():
    total_clients = 1
    pages_counter = 0
    bad_clients = []
    while pages_counter*PER_PAGE < total_clients:
        pages_counter += 1
        clients, total_clients = query_clients(pages_counter)
        clients = filter_clients(clients)
        bad_clients.extend(clients)
    return bad_clients


def got_visit(clid: str):
    url = "https://api.yclients.com/api/v1/company/{}/clients/visits/search".format(CID)
    querystring = {
        "client_id": clid,
        "client_phone": None,
        "from": filt.critical_date,
        "to": datetime.today().date().isoformat(),
        "payment_statuses": None,
        "attendance": None
    }
    response = requests.post(url, headers=h, json=querystring)
    ans = response.json()
    data = ans['data']['records']
    return len(data) > 0


def mark_client(cl_id):
    if got_visit(cl_id): return
    url=f'https://api.yclients.com/api/v1/company/{CID}/clients/{cl_id}/comments'
    querystring = {'text': 'Потерян'}
    response = requests.post(url, headers=h, params=querystring)

# ==EXOLVE==


def get_time(t_str: str):
    return datetime.strptime(t_str, '%H:%M:%S').time().hour


def get_time_range(recepient: str):
    payload = {'number': recepient}
    r = requests.post(r'https://api.exolve.ru/hlr/v1/GetBestCallTime', headers={'Authorization': 'Bearer '+exolve_api_key}, data=json.dumps(payload))
    print(r.text)
    if r.status_code == 200:
        ans = json.loads(r.text)
        text = ans['result']
        elems = text.split(',')
        since, till = [get_time(t_str) for t_str in elems]
    else:
        since, till = 12, 12
        ans = {}
    ans.update({'since': since, 'till': till})
    return ans


def get_multiple_hlr(clients: list[str]):
    data = '\n'.join(clients)
    st = str(base64.b64encode(data.encode())).replace("b'", '').replace("'", '')
    payload = {'numbers': st}
    r = requests.post(r'https://api.exolve.ru/hlr/v1/GenerateBestCallTimeReport', headers={'Authorization': 'Bearer '+exolve_api_key}, data=json.dumps(payload))
    print(r.text)

    def get_times(t_str: str):
        elems = t_str.split(',')
        since, till = [get_time(e) for e in elems]
        return (since + till)/2
    assert r.status_code == 200
    while True:
        r = requests.post(r'https://api.exolve.ru/hlr/v1/GetHLRReport',
                          headers={'Authorization': 'Bearer ' + exolve_api_key}, data=r.text)
        assert r.status_code == 200
        ans = json.loads(r.text)
        status = int(ans['status'])
        assert status < 5
        if status == 3 or status == 4:
            data = ans['base64']
            with open('phones_utf8.txt', 'wt') as f:
                f.write(base64.b64decode(data).decode("utf-8"))
            my_data = pd.read_csv('phones_utf8.txt').drop('Error', axis=1).set_index('Number').fillna(
                value='12:00:00,12:00:00')
            return my_data.map(get_times)
        time.sleep(10)



def send_SMS(recepient: str):
    payload = {'number': exolve_phone, 'destination': recepient, 'text': send_str}
    r = requests.post(r'https://api.exolve.ru/messaging/v1/SendSMS', headers={'Authorization': 'Bearer '+exolve_api_key}, data=json.dumps(payload))
    print(r.text)
    return r.text, r.status_code


# ==SCHEDULING==
schedule = Scheduler()


def add_tasks(client: dict, df: pd.DataFrame | None = None):
    cl_id, number = client['id'], client['phone']
    number = number.strip('+')
    if df is None:
        time_range = get_time_range(number)
        sending_time = (time_range['till'] + time_range['since'])/2
    else:
        sending_time = float(df.loc[number].values)

    def send_sms_():
        if got_visit(cl_id): return
        send_SMS(number)
    for ai in range(SMS_NUMBER):
        schedule.once(timedelta(days=ai, hours=sending_time), send_sms_)

    def mark_client_(): mark_client(cl_id)
    schedule.once(timedelta(minutes=SMS_NUMBER+1, seconds=sending_time), mark_client_)


def main_task():
    filt.update_filter()
    bad_clients = retrieve_by_visits()
    for bc in bad_clients:
        add_tasks(bc)


'''def main_task():
    filt.update_filter()
    bad_clients = retrieve_by_visits()
    df = get_multiple_hlr(bad_clients)
    for bc in bad_clients:
        add_tasks(bc, df)'''


if __name__ == "__main__":
    # get_multiple_hlr([str(n) for n in range(9101160000, 9101170000)])
    main_task()
