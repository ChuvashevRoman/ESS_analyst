import requests
import pandas as pd


class Server:

    def __init__(self, ip_address, port):
        self.ip = ip_address
        self.port = port

    # Чтение временных рядов по REST API
    def get_data(self, element, mrid, param, purposeKey, start_date, end_date):
        url = f'http://{self.ip}:{self.port}/api/{element}/{mrid}/{param}/row?purposeKey={purposeKey}&start={start_date}&end={end_date}'
        req = requests.get(url)
        val = []
        time = []
        for item in req.json():
            val.append(float(item['value']))
            time.append(pd.Timestamp(item['timeStamp']))
        df = pd.DataFrame({'time': time, 'val': val})
        return df

    def post_data(self, mrid, telpoint, purposeKey, value):
        url = f'http://{self.ip}:{self.port}/api/values'
        path = f'/energyStoragingUnit[MRID=\"{mrid}\"]/{telpoint}'
        req = requests.post(url, json={"path": path,
                                       "purposeKey": purposeKey, "value": value})
        print(req)
        print(req.request.body)
