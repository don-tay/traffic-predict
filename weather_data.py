import json

import pandas as pd
import requests
import datetime
import dateutil.parser


def get_req_handler(url, params, retry=True):
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print("GET request to " + url + " failed with status code " + response.status_code)
        if retry:
            max_tries = 10
            attempts = 0
            while response.status_code != 200:
                print("Request failed, retrying...")
                response = requests.get(url, params=params)
                attempts += 1
                if attempts >= max_tries:
                    break
    return response


def formatted_timestamp(timestamp):
    return dateutil.parser.parse(timestamp).strftime('%Y-%m-%dT%H%M%S')


def real_time_weather():
    api_domain = "https://api.data.gov.sg/v1/environment/"
    api_endpoints = {
        "air_temp": "air-temperature",  # 5 stations
        "rainfall": "rainfall",  # 67 stations
        "RH%": "relative-humidity",  # 5 stations
        "wind_dir": "wind-direction",  # 5 stations
        "wind_speed": "wind-speed"  # 5 stations
    }

    call_timestamp = datetime.datetime.now()
    params = {"datetime": call_timestamp}

    real_time_dfs = {}

    for (weather_field, endpoint_url) in api_endpoints.items():
        print("Calling " + weather_field + " endpoint...")
        target_url = api_domain + endpoint_url
        print("Sending request to " + target_url + " ...")
        resp = get_req_handler(target_url, params)

        resp_content = json.loads(resp.content.decode('utf-8'))
        readings = resp_content['items'][0]['readings']
        stations = resp_content['metadata']['stations']
        reading_frame = pd.DataFrame({
            weather_field + "_realtime": [r['value'] for r in readings],
            "station_id": [r['station_id'] for r in readings]
        })
        reading_frame.station_id = reading_frame.station_id.astype(str)
        stat_frame = pd.DataFrame({
            "station_id": [s['id'] for s in stations],
            "station_loc": [(s['location']['latitude'], s['location']['longitude']) for s in stations],
            "station_name": [s['name'] for s in stations]
        })
        stat_frame.station_id = stat_frame.station_id.astype(str)
        field_df = pd.merge(reading_frame, stat_frame, how='outer', on='station_id')
        field_df['timestamp'] = formatted_timestamp(resp_content['items'][0]['timestamp'])
        real_time_dfs[weather_field] = field_df.copy()
        print("Collected info into dataframe!")

    return real_time_dfs


# try the above function here
real_time_weather_dfs = real_time_weather()
for (field, df) in real_time_weather_dfs.items():
    print("Field:", field)
    print(df)
