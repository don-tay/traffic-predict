from helper import get_req_handler, initBoto3Session, get_s3_objs
from dotenv import load_dotenv
import boto3
import os
from io import StringIO

import pandas as pd

import json
import requests

from math import sin, cos, sqrt, atan2, radians

import numpy as np

def get_super_table():
    
    # Helper function 
    def get_distance(lat1, lat2, lon1, lon2):
        """
        Code formula taken from 
        https://stackoverflow.com/questions/19412462/getting-distance-between-two-points-based-on-latitude-longitude
        """
        lat1 = radians(lat1)
        lon1 = radians(lon1)
        lat2 = radians(lat2)
        lon2 = radians(lon2)
        R = 6373.0
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        distance = R * c
        return distance
    
    def get_stations(file_name, data_dir):
        csv_obj = client.get_object(Bucket=bucket_name, Key= data_dir + file_name)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_string))
        df = df.drop("Unnamed: 0", axis = 1)
        df = df[["station_id", "station_loc"]].drop_duplicates()
        return df
    
    def get_csv(file_name, data_dir):
        csv_obj = client.get_object(Bucket=bucket_name, Key= data_dir + file_name)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_string))
        df = df.drop("Unnamed: 0", axis = 1)
        return df
    
    # Load Environment 
        
    load_dotenv()

    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]

    client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    bucket_name = os.environ["BUCKET_NAME"]
    weather_dir = os.environ["WEATHER_DIR"]
    bing_dir = os.environ["BING_DIR"]

    # Camera to locations 
    
    rainfall = get_stations("rainfall-realtime.csv", weather_dir)
    non_rainfall = get_stations("non-rainfall-realtime.csv", weather_dir)
    
    keep = ["S24", "S107"] # need to manual inspect and change 
    non_rainfall = non_rainfall[non_rainfall.station_id.isin(keep)]
    
    non_rainfall["not_rain_lat"] = non_rainfall.station_loc.apply(lambda x: float(x.split(", ")[0][1:]))
    non_rainfall["not_rain_lon"] = non_rainfall.station_loc.apply(lambda x: float(x.split(", ")[1][:-1]))
    
    rainfall["rain_lat"] = rainfall.station_loc.apply(lambda x: float(x.split(", ")[0][1:]))
    rainfall["rain_lon"] = rainfall.station_loc.apply(lambda x: float(x.split(", ")[1][:-1]))
    
    non_rainfall = non_rainfall.rename(columns = {"station_id" : "not_rainfall"}).drop("station_loc", axis = 1)
    rainfall = rainfall.rename(columns = {"station_id" : "rainfall"}).drop("station_loc", axis = 1)
    
    def get_camera_loc():
        URL = "https://api.data.gov.sg/v1/transport/traffic-images"
        response = requests.get(URL, timeout=10)
        resp_content = json.loads(response.content.decode('utf-8'))
        cameras = resp_content['items'][0]['cameras']

        df_rows = []

        for x in cameras:
            cam_id = x["camera_id"]
            location = x["location"]
            lat = location["latitude"]
            lon = location["longitude"]
            df_rows.append([cam_id, lat, lon])
        
        return pd.DataFrame(df_rows, columns = ["cam_id", "cam_lat", "cam_lon"])
    
    camera_df = get_camera_loc()
    count = 0 
    while (len(camera_df) != 87):
        camera_df = get_camera_loc()
        count += 1
        if count == 3:
            print("FAILED")
            return None
    
    df = camera_df.merge(non_rainfall, how='cross')
    
    df["not_rain_dist"] = df.apply(lambda x : get_distance(x.cam_lat, x.not_rain_lat, x.cam_lon, x.not_rain_lon), axis = 1)
    idx = df.groupby(['cam_id'])['not_rain_dist'].transform(min) == df['not_rain_dist']
    df = df[idx]
    
    df2 = df.merge(rainfall, how = "cross")
    df2["rain_dist"] = df2.apply(lambda x : get_distance(x.cam_lat, x.rain_lat, x.cam_lon, x.rain_lon), axis = 1)
    idx2 = df2.groupby(['cam_id'])['rain_dist'].transform(min) == df2['rain_dist']
    df2 = df2[idx2]
    
    area = get_csv("area_lat_lon.csv", "")
    
    df["not_rain_dist"] = df.apply(lambda x : get_distance(x.cam_lat, x.not_rain_lat, x.cam_lon, x.not_rain_lon), axis = 1)
    idx = df.groupby(['cam_id'])['not_rain_dist'].transform(min) == df['not_rain_dist']
    df = df[idx]
    
    df2 = df.merge(rainfall, how = "cross")
    df2["rain_dist"] = df2.apply(lambda x : get_distance(x.cam_lat, x.rain_lat, x.cam_lon, x.rain_lon), axis = 1)
    idx2 = df2.groupby(['cam_id'])['rain_dist'].transform(min) == df2['rain_dist']
    df2 = df2[idx2]
    
    area["area_lat"] = area.coord.apply(lambda x: float(x.split(", ")[0]))
    area["area_lon"] = area.coord.apply(lambda x: float(x.split(", ")[1]))
    area = area.drop("coord", axis = 1)
    
    df3 = df2.merge(area, how = "cross")
    df3["area_dist"] = df3.apply(lambda x : get_distance(x.cam_lat, x.area_lat, x.cam_lon, x.area_lon), axis = 1)
    idx3 = df3.groupby(['cam_id'])['area_dist'].transform(min) == df3['area_dist']
    df3 = df3[idx3]
    
    compass = ["north", "south", "east", "west", "central"]
    compass_lat = [1.4171600757873817,
                   1.2689570746680328,
                   1.3474858448902267,
                   1.3529774835842006,
                   1.3569871019585316]
    compass_lon = [103.80906027423956,
                   103.82763148734753,
                   103.94089620103182,
                   103.70537681098106,
                   103.82099141380705]
    compass_df = pd.DataFrame([compass, compass_lat, compass_lon]).T.\
    rename(columns = {0 : "compass", 1 : "compass_lat", 2: "compass_lon"})
    
    df4 = df3.merge(compass_df, how = "cross")
    df4["compass_dist"] = df4.apply(lambda x : get_distance(x.cam_lat, x.compass_lat, x.cam_lon, x.compass_lon), axis = 1)
    idx4 = df4.groupby(['cam_id'])['compass_dist'].transform(min) == df4['compass_dist']
    df4 = df4[idx4]
    
    df_final = df4[["cam_id", "not_rainfall", "rainfall", "region", "compass"]].sort_values("cam_id").reset_index().drop("index", axis = 1)
    
    cam_to_loc = df_final.rename(columns = {"rainfall" : "rainfall_station_id",
                                        "not_rainfall" : "non_rainfall_station_id",
                                        "region" : "2hr_forecast_area"})
    
    cam_to_loc.cam_id = cam_to_loc.cam_id.apply(lambda x : int(x))
    
    # Read each file 
    rainfall_realtime = get_csv("rainfall-realtime.csv", weather_dir)
    non_rainfall_realtime = get_csv("non-rainfall-realtime.csv", weather_dir)
    forecast_4d = get_csv("forecast-4DAY.csv", weather_dir)
    forecast_24h = get_csv("forecast-24HR.csv", weather_dir)
    forecast_2h = get_csv("forecast-2HR.csv", weather_dir)
    bing_data = get_csv("all-congestion-levels.csv", bing_dir)
    bing_key_table = get_csv("route_data.csv", bing_dir)

    # Process Rainfall Realtime 
    rainfall_realtime = rainfall_realtime[["call_timestamp", "station_id", "rainfall_realtime"]]
    rainfall_realtime = rainfall_realtime.rename(columns = {"station_id" : "rainfall_station_id"})

    # Process Non Rainfall Realtime
    non_rainfall_realtime = non_rainfall_realtime.rename(columns = {"RH%_realtime" : "humidity_realtime", 
                                                                   "station_id" : "non_rainfall_station_id"})
    non_rainfall_realtime = non_rainfall_realtime[["call_timestamp", "non_rainfall_station_id", "wind_speed_realtime", 
                                                   "wind_dir_realtime", "humidity_realtime", "air_temp_realtime"]]

    # Process 4 day Forecast 
    forecast_4d["4day_date"] = pd.to_datetime(forecast_4d["4day_date"])
    forecast_4d["grp_row_num"] = forecast_4d.sort_values("4day_date")\
                                .groupby(["4day_update_timestamp", "call_timestamp"])["4day_date"].cumcount()+1
    forecast_4d["grp_row_num"] = forecast_4d.grp_row_num.apply(lambda x : str(x))
    forecast_4d = forecast_4d.drop("4day_date", axis = 1)
    forecast_4d = forecast_4d.pivot(index = "call_timestamp", columns = "grp_row_num")
    forecast_4d.columns = ['_'.join(col) for col in forecast_4d.columns]
    forecast_4d = forecast_4d.drop(["4day_update_timestamp_1", "4day_update_timestamp_2", 
                      "4day_update_timestamp_3", "4day_update_timestamp_4"], axis = 1)
    forecast_4d = forecast_4d.reset_index()

    # Processs 24h 
    forecast_24h = forecast_24h.drop(["24hr_period_4_start", "24hr_period_4_end", "24hr_period_4_forecast_west", 
                                     "24hr_period_4_forecast_east", "24hr_period_4_forecast_central", 
                                     "24hr_period_4_forecast_south", "24hr_period_4_forecast_north"], axis = 1)

    long_df1 = pd.melt(forecast_24h, id_vars = ["call_timestamp", "24hr_period_1_start", "24hr_period_1_end"],
            value_vars= ["24hr_period_1_forecast_west", 
                                     "24hr_period_1_forecast_east", "24hr_period_1_forecast_central", 
                                     "24hr_period_1_forecast_south", "24hr_period_1_forecast_north"])

    long_df2 = pd.melt(forecast_24h, id_vars = ["call_timestamp", "24hr_period_2_start", "24hr_period_2_end"],
            value_vars= ["24hr_period_2_forecast_west", 
                                     "24hr_period_2_forecast_east", "24hr_period_2_forecast_central", 
                                     "24hr_period_2_forecast_south", "24hr_period_2_forecast_north"])

    long_df3 = pd.melt(forecast_24h, id_vars = ["call_timestamp", "24hr_period_3_start", "24hr_period_3_end"],
            value_vars= ["24hr_period_3_forecast_west", 
                                     "24hr_period_3_forecast_east", "24hr_period_3_forecast_central", 
                                     "24hr_period_3_forecast_south", "24hr_period_3_forecast_north"])

    long_df1["compass"] = long_df1.variable.apply(lambda x : x.split("_")[-1])
    long_df2["compass"] = long_df2.variable.apply(lambda x : x.split("_")[-1])
    long_df3["compass"] = long_df3.variable.apply(lambda x : x.split("_")[-1])

    long_df1 = long_df1.drop("variable", axis = 1).rename(columns = {"value" : "24hr_period_1"})
    long_df2 = long_df2.drop("variable", axis = 1).rename(columns = {"value" : "24hr_period_2"})
    long_df3 = long_df3.drop("variable", axis = 1).rename(columns = {"value" : "24hr_period_3"})

    forecast_24h = forecast_24h.drop(["24hr_period_1_forecast_west", 
                                     "24hr_period_1_forecast_east", "24hr_period_1_forecast_central", 
                                     "24hr_period_1_forecast_south", "24hr_period_1_forecast_north", 
                                     "24hr_period_2_forecast_west", 
                                     "24hr_period_2_forecast_east", "24hr_period_2_forecast_central", 
                                     "24hr_period_2_forecast_south", "24hr_period_2_forecast_north", 
                                     "24hr_period_3_forecast_west", 
                                     "24hr_period_3_forecast_east", "24hr_period_3_forecast_central", 
                                     "24hr_period_3_forecast_south", "24hr_period_3_forecast_north", 
                                     "24hr_period_1_start", "24hr_period_1_end", 
                                     "24hr_period_2_start", "24hr_period_2_end", 
                                     "24hr_period_3_start", "24hr_period_3_end"], axis = 1)

    forecast_24h = forecast_24h\
    .merge(long_df1, how = "inner", on = "call_timestamp")\
    .merge(long_df2, how = "inner", on = ["call_timestamp", "compass"])\
    .merge(long_df3, how = "inner", on = ["call_timestamp", "compass"])

    # Process 2h 
    forecast_2h = forecast_2h.drop(["2hr_forecast_area_loc", "2hr_start", "2hr_end"], axis = 1)

    # Process Bing 
    bing_key_table = bing_key_table[["camera_id", "direction"]].rename(columns = {"camera_id" : "cam_id"})
    bing_data = bing_data[["camera_id", "direction", "call_timestamp", "trafficCongestion"]].rename(columns = {"camera_id" : "cam_id"})

    # Combine Tables 
    cam_all = cam_to_loc.merge(bing_key_table, on = "cam_id")

    all_time = set(rainfall_realtime.call_timestamp)\
    .intersection(set(non_rainfall_realtime.call_timestamp))\
    .intersection(set(forecast_4d.call_timestamp))\
    .intersection(set(forecast_24h.call_timestamp))\
    .intersection(set(forecast_2h.call_timestamp))\
    .intersection(set(bing_data.call_timestamp))

    all_time = pd.DataFrame(all_time, columns = ["call_timestamp"])
    base_df = all_time.merge(cam_all, how = "cross") 
    df = base_df.merge(rainfall_realtime, on = ["call_timestamp", "rainfall_station_id"], how = "left")
    df2 = df.merge(non_rainfall_realtime, on = ["call_timestamp", "non_rainfall_station_id"], how = "left")
    df3 = df2.merge(forecast_2h, on = ["call_timestamp", "2hr_forecast_area"], how = "left")
    df4 = df3.merge(forecast_4d, on = ["call_timestamp"])
    df5 = df4.merge(forecast_24h, on = ["call_timestamp", "compass"])
    df6 = df5.merge(bing_data, on = ["call_timestamp", "cam_id", "direction"])
    
    return df6