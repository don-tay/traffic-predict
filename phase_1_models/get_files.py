from helper import get_req_handler, initBoto3Session, get_s3_objs
from dotenv import load_dotenv
import boto3
import os
from io import StringIO

import pandas as pd

def get_super_table():
    load_dotenv()

    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]

    client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    bucket_name = os.environ["BUCKET_NAME"]
    weather_dir = os.environ["WEATHER_DIR"]
    bing_dir = os.environ["BING_DIR"]

    def get_csv(file_name, data_dir):
        csv_obj = client.get_object(Bucket=bucket_name, Key= data_dir + file_name)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_string))
        df = df.drop("Unnamed: 0", axis = 1)
        return df

    # Camera to locations 
    cam_to_loc = df = get_csv("camera_station_mapping.csv", "").rename(
        columns = {"rainfall" : "rainfall_station_id",
                   "not_rainfall" : "non_rainfall_station_id",
                   "region" : "2hr_forecast_area"})

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