import os

import pandas as pd
import json
from io import BytesIO
import datetime
import dateutil.parser
import time
import boto3
import schedule

from dotenv import load_dotenv
from helper import initBoto3Session, get_s3_objs, get_req_handler, output_csv
from operator import itemgetter

############ CONFIG INIT BOILERPLATE ############
# load env vars
load_dotenv()

initBoto3Session()

# initialize S3 ServiceResource and bucket (init boto3 session first)
s3_resource = boto3.resource("s3")
data_bucket = s3_resource.Bucket(os.environ["BUCKET_NAME"])

############ END OF CONFIG INIT BOILERPLATE ############
WEATHER_DATA_DIR = os.environ["WEATHER_DATA_DIR"]
WEATHER_API_URL = os.environ["WEATHER_API_URL"]


def formatted_timestamp(timestamp):
    return dateutil.parser.parse(timestamp).strftime("%Y-%m-%dT%H%M%S")


def merge_bucket_csvs(bucket_dir, file_prefix, key_cols):
    # merge CSV files stored in Bucket (produced for each timestamp) into a single one
    search_key = bucket_dir + file_prefix
    res = get_s3_objs(data_bucket, search_key)
    merged_df = pd.DataFrame()
    for filename, stream_data in res.items():
        print(filename)
        tmp_df = pd.read_csv(BytesIO(stream_data.read()), index_col=0)
        if merged_df.empty:
            merged_df = tmp_df.copy()
        else:
            merged_df = pd.concat([merged_df, tmp_df], ignore_index=True)
    # merged dataframe may have duplicates rows (which have same values on the key_cols), these are dropped here
    merged_df = merged_df.drop_duplicates(
        subset=key_cols, keep="first", ignore_index=True
    )

    output_csv(
        merged_df,
        bucket_dir,
        file_prefix + "_merged.csv",
        output_loc="AWS",
        s3_bucket=data_bucket,
    )
    # TODO: delete the individual csvs programmatically after the merge? if so, how to do that?


def real_time_weather(output_loc="local"):
    api_endpoints = {
        "air_temp": "air-temperature",  # 5 stations
        "rainfall": "rainfall",  # 67 stations
        "RH%": "relative-humidity",  # 5 stations
        "wind_dir": "wind-direction",  # 5 stations
        "wind_speed": "wind-speed",  # 5 stations
    }

    call_timestamp = datetime.datetime.now()
    params = {"datetime": call_timestamp}

    R_df = pd.DataFrame()
    NR_df = pd.DataFrame()
    for (weather_field, endpoint_url) in api_endpoints.items():
        # print("Calling " + weather_field + " endpoint...")
        target_url = WEATHER_API_URL + endpoint_url
        # print("Sending request to " + target_url + " ...")
        resp = get_req_handler(target_url, params)

        resp_content = json.loads(resp.content.decode("utf-8"))

        readings = resp_content["items"][0]["readings"]
        reading_dict = {weather_field + "_realtime": [], "station_id": []}
        for r in readings:
            reading_dict[weather_field + "_realtime"].append(r["value"])
            reading_dict["station_id"].append(r["station_id"])
        reading_frame = pd.DataFrame(reading_dict)
        reading_frame.station_id = reading_frame.station_id.astype(str)

        stations = resp_content["metadata"]["stations"]
        station_dict = {
            "station_id": [],
            "station_loc": [],
            "station_name": [],
        }
        for s in stations:
            station_dict["station_id"].append(s["id"])
            station_dict["station_loc"].append(
                (s["location"]["latitude"], s["location"]["longitude"])
            )
            station_dict["station_name"].append(s["name"])
        stat_frame = pd.DataFrame(station_dict)
        stat_frame.station_id = stat_frame.station_id.astype(str)

        field_df = pd.merge(reading_frame, stat_frame, how="outer", on="station_id")
        field_df["timestamp"] = formatted_timestamp(
            resp_content["items"][0]["timestamp"]
        )

        if weather_field == "rainfall":
            R_df = field_df.copy()
        else:
            if NR_df.empty:
                NR_df = field_df.copy()
            else:
                NR_df = pd.merge(
                    field_df.copy(),
                    NR_df,
                    how="inner",
                    on=["station_id", "timestamp"],
                    suffixes=("_remove", ""),
                )
                NR_df.drop(
                    [c for c in NR_df.columns if "remove" in c],
                    axis=1,
                    inplace=True,
                )

        # print("Collected info into dataframe!")

    R_col_order = (
        ["timestamp"]
        + [c for c in R_df.columns if "station" in c]
        + [c for c in R_df.columns if "_realtime" in c]
    )
    NR_col_order = (
        ["timestamp"]
        + [c for c in NR_df.columns if "station" in c]
        + [c for c in NR_df.columns if "_realtime" in c]
    )
    R_df = R_df.reindex(columns=R_col_order)
    NR_df = NR_df.reindex(columns=NR_col_order)

    # rainfall dataframe saved to csv: rainfall-realtime+<timestamp>.csv
    R_filename = (
        "rainfall-realtime_"
        + pd.to_datetime(R_df["timestamp"]).min().strftime("%Y-%m-%dT%H%M%S")
        + ".csv"
    )
    # non-rainfall dataframe saved to csv: non-rainfall-realtime+<timestamp>.csv
    NR_filename = (
        "non-rainfall-realtime_"
        + pd.to_datetime(NR_df["timestamp"]).min().strftime("%Y-%m-%dT%H%M%S")
        + ".csv"
    )

    if output_loc == "return":
        return R_df, NR_df
    else:
        output_csv(
            R_df,
            WEATHER_DATA_DIR,
            R_filename,
            output_loc=output_loc,
            s3_bucket=data_bucket,
        )
        output_csv(
            NR_df,
            WEATHER_DATA_DIR,
            NR_filename,
            output_loc=output_loc,
            s3_bucket=data_bucket,
        )


def forecast_weather_2HR(output_loc="local"):
    # collect 2-hour-forecast data:
    call_timestamp = datetime.datetime.now()
    params = {"datetime": call_timestamp}

    forecast_field = "forecast_2hr"
    endpoint_url = "2-hour-weather-forecast"
    # print("Calling " + forecast_field + " endpoint...")
    target_url = WEATHER_API_URL + endpoint_url
    # print("Sending request to " + target_url + " ...")
    resp = get_req_handler(target_url, params)
    resp_content = json.loads(resp.content.decode("utf-8"))

    resp_items = resp_content["items"][0]
    forecasts, valid_period = itemgetter("forecasts", "valid_period")(resp_items)

    valid_start, valid_end = formatted_timestamp(
        valid_period["start"]
    ), formatted_timestamp(valid_period["end"])

    forecast_dict = {"forecast_2hr_area": [], "forecast_2hr_value": []}
    for fr in forecasts:
        forecast_dict["forecast_2hr_value"].append(fr["forecast"])
        forecast_dict["forecast_2hr_area"].append(fr["area"])
    forecast_frame = pd.DataFrame(forecast_dict)
    forecast_frame.forecast_2hr_area = forecast_frame.forecast_2hr_area.astype(str)

    resp_area = resp_content["area_metadata"]

    area_dict = {"forecast_2hr_area": [], "forecast_2hr_area_loc": []}
    for ar in resp_area:
        area_dict["forecast_2hr_area"].append(ar["name"])
        area_dict["forecast_2hr_area_loc"].append(
            (ar["label_location"]["latitude"], ar["label_location"]["longitude"])
        )
    area_frame = pd.DataFrame(area_dict)

    area_frame.forecast_2hr_area = area_frame.forecast_2hr_area.astype(str)

    forecast_2HR_frame = pd.merge(area_frame, forecast_frame, on="forecast_2hr_area")

    forecast_2HR_frame["2hr_start"] = valid_start
    forecast_2HR_frame["2hr_end"] = valid_end

    # filename based on start of valid period for the forecast (example: 143000 -> 163000)
    forecast_2HR_filename = "forecast-2HR_" + valid_start + ".csv"

    # output frames
    if output_loc == "return":
        return forecast_2HR_frame
    else:
        output_csv(
            forecast_2HR_frame,
            WEATHER_DATA_DIR,
            forecast_2HR_filename,
            output_loc=output_loc,
            s3_bucket=data_bucket,
        )


# scheduling weather data ingestion tasks:

RT_SCHEDULE_FREQ = 10
schedule.every(RT_SCHEDULE_FREQ).minutes.do(real_time_weather, output_loc="AWS")
print(
    f"Init scheduler to run real time weather data ingest job every {RT_SCHEDULE_FREQ} mins"
)

FR_2HR_SCHEDULE_FREQ = 30
schedule.every(FR_2HR_SCHEDULE_FREQ).minutes.do(forecast_weather_2HR, output_loc="AWS")
print(
    f"Init scheduler to run 2hr weather forecast data ingest job every {FR_2HR_SCHEDULE_FREQ} mins"
)

while True:
    schedule.run_pending()
    time.sleep(1)


# one-time calls, uncomment and run as needed

# real_time_weather(output_loc="AWS")
# forecast_weather_2HR(output_loc="AWS")
# merge_bucket_csvs(WEATHER_DATA_DIR, "forecast-2HR", ["forecast_2hr_area", "2hr_start"])
# merge_bucket_csvs(WEATHER_DATA_DIR, "rainfall-realtime", ["timestamp", "station_id"])
# merge_bucket_csvs(
#     WEATHER_DATA_DIR, "non-rainfall-realtime", ["timestamp", "station_id"]
# )
