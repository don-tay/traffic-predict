import sys
import os
from io import StringIO

import pandas as pd
import json
import datetime
import dateutil.parser
import time

import boto3
import schedule

from dotenv import load_dotenv
from helper import get_req_handler, initBoto3Session
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


def real_time_weather(output="local"):
    api_domain = WEATHER_API_URL
    api_endpoints = {
        "air_temp": "air-temperature",  # 5 stations
        "rainfall": "rainfall",  # 67 stations
        "RH%": "relative-humidity",  # 5 stations
        "wind_dir": "wind-direction",  # 5 stations
        "wind_speed": "wind-speed",  # 5 stations
    }

    call_timestamp = datetime.datetime.now()
    params = {"datetime": call_timestamp}

    rainfall_df = pd.DataFrame()
    non_rainfall_df = pd.DataFrame()
    for (weather_field, endpoint_url) in api_endpoints.items():
        print("Calling " + weather_field + " endpoint...")
        target_url = api_domain + endpoint_url
        print("Sending request to " + target_url + " ...")
        resp = get_req_handler(target_url, params)

        resp_content = json.loads(resp.content.decode("utf-8"))
        readings = resp_content["items"][0]["readings"]
        stations = resp_content["metadata"]["stations"]
        reading_frame = pd.DataFrame(
            {
                weather_field + "_realtime": [r["value"] for r in readings],
                "station_id": [r["station_id"] for r in readings],
            }
        )
        reading_frame.station_id = reading_frame.station_id.astype(str)
        stat_frame = pd.DataFrame(
            {
                "station_id": [s["id"] for s in stations],
                "station_loc": [
                    (s["location"]["latitude"], s["location"]["longitude"])
                    for s in stations
                ],
                "station_name": [s["name"] for s in stations],
            }
        )
        stat_frame.station_id = stat_frame.station_id.astype(str)
        field_df = pd.merge(reading_frame, stat_frame, how="outer", on="station_id")

        field_df["timestamp"] = formatted_timestamp(
            resp_content["items"][0]["timestamp"]
        )
        if weather_field == "rainfall":
            rainfall_df = field_df.copy()
        else:
            if non_rainfall_df.empty:
                non_rainfall_df = field_df.copy()
            else:
                non_rainfall_df = pd.merge(
                    field_df.copy(),
                    non_rainfall_df,
                    how="inner",
                    on=["station_id", "timestamp"],
                    suffixes=("_remove", ""),
                )
                non_rainfall_df.drop(
                    [c for c in non_rainfall_df.columns if "remove" in c],
                    axis=1,
                    inplace=True,
                )

        print("Collected info into dataframe!")

    R_col_order = (
        ["timestamp"]
        + [c for c in rainfall_df.columns if "station" in c]
        + [c for c in rainfall_df.columns if "_realtime" in c]
    )
    NR_col_order = (
        ["timestamp"]
        + [c for c in non_rainfall_df.columns if "station" in c]
        + [c for c in non_rainfall_df.columns if "_realtime" in c]
    )
    rainfall_df = rainfall_df.reindex(columns=R_col_order)
    non_rainfall_df = non_rainfall_df.reindex(columns=NR_col_order)

    # rainfall dataframe saved to csv: rainfall-realtime+<timestamp>.csv
    R_filename = WEATHER_DATA_DIR + (
        "rainfall-realtime_"
        + pd.to_datetime(rainfall_df["timestamp"]).min().strftime("%Y-%m-%dT%H%M%S")
        + ".csv"
    )
    # non-rainfall dataframe saved to csv: non-rainfall-realtime+<timestamp>.csv
    NR_filename = WEATHER_DATA_DIR + (
        "non-rainfall-realtime_"
        + pd.to_datetime(non_rainfall_df["timestamp"]).min().strftime("%Y-%m-%dT%H%M%S")
        + ".csv"
    )

    if output == "return":
        return rainfall_df, non_rainfall_df
    elif output == "local":
        if not os.path.exists(WEATHER_DATA_DIR):
            os.makedirs(WEATHER_DATA_DIR)
        rainfall_df.to_csv(R_filename, encoding="utf-8")
        non_rainfall_df.to_csv(NR_filename, encoding="utf-8")
    elif output == "AWS":
        try:
            # upload rainfall dataframe to S3 bucket
            R_csv_buffer = StringIO()
            rainfall_df.to_csv(R_csv_buffer, encoding="utf-8")
            data_bucket.put_object(Body=R_csv_buffer.getvalue(), Key=R_filename)
        except Exception:
            print("Failed to upload rainfall data in " + R_filename, file=sys.stderr)
        else:
            print("Uploaded rainfall data in " + R_filename + " to bucket!")

        try:
            # upload non-rainfall dataframe to S3 bucket
            NR_csv_buffer = StringIO()
            non_rainfall_df.to_csv(NR_csv_buffer, encoding="utf-8")
            data_bucket.put_object(Body=NR_csv_buffer.getvalue(), Key=NR_filename)
        except Exception:
            print(
                "Failed to upload non-rainfall data in " + NR_filename, file=sys.stderr
            )
        else:
            print("Uploaded non-rainfall data in " + NR_filename + " to bucket!")


# scheduling weather data ingestion tasks:

schedule.every(10).minutes.do(real_time_weather, output="AWS")
print("Init scheduler to run ingest job every 5 min")

while True:
    schedule.run_pending()
    time.sleep(1)
