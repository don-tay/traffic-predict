import os
from pprint import pprint

import pandas as pd
import json
import datetime

import boto3
from dotenv import load_dotenv
from helper import (
    getCurrentDateTime,
    initBoto3Session,
    get_req_handler,
    formatted_timestamp,
    log_json,
    output_csv,
    merge_bucket_csvs,
)
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


def real_time_weather(
    output_loc="local", call_timestamp=getCurrentDateTime(), log_loc=None
):
    api_endpoints = {
        "air_temp": "air-temperature",  # 5 stations
        "rainfall": "rainfall",  # 68 stations
        "RH%": "relative-humidity",  # 5 stations
        "wind_dir": "wind-direction",  # 5 stations
        "wind_speed": "wind-speed",  # 5 stations
    }
    call_timestamp_str = formatted_timestamp(call_timestamp)
    params = {"date_time": call_timestamp.strftime("%Y-%m-%dT%H:%M:%S")}

    R_df = pd.DataFrame()
    NR_df = pd.DataFrame()
    for (weather_field, endpoint_url) in api_endpoints.items():
        target_url = WEATHER_API_URL + endpoint_url
        resp = get_req_handler(target_url, params)
        resp_content = json.loads(resp.content.decode("utf-8"))

        if log_loc:
            log_file = f"realtime-{weather_field}__{call_timestamp_str}"
            log_json(
                resp_content,
                WEATHER_DATA_DIR,
                log_file,
                output_loc=log_loc,
                s3_bucket=data_bucket,
            )

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

    R_df["call_timestamp"] = call_timestamp_str
    NR_df["call_timestamp"] = call_timestamp_str
    # rainfall dataframe saved to csv: rainfall-realtime+<call_timestamp>.csv
    R_filename = "rainfall-realtime_" + call_timestamp_str + ".csv"
    # non-rainfall dataframe saved to csv: non-rainfall-realtime+<call_timestamp>.csv
    NR_filename = "non-rainfall-realtime_" + call_timestamp_str + ".csv"

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


def forecast_weather_2HR(output_loc="local", call_timestamp=getCurrentDateTime()):
    call_timestamp_str = formatted_timestamp(call_timestamp)
    params = {"date_time": call_timestamp.strftime("%Y-%m-%dT%H:%M:%S")}

    forecast_field = "2hr_forecast_"
    endpoint_url = "2-hour-weather-forecast"
    target_url = WEATHER_API_URL + endpoint_url
    resp = get_req_handler(target_url, params)
    resp_content = json.loads(resp.content.decode("utf-8"))

    resp_items = resp_content["items"][0]
    forecasts, valid_period = itemgetter("forecasts", "valid_period")(resp_items)

    valid_start, valid_end = formatted_timestamp(
        valid_period["start"]
    ), formatted_timestamp(valid_period["end"])

    forecast_dict = {forecast_field + "area": [], forecast_field + "value": []}
    for fr in forecasts:
        forecast_dict[forecast_field + "value"].append(fr["forecast"])
        forecast_dict[forecast_field + "area"].append(fr["area"])
    FR_frame = pd.DataFrame(forecast_dict)
    FR_frame["2hr_forecast_area"] = FR_frame["2hr_forecast_area"].astype(str)

    resp_area = resp_content["area_metadata"]

    area_dict = {forecast_field + "area": [], forecast_field + "area_loc": []}
    for ar in resp_area:
        area_dict[forecast_field + "area"].append(ar["name"])
        area_dict[forecast_field + "area_loc"].append(
            (ar["label_location"]["latitude"], ar["label_location"]["longitude"])
        )
    area_frame = pd.DataFrame(area_dict)
    area_frame["2hr_forecast_area"] = area_frame["2hr_forecast_area"].astype(str)

    forecast_2HR_frame = pd.merge(area_frame, FR_frame, on=forecast_field + "area")

    forecast_2HR_frame["2hr_start"] = valid_start
    forecast_2HR_frame["2hr_end"] = valid_end

    forecast_2HR_frame["call_timestamp"] = call_timestamp_str
    # filename based on call_timestamp
    forecast_2HR_filename = "forecast-2HR_" + call_timestamp_str + ".csv"

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


def forecast_weather_24HR(output_loc="local", call_timestamp=getCurrentDateTime()):
    call_timestamp_str = formatted_timestamp(call_timestamp)
    params = {"date_time": call_timestamp.strftime("%Y-%m-%dT%H:%M:%S")}

    endpoint_url = "24-hour-weather-forecast"

    target_url = WEATHER_API_URL + endpoint_url

    resp = get_req_handler(target_url, params)
    resp_content = json.loads(resp.content.decode("utf-8"))
    resp_items = resp_content["items"][0]
    general, periods, valid_period = itemgetter("general", "periods", "valid_period")(
        resp_items
    )

    # process general forecasts for the day (full 24 hr)
    general_timing_frame = pd.json_normalize(valid_period)
    general_timing_frame = general_timing_frame.rename(
        columns={c: "24hr_" + c for c in general_timing_frame.columns}
    )
    general_timing_frame = general_timing_frame.applymap(
        lambda c: formatted_timestamp(c)
    )

    general_forecast_frame = pd.json_normalize(general, sep="_")
    general_forecast_frame = general_forecast_frame.rename(
        columns={c: "24hr_general_" + c for c in general_forecast_frame.columns}
    )

    # process regional forecasts for each time-period (6hr, 12 hr, 6 hr)
    period_forecast_dict = {}
    p_count = 1
    for p in periods:
        prefix = "24hr_period_" + str(p_count) + "_"
        period_forecast_dict[prefix + "start"] = formatted_timestamp(p["time"]["start"])
        period_forecast_dict[prefix + "end"] = formatted_timestamp(p["time"]["end"])
        for (r, v) in p["regions"].items():
            period_forecast_dict[prefix + "forecast_" + r] = v
        p_count += 1

    period_forecast_frame = pd.json_normalize(period_forecast_dict)

    forecast_24HR_frame = pd.concat(
        [general_timing_frame, general_forecast_frame, period_forecast_frame], axis=1
    )
    forecast_24HR_frame["call_timestamp"] = call_timestamp_str
    forecast_24HR_filename = "forecast-24HR_" + call_timestamp_str + ".csv"

    if output_loc == "return":
        return forecast_24HR_frame
    else:
        output_csv(
            forecast_24HR_frame,
            WEATHER_DATA_DIR,
            forecast_24HR_filename,
            output_loc=output_loc,
            s3_bucket=data_bucket,
        )


def forecast_weather_4DAY(output_loc="local", call_timestamp=getCurrentDateTime()):
    call_timestamp_str = formatted_timestamp(call_timestamp)
    params = {"date_time": call_timestamp.strftime("%Y-%m-%dT%H:%M:%S")}

    endpoint_url = "4-day-weather-forecast"

    target_url = WEATHER_API_URL + endpoint_url

    resp = get_req_handler(target_url, params)
    resp_content = json.loads(resp.content.decode("utf-8"))
    resp_items = resp_content["items"][0]
    forecasts, update_timestamp = itemgetter("forecasts", "update_timestamp")(
        resp_items
    )

    day_forecasts = []

    for f in forecasts:
        day_df = pd.json_normalize(f, sep="_")
        day_df.drop(["timestamp"], axis=1, inplace=True)
        day_forecasts.append(day_df.copy())
    day_forecast_frame = pd.concat(day_forecasts, axis=0)
    day_forecast_frame["update_timestamp"] = formatted_timestamp(update_timestamp)

    forecast_4DAY_frame = day_forecast_frame.rename(
        columns={c: "4day_" + c for c in day_forecast_frame.columns}
    )
    forecast_4DAY_frame["4day_date"] = forecast_4DAY_frame["4day_date"].apply(
        formatted_timestamp
    )
    forecast_4DAY_frame["call_timestamp"] = call_timestamp_str
    # name based on call_timestamp
    forecast_4DAY_filename = "forecast-4DAY_" + call_timestamp_str + ".csv"
    if output_loc == "return":
        return forecast_4DAY_frame
    else:
        output_csv(
            forecast_4DAY_frame,
            WEATHER_DATA_DIR,
            forecast_4DAY_filename,
            output_loc=output_loc,
            s3_bucket=data_bucket,
        )


def merge_weather_csvs(drop_duplicates=False):

    table_name_key_cols = {
        "forecast-2HR": ["2hr_forecast_area", "2hr_start"],
        "forecast-24HR": ["24hr_start"],
        "forecast-4DAY": ["4day_date"],
        "rainfall-realtime": ["timestamp", "station_id"],
        "non-rainfall-realtime": ["timestamp", "station_id"],
    }
    for table_name in table_name_key_cols.keys():
        print(f"Merging {table_name}...")
        if drop_duplicates:
            key_cols = table_name_key_cols[table_name]
        else:
            key_cols = None
        merge_bucket_csvs(
            data_bucket,
            WEATHER_DATA_DIR,
            table_name,
            key_cols,
        )


# one-time calls, uncomment and run as needed

# real_time_weather(output_loc="AWS", log_loc="AWS")
# forecast_weather_2HR(output_loc="AWS")
# forecast_weather_24HR(output_loc="AWS")
# forecast_weather_4DAY(output_loc="AWS")
# merge_weather_csvs()
