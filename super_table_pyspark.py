from datetime import datetime
from pprint import pprint
import re
from db.super_tbl import copy_csv_to_db, spark_df_to_db


from helper import (
    get_req_handler,
    getCurrentDateTime,
    formatted_timestamp,
)
from csv_schemas import *
from dotenv import load_dotenv
import boto3
import os
from io import StringIO

import pandas as pd
import json

from math import sin, cos, sqrt, atan2, radians

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


# Load Environment

load_dotenv()

aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]

client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

bucket_name = os.environ["BUCKET_NAME"]
weather_dir = os.environ["WEATHER_DATA_DIR"]
bing_dir = os.environ["BING_DATA_DIR"]

# Spark Config
# can vary number of cores used for local, and number of cores/memory for driver/executors, see:
# https://stackoverflow.com/questions/32356143/what-does-setmaster-local-mean-in-spark
# https://stackoverflow.com/questions/24622108/apache-spark-the-number-of-cores-vs-the-number-of-executors
# https://stackoverflow.com/questions/63387253/pyspark-setting-executors-cores-and-memory-local-machine
# using more cores may be slower due to data I/O
spark = (
    SparkSession.builder.master("local[4]")
    # JDBC to postgres requires driver from https://mvnrepository.com/artifact/org.postgresql/postgresql/42.3.3
    .config("spark.jars.packages", "org.postgresql:postgresql:42.3.3")
    .config("spark.driver.cores", 8)
    .config("spark.driver.memory", "8g")
    .config("spark.executor.cores", 4)
    .config("spark.executor.memory", "4g")
    .appName("superTable")
    .getOrCreate()
)
print("Spark Config:")
pprint(spark.sparkContext.getConf().getAll())


# Helper functions
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
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c
    return distance


def print_spark_df(df, show=False):
    print(type(df))
    df.printSchema()
    if show:
        df.show()


def get_df_from_csv(
    file_name,
    data_dir,
    drop_cols=None,
    as_spark=True,
    schema=None,
    num_partitions=None,
):
    # get streaming object from S3
    csv_obj = client.get_object(Bucket=bucket_name, Key=data_dir + file_name)
    body = csv_obj["Body"]
    csv_string = body.read().decode("utf-8")
    # read into pandas dataframe
    df = pd.read_csv(StringIO(csv_string))
    df = df.drop("Unnamed: 0", axis=1)
    if drop_cols:
        df = df.drop(drop_cols, axis=1)
    # convert to spark dataframe
    if as_spark:
        df = (
            spark.createDataFrame(df, schema=schema)
            if schema is not None
            else spark.createDataFrame(df, samplingRatio=0.1)
        )
        if num_partitions:
            df.repartition(numPartitions=num_partitions)
    return df


def spark_df_melt(df, id_vars, value_vars, var_name="variable", value_name="value"):
    _vars_and_vals = F.array(
        *(
            F.struct(F.lit(c).alias(var_name), F.col(c).alias(value_name))
            for c in value_vars
        )
    )

    tmp_df = df.withColumn("_vars_and_vals", F.explode(_vars_and_vals))
    out_cols = id_vars + [
        F.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]
    ]
    return tmp_df.select(out_cols)


def get_stations(file_name, data_dir, **kwargs):
    # kwargs to be used in get_df_from_csv
    df = get_df_from_csv(file_name, data_dir, **kwargs)
    df = df[["station_id", "station_loc"]].drop_duplicates()
    return df


def get_camera_loc():
    URL = "https://api.data.gov.sg/v1/transport/traffic-images"
    response = get_req_handler(URL)
    resp_content = json.loads(response.content.decode("utf-8"))
    cameras = resp_content["items"][0]["cameras"]

    df_rows = []

    for x in cameras:
        cam_id = x["camera_id"]
        location = x["location"]
        lat = location["latitude"]
        lon = location["longitude"]
        df_rows.append([cam_id, lat, lon])

    return pd.DataFrame(df_rows, columns=["cam_id", "cam_lat", "cam_lon"])


def camera_to_loc_mapping():
    rainfall = get_stations("rainfall-realtime.csv", weather_dir, as_spark=False)
    non_rainfall = get_stations(
        "non-rainfall-realtime.csv", weather_dir, as_spark=False
    )

    keep = ["S24", "S107"]  # need to manual inspect and change
    non_rainfall = non_rainfall[non_rainfall.station_id.isin(keep)]

    non_rainfall["not_rain_lat"] = non_rainfall.station_loc.apply(
        lambda x: float(x.split(", ")[0][1:])
    )
    non_rainfall["not_rain_lon"] = non_rainfall.station_loc.apply(
        lambda x: float(x.split(", ")[1][:-1])
    )

    rainfall["rain_lat"] = rainfall.station_loc.apply(
        lambda x: float(x.split(", ")[0][1:])
    )
    rainfall["rain_lon"] = rainfall.station_loc.apply(
        lambda x: float(x.split(", ")[1][:-1])
    )

    non_rainfall = non_rainfall.rename(columns={"station_id": "not_rainfall"}).drop(
        "station_loc", axis=1
    )
    rainfall = rainfall.rename(columns={"station_id": "rainfall"}).drop(
        "station_loc", axis=1
    )

    camera_df = get_camera_loc()
    while len(camera_df) != 87:
        camera_df = get_camera_loc()

    df = camera_df.merge(non_rainfall, how="cross")

    df["not_rain_dist"] = df.apply(
        lambda x: get_distance(x.cam_lat, x.not_rain_lat, x.cam_lon, x.not_rain_lon),
        axis=1,
    )
    idx = df.groupby(["cam_id"])["not_rain_dist"].transform(min) == df["not_rain_dist"]
    df = df[idx]

    df2 = df.merge(rainfall, how="cross")
    df2["rain_dist"] = df2.apply(
        lambda x: get_distance(x.cam_lat, x.rain_lat, x.cam_lon, x.rain_lon), axis=1
    )
    idx2 = df2.groupby(["cam_id"])["rain_dist"].transform(min) == df2["rain_dist"]
    df2 = df2[idx2]

    area = get_df_from_csv("area_lat_lon.csv", "", as_spark=False)

    df["not_rain_dist"] = df.apply(
        lambda x: get_distance(x.cam_lat, x.not_rain_lat, x.cam_lon, x.not_rain_lon),
        axis=1,
    )
    idx = df.groupby(["cam_id"])["not_rain_dist"].transform(min) == df["not_rain_dist"]
    df = df[idx]

    df2 = df.merge(rainfall, how="cross")
    df2["rain_dist"] = df2.apply(
        lambda x: get_distance(x.cam_lat, x.rain_lat, x.cam_lon, x.rain_lon), axis=1
    )
    idx2 = df2.groupby(["cam_id"])["rain_dist"].transform(min) == df2["rain_dist"]
    df2 = df2[idx2]

    area["area_lat"] = area.coord.apply(lambda x: float(x.split(", ")[0]))
    area["area_lon"] = area.coord.apply(lambda x: float(x.split(", ")[1]))
    area = area.drop("coord", axis=1)

    df3 = df2.merge(area, how="cross")
    df3["area_dist"] = df3.apply(
        lambda x: get_distance(x.cam_lat, x.area_lat, x.cam_lon, x.area_lon), axis=1
    )
    idx3 = df3.groupby(["cam_id"])["area_dist"].transform(min) == df3["area_dist"]
    df3 = df3[idx3]

    compass = ["north", "south", "east", "west", "central"]
    compass_lat = [
        1.4171600757873817,
        1.2689570746680328,
        1.3474858448902267,
        1.3529774835842006,
        1.3569871019585316,
    ]
    compass_lon = [
        103.80906027423956,
        103.82763148734753,
        103.94089620103182,
        103.70537681098106,
        103.82099141380705,
    ]
    compass_df = pd.DataFrame([compass, compass_lat, compass_lon]).T.rename(
        columns={0: "compass", 1: "compass_lat", 2: "compass_lon"}
    )

    df4 = df3.merge(compass_df, how="cross")
    df4["compass_dist"] = df4.apply(
        lambda x: get_distance(x.cam_lat, x.compass_lat, x.cam_lon, x.compass_lon),
        axis=1,
    )
    idx4 = df4.groupby(["cam_id"])["compass_dist"].transform(min) == df4["compass_dist"]
    df4 = df4[idx4]

    df_final = (
        df4[["cam_id", "not_rainfall", "rainfall", "region", "compass"]]
        .sort_values("cam_id")
        .reset_index()
        .drop("index", axis=1)
    )

    cam_to_loc = df_final.rename(
        columns={
            "rainfall": "rainfall_station_id",
            "not_rainfall": "non_rainfall_station_id",
            "region": "2hr_forecast_area",
        }
    )

    cam_to_loc.cam_id = cam_to_loc.cam_id.apply(lambda x: int(x))
    return cam_to_loc


# functions to process each data table
def proc_rainfall(call_timestamp=None):
    file_path = "rainfall-realtime"
    if call_timestamp:
        file_path += "_" + formatted_timestamp(call_timestamp)
    file_path += ".csv"

    schema = schema_rainfall()
    # Process Rainfall Realtime
    rainfall_realtime = get_df_from_csv(file_path, weather_dir)
    rainfall_realtime = rainfall_realtime.select(
        ["call_timestamp", "station_id", "rainfall_realtime"]
    ).withColumnRenamed("station_id", "rainfall_station_id")
    return rainfall_realtime


def proc_non_rainfall(call_timestamp=None):
    file_path = "non-rainfall-realtime"
    if call_timestamp:
        file_path += "_" + formatted_timestamp(call_timestamp)
    file_path += ".csv"

    schema = schema_non_rainfall()

    # Process Non Rainfall Realtime
    non_rainfall_realtime = get_df_from_csv(file_path, weather_dir)

    non_rainfall_realtime = non_rainfall_realtime.withColumnRenamed(
        "RH%_realtime", "humidity_realtime"
    ).withColumnRenamed("station_id", "non_rainfall_station_id")

    non_rainfall_realtime = non_rainfall_realtime.select(
        [
            "call_timestamp",
            "non_rainfall_station_id",
            "wind_speed_realtime",
            "wind_dir_realtime",
            "humidity_realtime",
            "air_temp_realtime",
        ]
    )
    return non_rainfall_realtime


def proc_forecast_4DAY(call_timestamp=None):
    file_path = "forecast-4DAY"
    if call_timestamp:
        file_path += "_" + formatted_timestamp(call_timestamp)
    file_path += ".csv"
    # Process 4 day Forecast

    schema = schema_forecast_4DAY()
    forecast_4d = get_df_from_csv(file_path, weather_dir)
    forecast_4d = forecast_4d.withColumn("4day_date", F.to_date("4day_date"))

    # produce row numbers for each date forecasted for each timestamp
    forecast_4d = forecast_4d.sort("4day_date").withColumn(
        "grp_row_num",
        F.row_number().over(
            Window.partitionBy(["4day_update_timestamp", "call_timestamp"]).orderBy(
                "4day_date"
            )
        ),
    )

    # pivot for each call_timestamp (as row) producing columns for each date (1 to 4) forecasted
    forecast_4d = forecast_4d.drop("4day_date")

    pivot_agg = {
        c: "first"
        for c in forecast_4d.columns
        if c not in ("call_timestamp", "grp_row_num")
    }

    forecast_4d = (
        forecast_4d.groupBy("call_timestamp").pivot("grp_row_num").agg(pivot_agg)
    )

    rename_list = [forecast_4d.columns[0]] + [
        re.findall(r"\((\w+)\)", c)[0] + "_" + c[0] for c in forecast_4d.columns[1:]
    ]

    forecast_4d = forecast_4d.toDF(*rename_list)
    drop_cols = [
        "4day_update_timestamp_1",
        "4day_update_timestamp_2",
        "4day_update_timestamp_3",
        "4day_update_timestamp_4",
    ]
    forecast_4d = forecast_4d.drop(*drop_cols)

    # rename columns to start with alphabets
    forecast_4d = forecast_4d.toDF(
        *["four_" + c[1:] if c.startswith("4") else c for c in forecast_4d.columns]
    )
    return forecast_4d


def proc_forecast_24HR(call_timestamp=None):
    file_path = "forecast-24HR"
    if call_timestamp:
        file_path += "_" + formatted_timestamp(call_timestamp)
    file_path += ".csv"
    pre_drop_cols = [
        "24hr_period_4_start",
        "24hr_period_4_end",
        "24hr_period_4_forecast_west",
        "24hr_period_4_forecast_east",
        "24hr_period_4_forecast_central",
        "24hr_period_4_forecast_south",
        "24hr_period_4_forecast_north",
    ]

    schema = schema_forecast_24HR()

    forecast_24h = get_df_from_csv(file_path, weather_dir, drop_cols=pre_drop_cols)
    # Process 24h
    long_df_dict = {}
    overlap_cols = []
    for p in range(1, 3 + 1):
        long_df = spark_df_melt(
            forecast_24h,
            id_vars=[
                "call_timestamp",
                f"24hr_period_{p}_start",
                f"24hr_period_{p}_end",
            ],
            value_vars=[
                f"24hr_period_{p}_forecast_west",
                f"24hr_period_{p}_forecast_east",
                f"24hr_period_{p}_forecast_central",
                f"24hr_period_{p}_forecast_south",
                f"24hr_period_{p}_forecast_north",
            ],
        )
        long_df = long_df.withColumn(
            "split_arr", F.split(long_df.variable, "_")
        ).select("*", F.element_at(F.col("split_arr"), -1).alias("compass"))
        long_df = long_df.drop("split_arr", "variable").withColumnRenamed(
            "value", f"24hr_period_{p}"
        )
        long_df_dict[p] = long_df

        # to remove from the main df
        overlap_cols.extend(
            [
                f"24hr_period_{p}_forecast_west",
                f"24hr_period_{p}_forecast_east",
                f"24hr_period_{p}_forecast_central",
                f"24hr_period_{p}_forecast_south",
                f"24hr_period_{p}_forecast_north",
                f"24hr_period_{p}_start",
                f"24hr_period_{p}_end",
            ]
        )

    forecast_24h = forecast_24h.drop(*overlap_cols)

    forecast_24h = (
        forecast_24h.join(long_df_dict[1], how="inner", on="call_timestamp")
        .join(long_df_dict[2], how="inner", on=["call_timestamp", "compass"])
        .join(long_df_dict[3], how="inner", on=["call_timestamp", "compass"])
    )
    # rename columns to start with alphabets
    forecast_24h = forecast_24h.toDF(
        *[
            "twenty_four_" + c[2:] if c.startswith("24") else c
            for c in forecast_24h.columns
        ]
    )
    return forecast_24h


def proc_forecast_2HR(call_timestamp=None):
    file_path = "forecast-2HR"
    if call_timestamp:
        file_path += "_" + formatted_timestamp(call_timestamp)
    file_path += ".csv"

    schema = schema_forecast_2HR()
    forecast_2h = get_df_from_csv("forecast-2HR.csv", weather_dir)
    # Process 2h
    forecast_2h = forecast_2h.drop("2hr_forecast_area_loc", "2hr_start", "2hr_end")
    # rename columns to start with alphabets
    forecast_2h = forecast_2h.toDF(
        *["two_" + c[1:] if c.startswith("2") else c for c in forecast_2h.columns]
    )
    return forecast_2h


def proc_bing(call_timestamp=None):
    file_path = "all-congestion-levels"
    if call_timestamp:
        file_path += "_" + formatted_timestamp(call_timestamp)
    file_path += ".csv"

    bing_key_schema, bing_data_schema = schema_bing()

    bing_data = get_df_from_csv(file_path, bing_dir, schema=bing_data_schema)

    bing_key_table = get_df_from_csv("route_data.csv", bing_dir, schema=bing_key_schema)
    # Process Bing
    bing_key_table = bing_key_table.select("camera_id", "direction").withColumnRenamed(
        "camera_id", "cam_id"
    )
    bing_data = (
        bing_data.select(
            "camera_id", "direction", "call_timestamp", "trafficCongestion"
        )
        .withColumnRenamed("camera_id", "cam_id")
        .withColumnRenamed("trafficCongestion", "trafficcongestion")
    )

    return bing_key_table, bing_data


def get_super_table(
    call_timestamp=None,
    push_to_DB="spark",
    dest_table="traffic_weather_comb",
    write_mode="append",
):
    # if write mode specified as append (default), then only append the rows from today to the DB
    # otherwise, overwrite the DB with all rows in the source CSVs (can use overwrite mode to benchmark spark performance)
    assert (
        write_mode == "append" or write_mode == "overwrite"
    ), "write_mode should be one of 'append', 'overwrite'"

    # Camera to locations -
    # TODO: perhaps run mapping function one time & pull from S3 instead
    # alternatively save that file in the server EC2 machine
    # cam_to_loc = camera_to_loc_mapping()
    cam_to_loc = get_df_from_csv("camera_station_mapping.csv", "")
    cam_to_loc = (
        cam_to_loc.withColumnRenamed("rainfall", "rainfall_station_id")
        .withColumnRenamed("not_rainfall", "non_rainfall_station_id")
        .withColumnRenamed("region", "two_hr_forecast_area")
    )

    print("Camera-Station Mapping DF:")
    print_spark_df(cam_to_loc)

    # Read each file & process tables
    print("Rainfall Realtime DF:")
    rainfall_realtime = proc_rainfall(call_timestamp=call_timestamp)
    print_spark_df(rainfall_realtime)

    print("Non-rainfall Realtime DF:")
    non_rainfall_realtime = proc_non_rainfall(call_timestamp=call_timestamp)
    print_spark_df(non_rainfall_realtime)

    print("start Forecast 4DAY DF at", getCurrentDateTime())
    forecast_4d = proc_forecast_4DAY(call_timestamp=call_timestamp)
    print_spark_df(forecast_4d)
    print("end Forecast 4DAY DF at", getCurrentDateTime())

    print("start Forecast 24HR DF:", getCurrentDateTime())
    forecast_24h = proc_forecast_24HR(call_timestamp=call_timestamp)
    print_spark_df(forecast_24h)
    print("end Forecast 24HR DF:", getCurrentDateTime())

    print("Forecast 2HR DF:")
    forecast_2h = proc_forecast_2HR(call_timestamp=call_timestamp)
    print_spark_df(forecast_2h)

    print("Bing DFs:")
    bing_key_table, bing_data = proc_bing(call_timestamp=call_timestamp)
    print("Bing Key Table DF:")
    print_spark_df(bing_key_table)
    print("Bing Data DF:")
    print_spark_df(bing_data)

    # Combine Tables
    cam_all = cam_to_loc.join(bing_key_table, on="cam_id")
    all_time = (
        rainfall_realtime.select("call_timestamp")
        .intersect(non_rainfall_realtime.select("call_timestamp"))
        .intersect(forecast_4d.select("call_timestamp"))
        .intersect(forecast_24h.select("call_timestamp"))
        .intersect(forecast_2h.select("call_timestamp"))
        .intersect(bing_data.select("call_timestamp"))
        .distinct()
    )
    if write_mode == "append":
        print("Writing rows for the date:", datetime.today)
        all_time = all_time.filter(
            F.to_date(F.col("call_timestamp")).eqNullSafe(F.current_date())
        )

    base_df = all_time.join(cam_all, how="cross")

    df = base_df.join(
        rainfall_realtime, on=["call_timestamp", "rainfall_station_id"], how="left"
    )
    df2 = df.join(
        non_rainfall_realtime,
        on=["call_timestamp", "non_rainfall_station_id"],
        how="left",
    )
    df3 = df2.join(
        forecast_2h, on=["call_timestamp", "two_hr_forecast_area"], how="left"
    )
    df4 = df3.join(forecast_4d, on=["call_timestamp"])
    df5 = df4.join(forecast_24h, on=["call_timestamp", "compass"])
    df6 = df5.join(bing_data, on=["call_timestamp", "cam_id", "direction"]).sort(
        "call_timestamp"
    )

    # writing super table to a CSV locally for now
    # TODO: push table to redshift, also do so for the other smaller tables
    print(f"start copy to DB using {push_to_DB}:", getCurrentDateTime())
    if push_to_DB == "pandas":
        pd_df6 = df6.toPandas()
        if isinstance(pd_df6, pd.DataFrame):
            # use pd DF to_csv method to write to single csv sT.csv (csv has no header to allow easy insertion into db)
            file_path = "sT.csv"
            pd_df6.to_csv(file_path, index=False, header=False)
            with open(file_path) as f:
                copy_csv_to_db(f)
            print("end:", getCurrentDateTime())
    elif push_to_DB == "spark":
        spark_df_to_db(df6, table_name=dest_table, write_mode=write_mode)
        print(f"write to Redshift DB Table {dest_table} done at:", getCurrentDateTime())

        # use spark API to write into dir sT/
        df6.repartition(1).write.csv("sT", header=True, mode=write_mode)
        print("write to local csv done at:", getCurrentDateTime())
    return df6


sT = get_super_table(
    push_to_DB="spark", dest_table="traffic_weather_comb_spark", write_mode="append"
)
