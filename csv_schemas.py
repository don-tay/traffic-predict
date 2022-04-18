from pyspark.sql.types import *


def schema_rainfall():
    schema = StructType(
        [
            StructField("timestamp", StringType(), True),
            StructField("station_id", StringType(), True),
            StructField("station_loc", StringType(), True),
            StructField("station_name", StringType(), True),
            StructField("rainfall_realtime", DoubleType(), True),
            StructField("call_timestamp", StringType(), True),
        ]
    )
    return schema


def schema_non_rainfall():
    schema = StructType(
        [
            StructField("timestamp", StringType(), True),
            StructField("station_id", StringType(), True),
            StructField("station_loc", StringType(), True),
            StructField("station_name", StringType(), True),
            StructField("wind_speed_realtime", DoubleType(), True),
            StructField("wind_dir_realtime", DoubleType(), True),
            StructField("call_timestamp", StringType(), True),
            StructField("RH%_realtime", DoubleType(), True),
            StructField("air_temp_realtime", DoubleType(), True),
        ]
    )
    return schema


def schema_forecast_4DAY():
    schema = StructType(
        [
            StructField("4day_date", StringType(), True),
            StructField("4day_forecast", StringType(), True),
            StructField("4day_temperature_low", DoubleType(), True),
            StructField("4day_temperature_high", DoubleType(), True),
            StructField("4day_relative_humidity_low", DoubleType(), True),
            StructField("4day_relative_humidity_high", DoubleType(), True),
            StructField("4day_wind_speed_low", DoubleType(), True),
            StructField("4day_wind_speed_high", DoubleType(), True),
            StructField("4day_wind_direction", StringType(), True),
            StructField("4day_update_timestamp", StringType(), True),
            StructField("call_timestamp", StringType(), True),
        ]
    )
    return schema


def schema_forecast_24HR():
    gen_structs = [
        StructField("24hr_start", StringType(), True),
        StructField("24hr_end", StringType(), True),
        StructField("24hr_general_forecast", StringType(), True),
        StructField("24hr_general_relative_humidity_low", DoubleType(), True),
        StructField("24hr_general_relative_humidity_high", DoubleType(), True),
        StructField("24hr_general_temperature_low", DoubleType(), True),
        StructField("24hr_general_temperature_high", DoubleType(), True),
        StructField("24hr_general_wind_speed_low", DoubleType(), True),
        StructField("24hr_general_wind_speed_high", DoubleType(), True),
        StructField("24hr_general_wind_direction", StringType(), True),
    ]
    all_structs = gen_structs
    for p in range(1, 3 + 1):
        period_structs = [
            StructField(f"24hr_period_{p}_start", StringType(), True),
            StructField(f"24hr_period_{p}_end", StringType(), True),
            StructField(f"24hr_period_{p}_forecast_west", StringType(), True),
            StructField(f"24hr_period_{p}_forecast_east", StringType(), True),
            StructField(f"24hr_period_{p}_forecast_central", StringType(), True),
            StructField(f"24hr_period_{p}_forecast_south", StringType(), True),
            StructField(f"24hr_period_{p}_forecast_north", StringType(), True),
        ]
        all_structs = all_structs + period_structs.copy()
    all_structs = all_structs + [StructField("call_timestamp", StringType(), True)]

    schema = StructType(all_structs)
    return schema


def schema_forecast_2HR():
    schema = StructType(
        [
            StructField("2hr_forecast_area", StringType(), True),
            StructField("2hr_forecast_area_loc", StringType(), True),
            StructField("2hr_forecast_value", StringType(), True),
            StructField("2hr_start", StringType(), True),
            StructField("2hr_end", StringType(), True),
            StructField("call_timestamp", StringType(), True),
        ]
    )
    return schema


def schema_bing():
    bing_key_schema = StructType(
        [
            StructField("camera_id", LongType(), True),
            StructField("direction", LongType(), True),
            StructField("cam_loc", StringType(), True),
            StructField("remarks", StringType(), True),
            StructField("dir_start", StringType(), True),
            StructField("dir_finish", StringType(), True),
        ]
    )

    bing_data_schema = StructType(
        [
            StructField("camera_id", LongType(), True),
            StructField("direction", LongType(), True),
            StructField("trafficCongestion", StringType(), True),
            StructField("travelDistance", DoubleType(), True),
            StructField("travelDuration", LongType(), True),
            StructField("travelDurationTraffic", LongType(), True),
            StructField("distanceUnit", StringType(), True),
            StructField("durationUnit", StringType(), True),
            StructField("call_timestamp", StringType(), True),
            StructField("cam_loc", StringType(), True),
            StructField("remarks", StringType(), True),
            StructField("dir_start", StringType(), True),
            StructField("dir_finish", StringType(), True),
        ]
    )

    return bing_key_schema, bing_data_schema
