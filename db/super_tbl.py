import os
import io
from typing import Union

import pyspark.sql

from db.conn import create_db_conn

TBL_NAME = "traffic_weather_comb"


def spark_df_to_db(
    spark_df: pyspark.sql.DataFrame,
    table_name: str = TBL_NAME,
    write_mode: str = "append",
) -> None:
    db_host = "jdbc:postgresql://" + os.environ["DB_HOST"] + "/" + os.environ["DB_NAME"]
    db_conn_cred = {
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASS"],
        "driver": "org.postgresql.Driver",  # needed to allow spark to write to postgres db
    }
    spark_df.write.jdbc(db_host, table_name, mode=write_mode, properties=db_conn_cred)


# copy csv (w/o header) of super table into db
def copy_csv_to_db(
    file: Union[io.StringIO, io.TextIOWrapper], table_name: str = TBL_NAME
) -> None:
    conn = create_db_conn()
    cur = conn.cursor()
    insert_cols = (
        "call_timestamp",
        "cam_id",
        "direction",
        "compass",
        "two_hr_forecast_area",
        "non_rainfall_station_id",
        "rainfall_station_id",
        "rainfall_realtime",
        "wind_speed_realtime",
        "wind_dir_realtime",
        "humidity_realtime",
        "air_temp_realtime",
        "two_hr_forecast_value",
        "four_day_wind_speed_low_1",
        "four_day_wind_direction_1",
        "four_day_relative_humidity_high_1",
        "four_day_temperature_high_1",
        "four_day_wind_speed_high_1",
        "four_day_temperature_low_1",
        "four_day_forecast_1",
        "four_day_relative_humidity_low_1",
        "four_day_wind_speed_low_2",
        "four_day_wind_direction_2",
        "four_day_relative_humidity_high_2",
        "four_day_temperature_high_2",
        "four_day_wind_speed_high_2",
        "four_day_temperature_low_2",
        "four_day_forecast_2",
        "four_day_relative_humidity_low_2",
        "four_day_wind_speed_low_3",
        "four_day_wind_direction_3",
        "four_day_relative_humidity_high_3",
        "four_day_temperature_high_3",
        "four_day_wind_speed_high_3",
        "four_day_temperature_low_3",
        "four_day_forecast_3",
        "four_day_relative_humidity_low_3",
        "four_day_wind_speed_low_4",
        "four_day_wind_direction_4",
        "four_day_relative_humidity_high_4",
        "four_day_temperature_high_4",
        "four_day_wind_speed_high_4",
        "four_day_temperature_low_4",
        "four_day_forecast_4",
        "four_day_relative_humidity_low_4",
        "twenty_four_hr_start",
        "twenty_four_hr_end",
        "twenty_four_hr_general_forecast",
        "twenty_four_hr_general_relative_humidity_low",
        "twenty_four_hr_general_relative_humidity_high",
        "twenty_four_hr_general_temperature_low",
        "twenty_four_hr_general_temperature_high",
        "twenty_four_hr_general_wind_speed_low",
        "twenty_four_hr_general_wind_speed_high",
        "twenty_four_hr_general_wind_direction",
        "twenty_four_hr_period_1_start",
        "twenty_four_hr_period_1_end",
        "twenty_four_hr_period_1",
        "twenty_four_hr_period_2_start",
        "twenty_four_hr_period_2_end",
        "twenty_four_hr_period_2",
        "twenty_four_hr_period_3_start",
        "twenty_four_hr_period_3_end",
        "twenty_four_hr_period_3",
        "trafficcongestion",
    )
    cur.copy_from(file, TBL_NAME, sep=",", null="", columns=insert_cols)
    cur.close()
    conn.close()
