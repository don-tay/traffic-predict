from conn import create_db_conn


conn = create_db_conn()
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE traffic_weather_comb (
    id SERIAL PRIMARY KEY,
    call_timestamp varchar,
    cam_id int,
    direction int,
    compass varchar,
    two_hr_forecast_area varchar,
    non_rainfall_station_id varchar,
    rainfall_station_id varchar,
    rainfall_realtime float,
    wind_speed_realtime float,
    wind_dir_realtime int,
    humidity_realtime float,
    air_temp_realtime float,
    two_hr_forecast_value varchar,
    four_day_wind_speed_low_1 int,
    four_day_wind_direction_1 varchar,
    four_day_relative_humidity_high_1 int,
    four_day_temperature_high_1 int,
    four_day_wind_speed_high_1 int,
    four_day_temperature_low_1 int,
    four_day_forecast_1 varchar,
    four_day_relative_humidity_low_1 int,
    four_day_wind_speed_low_2 int,
    four_day_wind_direction_2 varchar,
    four_day_relative_humidity_high_2 int,
    four_day_temperature_high_2 int,
    four_day_wind_speed_high_2 int,
    four_day_temperature_low_2 int,
    four_day_forecast_2 varchar,
    four_day_relative_humidity_low_2 int,
    four_day_wind_speed_low_3 int,
    four_day_wind_direction_3 varchar,
    four_day_relative_humidity_high_3 int,
    four_day_temperature_high_3 int,
    four_day_wind_speed_high_3 int,
    four_day_temperature_low_3 int,
    four_day_forecast_3 varchar,
    four_day_relative_humidity_low_3 int,
    four_day_wind_speed_low_4 int,
    four_day_wind_direction_4 varchar,
    four_day_relative_humidity_high_4 int,
    four_day_temperature_high_4 int,
    four_day_wind_speed_high_4 int,
    four_day_temperature_low_4 int,
    four_day_forecast_4 varchar,
    four_day_relative_humidity_low_4 int,
    twenty_four_hr_start varchar,
    twenty_four_hr_end varchar,
    twenty_four_hr_general_forecast varchar,
    twenty_four_hr_general_relative_humidity_low int,
    twenty_four_hr_general_relative_humidity_high int,
    twenty_four_hr_general_temperature_low int,
    twenty_four_hr_general_temperature_high int,
    twenty_four_hr_general_wind_speed_low int,
    twenty_four_hr_general_wind_speed_high int,
    twenty_four_hr_general_wind_direction varchar,
    twenty_four_hr_period_1_start varchar,
    twenty_four_hr_period_1_end varchar,
    twenty_four_hr_period_1 varchar,
    twenty_four_hr_period_2_start varchar,
    twenty_four_hr_period_2_end varchar,
    twenty_four_hr_period_2 varchar,
    twenty_four_hr_period_3_start varchar,
    twenty_four_hr_period_3_end varchar,
    twenty_four_hr_period_3 varchar,
    trafficCongestion varchar,
    created_on timestamp default NOW()
    )
  """
)

cursor.close()
conn.close()
