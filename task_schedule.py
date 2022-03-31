import schedule
import time
from weather_data import *

# RT_SCHEDULE_FREQ = 5  # minutes
# schedule.every(RT_SCHEDULE_FREQ).minutes.do(real_time_weather, output_loc="AWS")
# print(
#     f"Init scheduler to run real time weather data ingest job every {RT_SCHEDULE_FREQ} mins"
# )

# FR_2HR_SCHEDULE_FREQ = 15  # minutes
# schedule.every(FR_2HR_SCHEDULE_FREQ).minutes.do(forecast_weather_2HR, output_loc="AWS")
# print(
#     f"Init scheduler to run 2hr weather forecast data ingest job every {FR_2HR_SCHEDULE_FREQ} mins"
# )

# FR_24HR_SCHEDULE_FREQ = 2  # hours
# schedule.every(FR_24HR_SCHEDULE_FREQ).hours.do(forecast_weather_24HR, output_loc="AWS")
# print(
#     f"Init scheduler to run 24hr weather forecast data ingest job every {FR_24HR_SCHEDULE_FREQ} hours"
# )

# FR_4DAY_SCHEDULE_FREQ = 3  # hours
# schedule.every(FR_4DAY_SCHEDULE_FREQ).hours.do(forecast_weather_4DAY, output_loc="AWS")
# print(
#     f"Init scheduler to run 4day weather forecast data ingest job every {FR_4DAY_SCHEDULE_FREQ} hours"
# )

# TODO: need to sync up camera image ingestion with weather data ingestion?
def ingest_all_weather(output_loc="AWS"):
    call_timestamp = datetime.datetime.now()
    print(
        "Calling all weather data endpoints with date_time parameter:", call_timestamp
    )
    real_time_weather(output_loc=output_loc, call_timestamp=call_timestamp)
    forecast_weather_2HR(output_loc=output_loc, call_timestamp=call_timestamp)
    forecast_weather_24HR(output_loc=output_loc, call_timestamp=call_timestamp)
    forecast_weather_4DAY(output_loc=output_loc, call_timestamp=call_timestamp)


# TODO: determine suitable ingestion frequency (based on real-time data update frequency?) and merging frequency

WEATHER_SCHEDULE_FREQ = 5  # minutes
schedule.every(WEATHER_SCHEDULE_FREQ).minutes.do(ingest_all_weather)
print(f"Init scheduler to run weather ingest job every {WEATHER_SCHEDULE_FREQ} minutes")

# perform merges between ingestion jobs
MERGE_SCHEDULE_FREQ = 12  # minutes
schedule.every(MERGE_SCHEDULE_FREQ).minutes.do(merge_weather_csvs)
print(
    f"Init scheduler to run timestamped file merge & archive job every {MERGE_SCHEDULE_FREQ} minutes"
)

while True:
    schedule.run_pending()
    time.sleep(1)
