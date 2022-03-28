import schedule
import time
from weather_data import *

RT_SCHEDULE_FREQ = 5  # minutes
schedule.every(RT_SCHEDULE_FREQ).minutes.do(real_time_weather, output_loc="AWS")
print(
    f"Init scheduler to run real time weather data ingest job every {RT_SCHEDULE_FREQ} mins"
)

FR_2HR_SCHEDULE_FREQ = 15  # minutes
schedule.every(FR_2HR_SCHEDULE_FREQ).minutes.do(forecast_weather_2HR, output_loc="AWS")
print(
    f"Init scheduler to run 2hr weather forecast data ingest job every {FR_2HR_SCHEDULE_FREQ} mins"
)

FR_24HR_SCHEDULE_FREQ = 2  # hours
schedule.every(FR_24HR_SCHEDULE_FREQ).hours.do(forecast_weather_24HR, output_loc="AWS")
print(
    f"Init scheduler to run 24hr weather forecast data ingest job every {FR_24HR_SCHEDULE_FREQ} hours"
)

FR_4DAY_SCHEDULE_FREQ = 3  # hours
schedule.every(FR_4DAY_SCHEDULE_FREQ).hours.do(forecast_weather_4DAY, output_loc="AWS")
print(
    f"Init scheduler to run 4day weather forecast data ingest job every {FR_4DAY_SCHEDULE_FREQ} hours"
)

ARCHIVE_SCHEDULE_FREQ = 2  # hours
schedule.every(ARCHIVE_SCHEDULE_FREQ).hours.do(merge_weather_csvs)
print(
    f"Init scheduler to run timestamped file merge & archive job every {ARCHIVE_SCHEDULE_FREQ} hours"
)

while True:
    schedule.run_pending()
    time.sleep(1)
