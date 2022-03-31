import schedule
import time
from weather_data import *
from image_ingest import ingest_image

# synced ingestion of camera image and weather data
# TODO: refactor output_loc to env vars
def ingest_all(output_loc="AWS"):
    call_timestamp = getCurrentDateTime()
    print(
        "Calling all weather data endpoints with date_time parameter:", call_timestamp
    )

    ingest_image(call_timestamp=call_timestamp)
    real_time_weather(output_loc=output_loc, call_timestamp=call_timestamp)
    forecast_weather_2HR(output_loc=output_loc, call_timestamp=call_timestamp)
    forecast_weather_24HR(output_loc=output_loc, call_timestamp=call_timestamp)
    forecast_weather_4DAY(output_loc=output_loc, call_timestamp=call_timestamp)


# TODO: determine suitable ingestion frequency (based on real-time data update frequency?) and merging frequency

INGEST_SCHEDULE_FREQ = 5  # minutes
schedule.every(INGEST_SCHEDULE_FREQ).minutes.do(ingest_all)
print(f"Init scheduler to run ingest job every {INGEST_SCHEDULE_FREQ} minutes")

# perform merges between ingestion jobs
MERGE_SCHEDULE_FREQ = 12  # minutes
schedule.every(MERGE_SCHEDULE_FREQ).minutes.do(merge_weather_csvs)
print(
    f"Init scheduler to run timestamped file merge & archive job every {MERGE_SCHEDULE_FREQ} minutes"
)

while True:
    schedule.run_pending()
    time.sleep(1)
