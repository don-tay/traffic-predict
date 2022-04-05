import schedule
import time
from weather_data import *
from image_ingest import ingest_image
from bing_map_ingest import merge_bing_csvs, process_route_congestion

# synced ingestion of camera image and weather data
# TODO: refactor output_loc to env vars
def ingest_all(output_loc="AWS"):
    call_timestamp = getCurrentDateTime()
    print("Calling all data endpoints with date_time parameter:", call_timestamp)

    ingest_image(call_timestamp=call_timestamp)
    real_time_weather(output_loc=output_loc, call_timestamp=call_timestamp)
    forecast_weather_2HR(output_loc=output_loc, call_timestamp=call_timestamp)
    forecast_weather_24HR(output_loc=output_loc, call_timestamp=call_timestamp)
    forecast_weather_4DAY(output_loc=output_loc, call_timestamp=call_timestamp)
    print("Completed weather data collection at", getCurrentDateTime())
    # call the bing API congestion function from 1 am to 11 pm only.
    cong_start = datetime.time(hour=1, minute=0, second=0, microsecond=0)
    cong_end = datetime.time(hour=23, minute=0, second=0, microsecond=0)
    if cong_start <= call_timestamp.time() <= cong_end:
        process_route_congestion(output_loc=output_loc, call_timestamp=call_timestamp)


def merge_files():
    merge_weather_csvs()
    merge_bing_csvs()


# TODO: determine suitable ingestion frequency (based on real-time data update frequency?) and merging frequency
INGEST_SCHEDULE_FREQ = 5  # minutes
schedule.every(INGEST_SCHEDULE_FREQ).minutes.do(ingest_all)
print(f"Init scheduler to run ingest job every {INGEST_SCHEDULE_FREQ} minutes")

# perform merges between ingestion jobs
MERGE_SCHEDULE_FREQ = 22  # minutes
schedule.every(MERGE_SCHEDULE_FREQ).minutes.do(merge_files)
print(
    f"Init scheduler to run timestamped file merge & archive job every {MERGE_SCHEDULE_FREQ} minutes"
)

while True:
    schedule.run_pending()
    time.sleep(1)
