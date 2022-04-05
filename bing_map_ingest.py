import ast
import os

import pandas as pd
import json
from io import StringIO
from pprint import pprint
import re

import boto3
from dotenv import load_dotenv
from helper import (
    get_s3_objs,
    getCurrentDateTime,
    initBoto3Session,
    get_req_handler,
    formatted_timestamp,
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
BING_DATA_DIR = os.environ["BING_DATA_DIR"]
BING_API_KEY = os.environ["BING_API_KEY"]
BING_ROUTES_URL = "https://dev.virtualearth.net/REST/V1/Routes"
BING_TRAFFIC_URL = "https://dev.virtualearth.net/REST/V1/Traffic/Incidents"


def call_route_API(
    camera_id,
    direction: int,  # from 1 to n
    start_loc: tuple,
    finish_loc: tuple,
    call_timestamp=getCurrentDateTime(),
    output_loc="local",
):
    call_timestamp_str = formatted_timestamp(call_timestamp)

    route_call_params = {
        "waypoint.1": ",".join((str(L) for L in start_loc)),
        "waypoint.2": ",".join((str(L) for L in finish_loc)),
        "optimize": "distance",
        "key": BING_API_KEY,
    }

    resp = get_req_handler(BING_ROUTES_URL, route_call_params)
    resp_content = json.loads(resp.content.decode("utf-8"))

    # for short route before and after the camera, there should be 1 resource
    resource = resp_content["resourceSets"][0]["resources"][0]

    extract_vals = {
        "camera_id": [camera_id],
        "direction": [direction],
    }
    extract_fields = [
        "trafficCongestion",
        "travelDistance",
        "travelDuration",
        "travelDurationTraffic",
        "distanceUnit",
        "durationUnit",
    ]

    for f in extract_fields:
        # attempt to extract the field from the resource
        try:
            v = itemgetter(f)(resource)
        except KeyError:
            v = []
        extract_vals[f] = [v]

    extract_frame = pd.DataFrame(extract_vals)
    extract_frame["call_timestamp"] = call_timestamp_str

    extract_filename = (
        "congestion_level_"
        + str(extract_vals["camera_id"])
        + "_"
        + call_timestamp_str
        + ".csv"
    )
    if output_loc == "return":
        return extract_frame
    else:
        output_csv(
            extract_frame,
            BING_DATA_DIR,
            extract_filename,
            output_loc=output_loc,
            s3_bucket=data_bucket,
        )


def produce_route_table(output_loc="local"):
    # run this while keeping camera_dir_locs.csv in the same directory
    # run this function once to store the route data in S3
    dir_loc_df = pd.read_csv("camera_dir_locs.csv")

    common_cols = [c for c in dir_loc_df.columns if not c.startswith("dir")]
    num_dirs = len([c for c in dir_loc_df.columns if c.startswith("dir")]) // 2

    dir_frames = {}
    for i in range(1, num_dirs + 1):
        dir_cols = [
            c for c in dir_loc_df.columns if c.startswith("dir") and c.endswith(str(i))
        ]

        dir_frames[i] = dir_loc_df[common_cols + dir_cols]
        dir_frames[i].insert(1, "direction", i)
        rename_cols = {
            c: (c[:-2] if c in dir_cols else c) for c in dir_frames[i].columns
        }
        dir_frames[i] = dir_frames[i].rename(columns=rename_cols)

    route_data = pd.concat(dir_frames.values(), ignore_index=False)
    route_data = route_data.sort_values(["camera_id", "direction"])
    route_data.dropna(axis=0, subset=["dir_start", "dir_finish"], inplace=True)
    route_data.reset_index(drop=True, inplace=True)

    loc_string_to_tuple = lambda s: tuple(float(c) for c in re.findall("[^,\s]+", s))
    route_data["cam_loc"] = route_data["cam_loc"].map(loc_string_to_tuple)
    route_data["dir_start"] = route_data["dir_start"].map(loc_string_to_tuple)
    route_data["dir_finish"] = route_data["dir_finish"].map(loc_string_to_tuple)

    if output_loc == "return":
        return route_data
    else:
        route_data_filename = "route_data.csv"
        output_csv(
            route_data,
            BING_DATA_DIR,
            route_data_filename,
            output_loc=output_loc,
            s3_bucket=data_bucket,
        )


def process_route_congestion(output_loc="local", call_timestamp=getCurrentDateTime()):
    # get route_data.csv from S3
    s3_obj = get_s3_objs(data_bucket, BING_DATA_DIR + "route_data.csv")
    csv_body = list(s3_obj.values())[0]
    csv_string = csv_body.read().decode("utf-8")
    route_data = pd.read_csv(StringIO(csv_string))
    route_data = route_data.drop("Unnamed: 0", axis=1)

    congestion_frames = []
    print(f"Calling Bing API for {route_data.shape[0]} routes at", getCurrentDateTime())
    for row in route_data.itertuples(index=False):
        # collect dataframe for each route
        congestion_frames.append(
            call_route_API(
                int(row.camera_id),
                int(row.direction),
                ast.literal_eval(row.dir_start),  # convert stringified tuple to tuple
                ast.literal_eval(row.dir_finish),  # convert stringified tuple to tuple
                call_timestamp=call_timestamp,
                output_loc="return",
            )
        )
    congestion_data = pd.concat(congestion_frames, ignore_index=True)
    congestion_data = congestion_data.sort_values(["camera_id", "direction"])
    print(f"Completed congestion data collection at", getCurrentDateTime())
    combined_congestion_data = pd.merge(
        congestion_data, route_data, on=["camera_id", "direction"], how="outer"
    )
    if output_loc == "return":
        return combined_congestion_data
    else:
        congestion_data_filename = (
            "all-congestion-levels_" + formatted_timestamp(call_timestamp) + ".csv"
        )
        output_csv(
            combined_congestion_data,
            BING_DATA_DIR,
            congestion_data_filename,
            output_loc=output_loc,
            s3_bucket=data_bucket,
        )


# function to call the traffic incident API, returns response for incidents within a bounding box
def call_traffic_API(start_loc: tuple, finish_loc: tuple, incident_type: list = [2]):
    bbox = [
        min(start_loc[0], finish_loc[0]),  # South Latitude
        min(start_loc[1], finish_loc[1]),  # West Longitude
        max(start_loc[0], finish_loc[0]),  # North Latitude
        max(start_loc[1], finish_loc[1]),  # East Longitude
    ]

    print(f"{bbox=}")
    traffic_call_params = {
        "type": ",".join([str(i) for i in incident_type]),
        "key": BING_API_KEY,
    }

    resp = get_req_handler(
        BING_TRAFFIC_URL + "/" + ",".join((str(L) for L in bbox)), traffic_call_params
    )
    resp_content = json.loads(resp.content.decode("utf-8"))
    pprint(resp_content)


def merge_bing_csvs():
    table_names = [
        "all-congestion-levels",
    ]
    for table in table_names:
        print(f"Merging {table}...")
        merge_bucket_csvs(
            data_bucket,
            BING_DATA_DIR,
            table,
            key_cols=None,
        )


### SAMPLE TESTS
# produce_route_table(output_loc="AWS") # call once if route_data.csv not in S3
# process_route_congestion(output_loc="AWS")
# merge_bing_csvs()
