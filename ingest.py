import sys
import time
import boto3
import os
import schedule

from dateutil import parser
from dotenv import load_dotenv
from helper import get_req_handler, initBoto3Session
from operator import itemgetter

############ CONFIG INIT BOILERPLATE ############
# load env vars
load_dotenv()

initBoto3Session()

# initialize S3 ServiceResource and bucket (init boto3 session first)
s3_resource = boto3.resource("s3")
img_bucket = s3_resource.Bucket(os.environ["BUCKET_NAME"])

############ END OF CONFIG INIT BOILERPLATE ############

CAM_IMG_DIR = os.environ["CAM_IMG_DIR"]
IMAGE_API_URL = os.environ["IMAGE_API_URL"]


def ingest_job():
    # GET request to data API
    response = get_req_handler(IMAGE_API_URL)

    res_json = response.json()
    if res_json["api_info"]["status"] != "healthy":
        print("Image API unhealthy", file=sys.stderr)

    item = res_json["items"][0]
    timestamp, cameras = itemgetter("timestamp", "cameras")(item)
    iso_datetime = parser.parse(timestamp).strftime("%Y-%m-%dT%H%M%S")

    for camera in cameras:
        image, location, camera_id, image_metadata = itemgetter(
            "image", "location", "camera_id", "image_metadata"
        )(camera)
        # sample filename format: cam-img/2022-03-21T235548_1002.jpg
        file_name = CAM_IMG_DIR + iso_datetime + "_" + camera_id + ".jpg"
        # get image stream
        img_resp = get_req_handler(image)
        try:
            # upload image to S3
            img_bucket.put_object(Body=img_resp.content, Key=file_name)
        except Exception:
            print("Failed to upload image " + file_name, file=sys.stderr)
    print("Completed image upload to S3 at " + str(timestamp))


schedule.every(5).minutes.do(ingest_job)
print("Init scheduler to run ingest job every 5 min")

while True:
    schedule.run_pending()
    time.sleep(1)
