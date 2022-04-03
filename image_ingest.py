import shutil
import sys
import boto3
import os

from dotenv import load_dotenv
from helper import (
    formatted_timestamp,
    get_req_handler,
    getCurrentDateTime,
    initBoto3Session,
)
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


def ingest_image(call_timestamp=getCurrentDateTime()):
    # GET request to data API
    response = get_req_handler(IMAGE_API_URL)

    res_json = response.json()
    if res_json["api_info"]["status"] != "healthy":
        print("Image API unhealthy", file=sys.stderr)

    item = res_json["items"][0]
    timestamp, cameras = itemgetter("timestamp", "cameras")(item)
    iso_datetime = formatted_timestamp(call_timestamp)

    if not os.path.exists(CAM_IMG_DIR):
        os.makedirs(CAM_IMG_DIR)

    for camera in cameras:
        image, location, camera_id, image_metadata = itemgetter(
            "image", "location", "camera_id", "image_metadata"
        )(camera)
        # sample filename format: cam-img/2022-03-21T235548_1002.jpg
        file_name = CAM_IMG_DIR + iso_datetime + "_" + camera_id + ".jpg"
        # get image stream
        img_resp = get_req_handler(image)

        with open(file_name, "wb") as f:
            f.write(img_resp.content)

    try:
        shutil.make_archive(iso_datetime, "zip", CAM_IMG_DIR)
        zip_filename = iso_datetime + ".zip"
        # upload image to S3
        with open(zip_filename, "rb") as f:
            img_bucket.upload_fileobj(Fileobj=f, Key=CAM_IMG_DIR + zip_filename)
        print("Completed image upload to S3 at " + str(call_timestamp))
        os.remove(zip_filename)
    except Exception:
        print("Failed to upload images " + CAM_IMG_DIR, file=sys.stderr)
    finally:
        shutil.rmtree(CAM_IMG_DIR)
