import boto3
import os

from datetime import datetime
from dotenv import load_dotenv
from helper import get_s3_objs, initBoto3Session

# Helper functions to get data from the various data lakes:
# 1. Image object from S3
# 2. Weather data from RDS (TODO)

today_date = datetime.today().date()

############ CONFIG INIT BOILERPLATE ############
# load env vars
load_dotenv()

initBoto3Session()

# initialize S3 ServiceResource and bucket (init boto3 session first)
s3_resource = boto3.resource("s3")
img_bucket = s3_resource.Bucket(os.environ["BUCKET_NAME"])

############ END OF CONFIG INIT BOILERPLATE ############

CAM_IMG_DIR = os.environ["CAM_IMG_DIR"]

# retrieve and download all of today's images to local file dir
search_key = CAM_IMG_DIR + str(today_date)
res = get_s3_objs(img_bucket, search_key)
if not os.path.exists(CAM_IMG_DIR):
    os.makedirs(CAM_IMG_DIR)
for filename, stream_data in res.items():
    with open(filename, "wb") as f:
        f.write(stream_data.read())

print("successfully retrieved all images for " + str(today_date))
