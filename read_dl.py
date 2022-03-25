import boto3
import os

from botocore.response import StreamingBody
from datetime import datetime
from dotenv import load_dotenv
from mypy_boto3_s3.service_resource import Bucket
from typing import Dict

# Helper functions to get data from the various data lakes:
# 1. Image object from S3
# 2. Weather data from RDS (TODO)

today_date = datetime.today().date()

load_dotenv()

# initialize AWS boto client
session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)

# initialize S3 ServiceResource and bucket
s3_resource = boto3.resource("s3")
img_bucket = s3_resource.Bucket(os.environ["BUCKET_NAME"])

# s3_bucket: S3.Bucket
# search_key: prefix key to search
# returns dictionary stucture: { filename: StreamingBody }
# NB: use method read() on StreamingBody to convert to bytes
def get_s3_objs(s3_bucket: Bucket, search_key: str) -> Dict[str, StreamingBody]:
    obj_summaries = s3_bucket.objects.filter(Prefix=search_key)

    res = dict([(summary.key, summary.get()["Body"]) for summary in obj_summaries])
    return res


# retrieve and download all of today's images to local file dir
search_key = os.environ["CAM_IMG_DIR"] + str(today_date)
res = get_s3_objs(img_bucket, search_key)
if not os.path.exists(os.environ["CAM_IMG_DIR"]):
    os.makedirs(os.environ["CAM_IMG_DIR"])
for filename, stream_data in res.items():
    with open(filename, "wb") as f:
        f.write(stream_data.read())

print("successfully retrieved all images for " + str(today_date))
