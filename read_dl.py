import boto3
import os

from datetime import datetime
from dotenv import load_dotenv

# Helper functions to get data from the various data lakes:
# 1. Image from S3
# 2. Weather data from RDS (TODO)

today_date = datetime.today().date()


# s3_bucket: S3 Service Resource type
# search_key: prefix key to search
# returns a dictionary stucture: { filename: StreamingBody }
# NB: use method read() on StreamingBody to convert to bytes
def get_s3_images(s3_bucket, search_key):
  obj_summaries = s3_bucket.objects.filter(
      Prefix=search_key
  )
  res = {}
  for summary in obj_summaries:
    obj = summary.get()
    res[summary.key] = obj['Body']
  return res

load_dotenv()

# initialize AWS boto client
session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

# initialize S3 ServiceResource and bucket
s3_resource = boto3.resource('s3')
img_bucket = s3_resource.Bucket(os.getenv("BUCKET_NAME"))

# retrieve and download all of today's images to local file dir
search_key = os.getenv("CAM_IMG_DIR") + str(today_date)
res = get_s3_images(img_bucket, search_key)
if not os.path.exists(os.getenv("CAM_IMG_DIR")):
    os.makedirs(os.getenv("CAM_IMG_DIR"))
for filename, stream_data in res.items():
    with open(filename, 'wb') as f:
        f.write(stream_data.read())

print('successfully retrieved all images for ' + str(today_date))