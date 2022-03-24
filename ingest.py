import sys
import boto3
import os
import requests

from dateutil import parser
from dotenv import load_dotenv
from operator import itemgetter

def get_req_handler(url):
    response = requests.get(url)
    if response.status_code != 200:
        print("Non-OK status " + response.status_code + " from " + url, file=sys.stderr)
    return response


load_dotenv()

# initialize AWS boto client
session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

# initialize S3 ServiceResource and bucket
s3_resource = boto3.resource('s3')
img_bucket = s3_resource.Bucket(os.getenv("BUCKET_NAME"))

# GET request to data API
response = get_req_handler(os.getenv("IMAGE_API_URL"))

res_json = response.json()
if res_json['api_info']['status'] != 'healthy':
    print("Image API unhealthy", file=sys.stderr)

item = res_json['items'][0]
timestamp, cameras = itemgetter('timestamp', 'cameras')(item)
iso_datetime = parser.parse(timestamp).strftime('%Y-%m-%dT%H%M%S')

for camera in cameras:
    image, location, camera_id, image_metadata = itemgetter('image', 'location', 'camera_id', 'image_metadata')(camera)
    # sample filename format: cam-img/2022-03-21T235548_1002.jpg
    file_name = os.getenv("CAM_IMG_DIR") + iso_datetime + '_' + camera_id + '.jpg'
    print(file_name)
    # get image stream
    img_resp = get_req_handler(image)
    try:
        # upload image to S3
        img_bucket.put_object(Body=img_resp.content, Key=file_name)
    except Exception:
        print('Failed to upload image ' + file_name)
