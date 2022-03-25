import boto3
import os
import requests
import sys

from botocore.response import StreamingBody
from mypy_boto3_s3.service_resource import Bucket
from typing import Dict

# Commonly used helper functions

# s3_bucket: S3.Bucket
# search_key: prefix key to search
# returns dictionary stucture: { filename: StreamingBody }
# NB: use method read() on StreamingBody to convert to bytes
def get_s3_objs(s3_bucket: Bucket, search_key: str) -> Dict[str, StreamingBody]:
    obj_summaries = s3_bucket.objects.filter(Prefix=search_key)

    res = dict([(summary.key, summary.get()["Body"]) for summary in obj_summaries])
    return res


# url: URL string to perform GET request
# returns request Response
def get_req_handler(url: str) -> requests.Response:
    response = requests.get(url)
    if response.status_code != 200:
        print(
            "Non-OK status " + str(response.status_code) + " from " + url,
            file=sys.stderr,
        )
    return response


# Initialize boto3 session from env vars (ensure load_dotenv fn is called beforehand)
def initBoto3Session() -> None:
    boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
