import boto3
import os
import requests
import sys

from botocore.response import StreamingBody
from mypy_boto3_s3.service_resource import Bucket
from typing import Dict
import pandas as pd
from io import StringIO

# Commonly used helper functions

# Initialize boto3 session from env vars (ensure load_dotenv fn is called beforehand)
def initBoto3Session() -> None:
    boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


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
def get_req_handler(url: str, params: dict) -> requests.Response:
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(
            "Non-OK status " + str(response.status_code) + " from " + url,
            file=sys.stderr,
        )
    return response


# dataframe: pandas dataframe to save as CSV (locally or in AWS bucket)
# directory: folder to save the CSV file
# filename: name of the CSV file (ending with .csv)
# output_loc: one of 'local', 'AWS'
# s3_bucket: S3.Bucket object where the CSV file is to be pushed
def output_csv(
    dataframe: pd.DataFrame,
    directory: str,
    filename: str,
    output_loc: str = "local",
    s3_bucket: Bucket = None,
):
    savepath = directory + filename
    if output_loc == "local":
        if not os.path.exists(directory):
            os.makedirs(directory)
        dataframe.to_csv(savepath, encoding="utf-8")
    elif output_loc == "AWS":
        if s3_bucket:
            try:
                # upload dataframe to S3 bucket
                csv_buffer = StringIO()
                dataframe.to_csv(csv_buffer, encoding="utf-8")
                s3_bucket.put_object(Body=csv_buffer.getvalue(), Key=savepath)
            except Exception:
                print("Failed to upload data in " + savepath, file=sys.stderr)
            else:
                print("Uploaded dataframe as csv file " + savepath + " to bucket!")
        else:
            print("When using output='AWS', specify S3.Bucket object in s3_bucket.")
