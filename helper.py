import boto3
import os
import pytz
import requests
import sys

from botocore.response import StreamingBody
from mypy_boto3_s3.service_resource import Bucket
from typing import Dict, Union
import pandas as pd
import datetime
from dateutil import parser
from io import StringIO, BytesIO

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
def get_req_handler(url: str, params: dict = {}) -> requests.Response:
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(
            "Non-OK status " + str(response.status_code) + " from " + url,
            file=sys.stderr,
        )
    return response


# returns string of datetime timestamp
# eg. 2022-03-21T235548
def formatted_timestamp(timestamp: Union[str, datetime.datetime]) -> str:
    format = "%Y-%m-%dT%H%M%S"
    if isinstance(timestamp, str):
        # timestamp provided from API calls is a string that needs to be converted
        return parser.parse(timestamp).strftime(format)
    elif isinstance(timestamp, datetime.datetime):
        # if timestamp is a datetime object (produced by code), directly use strftime function on it
        return timestamp.strftime(format)


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
    s3_bucket: Union[Bucket, None] = None,
):
    savepath = directory + filename
    if output_loc == "local":
        if not os.path.exists(directory):
            os.makedirs(directory)
        dataframe.to_csv(savepath, encoding="utf-8")
    elif output_loc == "AWS":
        if not s3_bucket:
            print(
                "When using output='AWS', specify S3.Bucket object in s3_bucket.",
                file=sys.stderr,
            )
            return
        try:
            # upload dataframe to S3 bucket
            csv_buffer = StringIO()
            dataframe.to_csv(csv_buffer, encoding="utf-8")
            s3_bucket.put_object(Body=csv_buffer.getvalue(), Key=savepath)  # type: ignore
        except Exception:
            print("Failed to upload data in " + savepath, file=sys.stderr)


# merge CSV files stored in Bucket (produced for each timestamp, and previously merged files) into a single one
# s3_bucket: S3.Bucket object where CSV files are to be merged
# bucket_dir: folder in bucket containing the CSV files
# file_prefix: prefix of filename for the csv files to merge
# key_cols: key columns in the CSV files used to identify and remove duplicate rows after merging
# archive_timestamp_files: whether to archive the individual timestamped files in a separate sub-folder
# verbose: whether or not to print status messages to stdout


def merge_bucket_csvs(
    s3_bucket,
    bucket_dir,
    file_prefix,
    key_cols=None,
    archive_timestamp_files=True,
    verbose=False,
):
    search_key = bucket_dir + file_prefix
    res = get_s3_objs(s3_bucket, search_key)
    merged_df = pd.DataFrame()
    for filename, stream_data in res.items():
        tmp_df = pd.read_csv(BytesIO(stream_data.read()), index_col=0)
        if merged_df.empty:
            merged_df = tmp_df.copy()
        else:
            merged_df = pd.concat([merged_df, tmp_df], ignore_index=True)
        # delete/archive the csv programmatically after the merge
        if archive_timestamp_files:
            s3_bucket.copy(
                CopySource={"Bucket": s3_bucket.name, "Key": filename},
                Key=bucket_dir + "timestamped_files/" + filename.split("/")[1],
            )
        s3_bucket.delete_objects(Delete={"Objects": [{"Key": filename}], "Quiet": True})
        if verbose:
            print("Found file:", filename, "merged file contents")

    # if key_cols is specified, drop duplicate rows based on them
    # merged dataframe may have duplicates rows (which have same values on the key_cols), these are dropped here
    if key_cols:
        merged_df = merged_df.drop_duplicates(
            subset=key_cols, keep="last", ignore_index=True
        )

    output_csv(
        merged_df,
        bucket_dir,
        file_prefix + ".csv",  # merged file will not have an underscore after prefix
        output_loc="AWS",
        s3_bucket=s3_bucket,
    )


# Get localized current datetime
def getCurrentDateTime():
    tz = pytz.timezone("Asia/Singapore")
    return tz.localize(datetime.datetime.now())
