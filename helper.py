import boto3
import os
import requests
import sys

from botocore.response import StreamingBody
from mypy_boto3_s3.service_resource import Bucket
from typing import Dict
import pandas as pd
import dateutil
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
def get_req_handler(url: str, params: dict) -> requests.Response:
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(
            "Non-OK status " + str(response.status_code) + " from " + url,
            file=sys.stderr,
        )
    return response


# returns string form of datetime timestamp (from API calls)
def formatted_timestamp(timestamp):
    return dateutil.parser.parse(timestamp).strftime("%Y-%m-%dT%H%M%S")


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


# merge CSV files stored in Bucket (produced for each timestamp, and previously merged files) into a single one
# s3_bucket: S3.Bucket object where CSV files are to be merged
# bucket_dir: folder in bucket containing the CSV files
# file_prefix: prefix of filename for the csv files to merge
# key_cols: key columns in the CSV files used to identify and remove duplicate rows after merging
# archive_timestamp_files: whether to archive the individual timestamped files in a separate sub-folder


def merge_bucket_csvs(
    s3_bucket, bucket_dir, file_prefix, key_cols, archive_timestamp_files=True
):
    search_key = bucket_dir + file_prefix
    res = get_s3_objs(s3_bucket, search_key)
    merged_df = pd.DataFrame()
    for filename, stream_data in res.items():
        print("Found file:", filename, end=", ")
        tmp_df = pd.read_csv(BytesIO(stream_data.read()), index_col=0)
        if merged_df.empty:
            merged_df = tmp_df.copy()
        else:
            merged_df = pd.concat([merged_df, tmp_df], ignore_index=True)
        print("merged file contents")
    # merged dataframe may have duplicates rows (which have same values on the key_cols), these are dropped here
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
    # delete/archive the timestamped csvs programmatically after the merge
    TS_search_key = search_key + "_"
    TS_res = get_s3_objs(s3_bucket, TS_search_key)

    print("Managing timestamped files ...")
    for filename, stream_data in TS_res.items():
        print("Timestamped file:", filename, end=", ")
        if archive_timestamp_files:

            s3_bucket.copy(
                CopySource={"Bucket": s3_bucket.name, "Key": filename},
                Key=bucket_dir + "timestamped_files/" + filename.split("/")[1],
            )
            print("archived into subfolder", end=", ")

        s3_bucket.delete_objects(Delete={"Objects": [{"Key": filename}], "Quiet": True})
        print("deleted original file")
