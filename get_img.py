import io
import zipfile
import boto3
import os

from botocore.response import StreamingBody
from dotenv import load_dotenv
from helper import get_s3_objs, initBoto3Session

# Helper functions to get image data by filename

############ CONFIG INIT BOILERPLATE ############
# load env vars
load_dotenv()

initBoto3Session()

# initialize S3 ServiceResource and bucket (init boto3 session first)
s3_resource = boto3.resource("s3")
img_bucket = s3_resource.Bucket(os.environ["BUCKET_NAME"])

############ END OF CONFIG INIT BOILERPLATE ############

CAM_IMG_DIR = os.environ["CAM_IMG_DIR"]

# get img bytes from img filename on S3
# filename format: YYYY-MM-DDTHHmmss_cameraid.jpg
def get_img(filename: str) -> bytes:
    zip_filename = filename.split("_")[0]
    # retrieve zip file
    search_key = CAM_IMG_DIR + str(zip_filename) + ".zip"
    res = get_s3_objs(img_bucket, search_key)
    stream_data = res.get(search_key)
    if not isinstance(stream_data, StreamingBody):
        raise FileNotFoundError(f"Zip file {search_key} not found on S3")
    zip_file = zipfile.ZipFile(io.BytesIO(stream_data.read()))
    if filename not in zip_file.namelist():
        raise FileNotFoundError(
            f"Image file {filename} not found in {zip_file.filename}"
        )
    return zip_file.open(filename).read()
