# Helper functions to get data from the various data lakes:
# 1. Image from S3
# 2. Weather data from RDS (TODO)


# s3_bucket: S3 Bucket Resource type
# start_date: date string in YYYY-mm-dd format
def get_s3_images(s3_bucket, start_date):
  objs = s3_bucket.list_objects_v2(
    StartAfter=start_date
  )
  print(objs)
