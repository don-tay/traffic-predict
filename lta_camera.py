import os
import json

import requests
import datetime

URL = "https://api.data.gov.sg/v1/transport/traffic-images"
timestamp = datetime.datetime.now()
params = {
    "datetime": timestamp
}
print("Sending request to " + URL + " ...")
response = requests.get(URL, params=params, timeout=10)
print("Received response!")
print("response URL: " + response.url)
print("response status code: " + str(response.status_code))

resp_content = json.loads(response.content.decode('utf-8'))
print("response content converted to type: " + str(type(resp_content)))

save_dir = "camera_images"  # this directory will be created at the same level as this script
saved_count = 0
limit = 100
for cam in resp_content['items'][0]['cameras']:
    # if saved_count >= limit:
    #     break
    cam_id = cam['camera_id']
    img_link = cam['image']
    timestamp = cam['timestamp']
    timestamp = timestamp.replace(":", "-")
    timestamp = timestamp.split('+', 1)[0]  # remove time-zone (+08:00
    img_request = requests.get(img_link)

    if img_request.ok:
        img_data = img_request.content
        img_dir = os.path.join(os.curdir, save_dir, cam_id)
        if not os.path.exists(img_dir):
            os.makedirs(img_dir)  # create subdirectory for each camera id
        img_file_path = os.path.join(img_dir, timestamp + ".jpg")

        with open(img_file_path, 'xb') as handler:
            handler.write(img_data)
        handler.close()
        print("Saved image at:" + img_file_path)
        saved_count += 1
    else:
        print("image GET failed. response code: " + str(img_request.status_code))

print(f"Saved {saved_count} images from API call!")
