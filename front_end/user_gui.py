from spyre import server
from conn import create_db_conn
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import pickle
#import io
#import PIL.Image as Image
import matplotlib.image as mpimg


### HELPER FUNCTION FROM DON ####
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

### END OF HELPER ###

# Connect to the database
conn = create_db_conn()
cur = conn.cursor()
cur2 = conn.cursor()
cur3 = conn.cursor()

# Set up the average df for prediction 
mean_query = """
SELECT cam_id, direction, substring(SUBSTRING(call_timestamp, 12, 12), 1,2) as time_hr, 
avg(coalesce(rainfall_realtime, 0)) as rainfall_realtime,
avg(coalesce(wind_speed_realtime, 0)) as wind_speed_realtime,
avg(coalesce(wind_dir_realtime, 0)) as wind_dir_realtime, 
avg(coalesce(humidity_realtime, 0)) as humidity_realtime, 
avg(coalesce(air_temp_realtime, 0)) as air_temp_realtime, 
avg(coalesce(four_day_temperature_low_1, 0)) as four_day_temperature_low_1, 
avg(coalesce(four_day_temperature_low_2, 0)) as four_day_temperature_low_2, 
avg(coalesce(four_day_temperature_low_3, 0)) as four_day_temperature_low_3, 
avg(coalesce(four_day_temperature_low_4, 0)) as four_day_temperature_low_4, 
avg(coalesce(four_day_temperature_high_1, 0)) as four_day_temperature_high_1, 
avg(coalesce(four_day_temperature_high_2, 0)) as four_day_temperature_high_2, 
avg(coalesce(four_day_temperature_high_3, 0)) as four_day_temperature_high_3, 
avg(coalesce(four_day_temperature_high_4, 0)) as four_day_temperature_high_4, 
avg(coalesce(four_day_relative_humidity_low_1, 0)) as four_day_relative_humidity_low_1, 
avg(coalesce(four_day_relative_humidity_low_2, 0)) as four_day_relative_humidity_low_2, 
avg(coalesce(four_day_relative_humidity_low_3, 0)) as four_day_relative_humidity_low_3, 
avg(coalesce(four_day_relative_humidity_low_4, 0)) as four_day_relative_humidity_low_4, 
avg(coalesce(four_day_relative_humidity_high_1, 0)) as four_day_relative_humidity_high_1, 
avg(coalesce(four_day_relative_humidity_high_2, 0)) as four_day_relative_humidity_high_2, 
avg(coalesce(four_day_relative_humidity_high_3, 0)) as four_day_relative_humidity_high_3, 
avg(coalesce(four_day_relative_humidity_high_4, 0)) as four_day_relative_humidity_high_4, 
avg(coalesce(four_day_wind_speed_low_1, 0)) as four_day_wind_speed_low_1, 
avg(coalesce(four_day_wind_speed_low_2, 0)) as four_day_wind_speed_low_2, 
avg(coalesce(four_day_wind_speed_low_3, 0)) as four_day_wind_speed_low_3, 
avg(coalesce(four_day_wind_speed_low_4, 0)) as four_day_wind_speed_low_4, 
avg(coalesce(four_day_wind_speed_high_1, 0)) as four_day_wind_speed_high_1, 
avg(coalesce(four_day_wind_speed_high_2, 0)) as four_day_wind_speed_high_2, 
avg(coalesce(four_day_wind_speed_high_3, 0)) as four_day_wind_speed_high_3, 
avg(coalesce(four_day_wind_speed_high_4, 0)) as four_day_wind_speed_high_4, 
avg(coalesce(twenty_four_hr_general_relative_humidity_low, 0)) as twenty_four_hr_general_relative_humidity_low, 
avg(coalesce(twenty_four_hr_general_relative_humidity_high , 0)) as twenty_four_hr_general_relative_humidity_high, 
avg(coalesce(twenty_four_hr_general_temperature_low, 0)) as twenty_four_hr_general_temperature_low, 
avg(coalesce(twenty_four_hr_general_temperature_high, 0)) as twenty_four_hr_general_temperature_high, 
avg(coalesce(twenty_four_hr_general_wind_speed_low, 0)) as twenty_four_hr_general_wind_speed_low, 
avg(coalesce(twenty_four_hr_general_wind_speed_high, 0)) as twenty_four_hr_general_wind_speed_high
FROM traffic_weather_comb
GROUP BY cam_id, direction, substring(SUBSTRING(call_timestamp, 12, 12), 1,2)
"""

names_rows = ["cam_id", "direction", "time_hr", 
              "rainfall_realtime", "wind_speed_realtime", "wind_dir_realtime", 'humidity_realtime', 'air_temp_realtime',
              '4day_temperature_low_1', '4day_temperature_low_2', '4day_temperature_low_3', '4day_temperature_low_4',
              '4day_temperature_high_1', '4day_temperature_high_2', '4day_temperature_high_3', '4day_temperature_high_4',
              '4day_relative_humidity_low_1', '4day_relative_humidity_low_2','4day_relative_humidity_low_3',
              '4day_relative_humidity_low_4', '4day_relative_humidity_high_1', '4day_relative_humidity_high_2',
              '4day_relative_humidity_high_3', '4day_relative_humidity_high_4','4day_wind_speed_low_1',
              '4day_wind_speed_low_2', '4day_wind_speed_low_3', '4day_wind_speed_low_4','4day_wind_speed_high_1',
              '4day_wind_speed_high_2','4day_wind_speed_high_3', '4day_wind_speed_high_4', 
              '24hr_general_relative_humidity_low','24hr_general_relative_humidity_high', '24hr_general_temperature_low',
              '24hr_general_temperature_high', '24hr_general_wind_speed_low', '24hr_general_wind_speed_high']

columns_neat = ["call_timestamp","cam_id",  "direction", "compass", "2hr_forecast_area", "non_rainfall_station_id", 
                "rainfall_station_id","rainfall_realtime","wind_speed_realtime", "wind_dir_realtime", "humidity_realtime",
                "air_temp_realtime", "2hr_forecast_value", "4day_forecast_1", "4day_wind_speed_low_1", "4day_wind_speed_high_1",
                "4day_wind_direction_1", "4day_relative_humidity_low_1", "4day_relative_humidity_high_1",
                "4day_temperature_low_1", "4day_temperature_high_1", "4day_forecast_2", "4day_wind_speed_low_2",
                "4day_wind_speed_high_2", "4day_wind_direction_2", "4day_relative_humidity_low_2", 
                "4day_relative_humidity_high_2","4day_temperature_low_2", "4day_temperature_high_2", "4day_forecast_3",
                "4day_wind_speed_low_3", "4day_wind_speed_high_3", "4day_wind_direction_3", "4day_relative_humidity_low_3",
                "4day_relative_humidity_high_3","4day_temperature_low_3", "4day_temperature_high_3", "4day_forecast_4",
                "4day_wind_speed_low_4", "4day_wind_speed_high_4", "4day_wind_direction_4", "4day_relative_humidity_low_4", 
                "4day_relative_humidity_high_4","4day_temperature_low_4", "4day_temperature_high_4",
                "24hr_start", "24hr_end", "24hr_general_forecast", "24hr_general_relative_humidity_low",
                "24hr_general_relative_humidity_high", "24hr_general_temperature_low",
                "24hr_general_temperature_high", "24hr_general_wind_speed_low", "24hr_general_wind_speed_high", 
                "24hr_general_wind_direction", "24hr_period_1_start", "24hr_period_1_end", "24hr_period_1",
                "24hr_period_2_start", "24hr_period_2_end", "24hr_period_2",
                "24hr_period_3_start", "24hr_period_3_end", "24hr_period_3", "trafficCongestion"]

all_cols = ["call_timestamp","cam_id",  "direction", "compass", "2hr_forecast_area", "non_rainfall_station_id", 
            "rainfall_station_id","rainfall_realtime","wind_speed_realtime", "wind_dir_realtime", "humidity_realtime", 
            "air_temp_realtime", "2hr_forecast_value", "4day_wind_speed_low_1", "4day_wind_direction_1",
            "4day_relative_humidity_high_1", "4day_temperature_high_1","4day_wind_speed_high_1","4day_temperature_low_1",
            "4day_forecast_1", "4day_relative_humidity_low_1", "4day_wind_speed_low_2", "4day_wind_direction_2", 
            "4day_relative_humidity_high_2", "4day_temperature_high_2","4day_wind_speed_high_2","4day_temperature_low_2", 
            "4day_forecast_2", "4day_relative_humidity_low_2", "4day_wind_speed_low_3", "4day_wind_direction_3",
            "4day_relative_humidity_high_3", "4day_temperature_high_3","4day_wind_speed_high_3","4day_temperature_low_3",
            "4day_forecast_3", "4day_relative_humidity_low_3","4day_wind_speed_low_4", "4day_wind_direction_4",
            "4day_relative_humidity_high_4", "4day_temperature_high_4","4day_wind_speed_high_4","4day_temperature_low_4", 
            "4day_forecast_4", "4day_relative_humidity_low_4", "24hr_start", "24hr_end", "24hr_general_forecast",              
            "24hr_general_relative_humidity_low", "24hr_general_relative_humidity_high", "24hr_general_temperature_low",
            "24hr_general_temperature_high", "24hr_general_wind_speed_low", "24hr_general_wind_speed_high",
            "24hr_general_wind_direction", "24hr_period_1_start", "24hr_period_1_end", "24hr_period_1", 
            "24hr_period_2_start", "24hr_period_2_end", "24hr_period_2", "24hr_period_3_start", "24hr_period_3_end", 
            "24hr_period_3", "trafficCongestion"]

# Make the query and df
cur.execute(mean_query)
data = cur.fetchall()
mean_df = pd.DataFrame(data, columns = names_rows)
mean_df = mean_df.fillna(0)

sort_cols = ["time_hr", 
              "rainfall_realtime", "wind_speed_realtime", "wind_dir_realtime", 'humidity_realtime', 'air_temp_realtime',
              '4day_temperature_low_1', '4day_temperature_low_2', '4day_temperature_low_3', '4day_temperature_low_4',
              '4day_temperature_high_1', '4day_temperature_high_2', '4day_temperature_high_3', '4day_temperature_high_4',
              '4day_relative_humidity_low_1', '4day_relative_humidity_low_2','4day_relative_humidity_low_3',
              '4day_relative_humidity_low_4', '4day_relative_humidity_high_1', '4day_relative_humidity_high_2',
              '4day_relative_humidity_high_3', '4day_relative_humidity_high_4','4day_wind_speed_low_1',
              '4day_wind_speed_low_2', '4day_wind_speed_low_3', '4day_wind_speed_low_4','4day_wind_speed_high_1',
              '4day_wind_speed_high_2','4day_wind_speed_high_3', '4day_wind_speed_high_4', 
              '24hr_general_relative_humidity_low','24hr_general_relative_humidity_high', '24hr_general_temperature_low',
              '24hr_general_temperature_high', '24hr_general_wind_speed_low', '24hr_general_wind_speed_high',
            "cam_id", "direction"]

mean_df = mean_df[sort_cols]

# Get camera list 
get_camera = "SELECT DISTINCT cam_id FROM traffic_weather_comb ORDER BY cam_id"
cur.execute(get_camera)
data = cur.fetchall()
camera_list = [x[0] for x in data]

# Get all possible dates 
get_dates = "SELECT DISTINCT SUBSTRING(call_timestamp, 1, 10) AS dates FROM traffic_weather_comb ORDER BY dates"
cur.execute(get_dates)
data = cur.fetchall()
date_list = [x[0] for x in data]

# for plot
congestion_map = {"None" : 1, "Mild" : 2, "Medium" : 3, "Heavy" : 4, "NaN" : 1}
col = ['r','b','g','m','y']

# For prediction
loaded_model = pickle.load(open("final_random_forest.sav", 'rb'))

# All possible datetime
cur3.execute("SELECT DISTINCT call_timestamp, SUBSTRING(call_timestamp, 1, 10) AS date,\
SUBSTRING(SUBSTRING(call_timestamp, 12, 12), 1,2) as hour,\
SUBSTRING(SUBSTRING(SUBSTRING(call_timestamp, 12, 12), 3,4),1,2) as minute FROM traffic_weather_comb")

time_stamps = cur3.fetchall()
all_time_df = pd.DataFrame(time_stamps, columns = ["timestamp", "date", "hour", "minute"])
all_time_df.minute = all_time_df.minute.apply(lambda x : int(x))

class CongestionDisplayApp(server.App):
    title = 'Congestion Display'

    inputs = [{
        'type': 'searchbox',
        'label': 'View Road Image',
        'options': camera_list,
        'key': 'image_id',
        'action_id': 'pic_change',
        'value': ''
    },{
        'type': 'searchbox',
        'label': 'Image Date',
        'key': 'image_date',
        'options': date_list,
        'action_id': 'pic_change',
        'value': ''
    }, {
        'type': 'text',
        'label': 'Image Time',
        'key': 'image_time',
        'action_id': 'pic_change',
        'value': ''
    }, {
        'type': 'searchbox',
        'label': 'Camera Id',
        'options': camera_list,
        'key': 'camera_id',
        'action_id': 'update_all',
        'value': ''
    }, {
        "type":'searchbox',
        "label": 'Start Date',
        'options': date_list,
        "value" : '',
        "key": 'start_date',
        "action_id" : "update_all",
    },{
        "type":'searchbox',
        "label": 'End Date',
        'options': date_list,
        "value" : '',
        "key": 'end_date',
        "action_id" : "update_all",
    },{
        "type":'slider',
        "label": 'Start Hour',
        "key": 'start_time',
        "value" : 0,
        "min" : 0,
        "max" : 24,
        "action_id" : "update_all",
    }, {
        "type":'slider',
        "label": 'End Hour',
        "key": 'end_time',
        "value" : 0,
        "min" : 1,
        "max" : 24,
        "action_id" : "update_all",
    }, {
        'type': 'searchbox',
        'label': 'Prediction Location',
        'options': camera_list,
        'key': 'location_id',
        'action_id': 'make_predict',
        'value': ''
    },{
        'type': 'text',
        'label': 'Prediction Date',
        'key': 'prediction_date',
        'action_id': 'make_predict',
        'value': ''
    }, {
        'type': 'text',
        'label': 'Prediction Time',
        'key': 'prediction_time',
        'action_id': 'make_predict',
        'value': ''
    }]

    ### can probably play around to figure out how to do a more dynamice changing
    ### maybe split the caregories such that got a few drop downs
    ### also do we want to consider the time element 

    outputs = [{
        'type': 'html',
        'id': 'overall_summary',
        'control_id': 'update_all',
        'tab': 'Welcome'
    }, {
        'type': 'image',
        'id': 'get_image',
        'control_id': 'pic_change',
        'tab': 'Live_Road_Image'
    }, {
        'type': 'table',
        'id': 'analysis_table',
        'control_id': 'update_all',
        'tab': 'Analysis_Table'
    }, {
        'type': 'plot',
        'id': 'analysis_plot',
        'control_id': 'update_all',
        'tab': 'Analysis_Plot'
    },{
        'type': 'table',
        'id': 'rf_prediction',
        'control_id': 'make_predict',
        'tab': 'Prediction_Feature'
    }]

    controls = [{
        'type': 'button',
        'label': 'Update',
        'id': 'update_all'
    },{
        'type': 'button',
        'label': 'Change Image',
        'id': 'pic_change'
    },{
        'type': 'button',
        'label': 'Predict',
        'id': 'make_predict'
    }]

    tabs = ['Welcome', 'Live_Road_Image', 'Analysis_Table', 'Analysis_Plot', 'Prediction_Feature']

    def overall_summary(self, params):
        return """Welcome to the traffic congestion predictor application. To view previous data, please fill in a camera id, start date, end date and time frame. Note that any blanks will be taken as 'to take all values'.
                To view images of the road, select the camera id in the image box and fill in the date and time. Then click on change image to update to the desired image.
                Click the update button to make changes to the filter conditions when necessary. To make a  prediction for future congestion levels, please fill in the prediction date time and predict location boxes followed by clicking the predict button.
                The time is entered in the format of YYYY-MM-DD and HHMMSS. Example, for 1st April 2022 for 1.30pm, it will be 2022-04-01 and time of 133000.
                Thank you and happy analysing! """

    def get_image(self, params):
        image_date = params["image_date"]
        image_time = params["image_time"]
        image_loc = str(params["image_id"])

        if image_loc == "":
            image_loc = "1001"

        if image_date == "" or image_time == "":
            image_date = min(all_time_df.timestamp)[0:10]
            image_time = min(all_time_df.timestamp)[11:15]

        if image_time[0:2] == "00":
            image_time = "0100"
        elif image_time[0:2] == "23":
            image_time = "22"

        narrow = all_time_df[(all_time_df.date == image_date) & (all_time_df.hour == image_time[0:2])].copy()
        
        narrow.loc[:,"dif"] = abs(int(image_time[2:4]) - narrow.minute)
        value = narrow[narrow.dif == min(narrow.dif)].iloc[0,0]

        file_name = value + "_" + image_loc + ".jpg"

        img_binary = get_img(file_name)
        img = mpimg.imread(io.BytesIO(img_binary), format='jpg')
        
        return img

        
    def analysis_table(self, params):
        start_time = int(params["start_time"])
        end_time = int(params["end_time"])
        start_date = (params["start_date"])
        end_date = (params["end_date"])

        if start_date != "":
            full_start = f"'{start_date}T{(start_time):02}0000'"
        else:
            full_start = f"'{min(date_list)}T{(start_time):02}0000'"

        if end_date != "":
            full_end = f"'{end_date}T{(end_time):02}0000'"
        else:
            full_end = f"'{max(date_list)}T{(end_time):02}0000'"

        camera_id = str(params["camera_id"])
        
        if full_end < full_start:
            return pd.DataFrame(["End is greater than start. Please change the time period"])
        
        query = f"SELECT * FROM traffic_weather_comb WHERE call_timestamp >= {full_start} AND call_timestamp <= {full_end}"

        if camera_id != "":
            query = query + f"AND cam_id = {camera_id}"

        query = query + " ORDER BY call_timestamp, cam_id, direction"

        # Get the table
        cur.execute(query)
        combined = []
        data = cur.fetchmany(1000)

        #while len(data) != 0:
        #    combined.extend(data)
        #    data = cur.fetchmany(1000)

        #df = pd.DataFrame(combined, columns = all_cols)
        query_df = pd.DataFrame(data, columns = all_cols)
        query_df = query_df[columns_neat]

        return query_df
        

    def analysis_plot(self, params):

        start_time = int(params["start_time"])
        end_time = int(params["end_time"])
        start_date = (params["start_date"])
        end_date = (params["end_date"])

        camera_id = str(params["camera_id"])

        if start_date != "":
            full_start = f"'{start_date}T{(start_time):02}0000'"
        else:
            full_start = f"'{min(date_list)}T{(start_time):02}0000'"

        if end_date != "":
            full_end = f"'{end_date}T{(end_time):02}0000'"
        else:
            full_end = f"'{min(date_list)}T{(end_time):02}0000'"

        if camera_id == "":
            camera_id = 1001


        get_values = "SELECT call_timestamp, direction, SUBSTRING(call_timestamp, 1, 10) as date, SUBSTRING(SUBSTRING(call_timestamp, 12,12),1,4) as time, trafficcongestion FROM traffic_weather_comb WHERE cam_id = "
        get_values = get_values + str(camera_id) + f" AND call_timestamp >= {full_start} AND call_timestamp <= {full_end} ORDER BY date, time"

        cur2.execute(get_values)
        data2 = cur2.fetchall()

        plot_df = pd.DataFrame(data2, columns = ["call", "direction", "date", "time", "congestion"])
        plot_df["numeric"] = plot_df.apply(lambda x : congestion_map[x.congestion], axis = 1)
        
        num_rows = len(plot_df.direction.unique())
        num_calls = len(plot_df.call.unique())
        num_dates = len(plot_df.date.unique())
        fig, ax = plt.subplots(num_rows,figsize = (12,num_rows * 4))
        locator = plt.MultipleLocator(num_calls // 5)
        locator2 = plt.MultipleLocator(1)

        for label, data in plot_df.groupby('direction'):
            ax[label-1].plot(data.call, data.numeric, col[label-1])
            ax[label-1].set(title = f"Congestion against time for direction {label}", ylabel='Congestion', xlabel='Time')
            ax[label-1].set_ylim([0.7,4.3])
            ax[label-1].xaxis.set_major_locator(locator)
            ax[label-1].xaxis.set_minor_locator(locator2)

        fig.tight_layout()

        return fig

    def rf_prediction(self, params):

        time = params["prediction_time"][0:2]
        loc_id = params["location_id"]

        print(time)
        print(loc_id)

        if time == "00":
            time = "01"
        elif time == "23":
            time= "22"
        elif time == "":
            time = "01"

        if loc_id == "":
            loc_id = 1001

        impt_df = mean_df[(mean_df.cam_id == int(loc_id)) & (mean_df.time_hr == time)]
        
        predict = loaded_model.predict(impt_df.drop("time_hr", axis = 1))
        impt_df = impt_df[names_rows]
        impt_df.insert(2, "Congestion Prediction", predict)

        return impt_df.drop("time_hr", axis = 1)

    


app = CongestionDisplayApp()
app.launch(port=1006)

