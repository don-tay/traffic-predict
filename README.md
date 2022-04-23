# Traffic Prediction

In this project, we aim to address the problem of road traffic congestion, with a focus on the expressways in Singapore. We utilise data including road camera images (from Data.gov.sg), other traffic data (from Bing), and weather data (from Data.gov.sg). The collected data are ingested into S3, processed via Spark into AWS Redshift. Finally, a predictive analytics model is trained on the dataset and visualisations on traffic predictions are generated.

## Setup

### Dependencies Required

- Python 3.8 or later
- [Python3 venv](https://docs.python.org/3/library/venv.html) 
- GNU Make (pre-installed in Linux/MacOS) - [Windows Installation](https://stackoverflow.com/questions/32127524/how-to-install-and-use-make-in-windows)

### First-time Setup

1. Duplicate `.env.example` file and rename it `.env`
2. In the `.env` file, fill in the values for the keys listed

## Scripts

### Script Description
| File                                                                                                                     | Description                                                                                                                                                                                     |
| ------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`bing_map_ingest.py`](https://github.com/Project-Kampong/deployment-scripts/blob/master/bing_map_ingest.py)           | Ingest traffic and route data from Bing Maps API into S3                                                                                                              |
| [`csv_schemas.py`](https://github.com/Project-Kampong/deployment-scripts/blob/master/csv_schemas.py)                           | Schema definition for DataFrames used in Spark transformation                                                                                                                                   |
| [`get_img.py`](https://github.com/Project-Kampong/deployment-scripts/blob/master/get_img.py)               | Helper function for frontend dashboard to pull traffic camera images                                             |
| [`helper.py`](https://github.com/Project-Kampong/deployment-scripts/blob/master/helper.py)       | Utility functions used by other scripts                          |
| [`image_ingest.py`](https://github.com/Project-Kampong/deployment-scripts/blob/master/image_ingest.py) | Ingest traffic image data from Data.gov.sg API into S3 |
| [`super_table_pyspark.py`](https://github.com/Project-Kampong/deployment-scripts/blob/master/super_table_pyspark.py) | Spark job to transform data in S3 into format for model training and insertion into data warehouse. Uncomment the appropriate `get_super_table()` fn call and run py script.  |
| [`task_schedule.py`](https://github.com/Project-Kampong/deployment-scripts/blob/master/task_schedule.py) | Task scheduler to run the listed ingestion jobs on production server at regular interval. Runnable locally.  |
| [`weather_data.py`](https://github.com/Project-Kampong/deployment-scripts/blob/master/weather_data.py) | Ingest realtime, 2h, 24h and 4d weather data from Data.gov.sg into S3 |

### Run Script

Run scripts with Make (For Spark scripts, this will run the Spark process in client mode)

```bash
### Linux/MacOS

### run task_schedule.py
make run

### run custom python script (eg. image_ingest.py)
make run APP=image_ingest.py


### Windows

### run task_schedule.py
make run VENV=.venv/Scripts PY=python

### run custom python script (eg. image_ingest.py)
make run APP=image_ingest.py VENV=.venv/Scripts PY=python
```

Alternatively, run Spark scripts with `spark-submit`:
```bash
spark-submit --conf "spark.jars.packages=org.postgresql:postgresql:42.3.3" super_table_pyspark.py
```
