# Traffic Prediction

In this project, we aim to address the problem of road traffic congestion, with a focus on the expressways in Singapore. We would utilise data including road camera images, other traffic data, and weather data. The collected data would be processed through a big data pipeline so that it can be used to generate visualisations and run predictive analytics models.

## Setup

### Dependencies Required

- Python 3.8 or later
- [Python3 venv](https://docs.python.org/3/library/venv.html) 
- GNU Make (pre-installed in Linux/MacOS) - [Windows Installation](https://stackoverflow.com/questions/32127524/how-to-install-and-use-make-in-windows)

### First-time Setup

1. Duplicate `.env.example` file and rename it `.env`
2. In the `.env` file, fill in the values for the keys listed

## Run Script

Run script with Make

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
