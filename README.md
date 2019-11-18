# Providence Bike Share

The goal of this project is to collect data from different bikeshare/scooter programs in Providence, RI through the location API. Then the data will be cleaned in Python and processed to track changes in location by `bike_id` to discern trips. The final clean, processed dataset will be analyzed further with Python & visualized using Python, QGIS and Tableau.

This is **NOT** offical trip data from any of the providers.

## Prerequisites

You must have Python 3 installed.  You can download it
[here](https://www.python.org/downloads/).  
To use AWS Lambda you must have an AWS account with IAM setup.

## AWS Cloud

I chose to use AWS Lambda to run the Python code collecting the data. The Lambda script ran every 5 minutes by a CloudWatch Events trigger. The Python script would then process the location gbfs feed api into a `.json` file and add it into an AWS S3 Bucket.

## GBFS Location API

- ~~[[Jump Bikes](https://pvd.jumpbikes.com/opendata/gbfs.json)~~ (removed)
- [Spin Scooters](https://web.spin.pm/api/gbfs/v1/providence/gbfs)
- [VeoRide Scooters]() (unknown)
- ~~[Lime Scooters](https://data.lime.bike/api/partners/v1/gbfs/providence/gbfs.json)~~ (removed)
- ~~[Bird Scooters](https://mds.bird.co/gbfs/providence/free_bikes)~~ (removed)
