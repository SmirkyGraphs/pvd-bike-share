# Providence Bike Share

The goal of this project is to collect data from different bikeshare/scooter programs in Providence, RI through the gbfs location API. Then the data will be cleaned in Python and processed to track changes in location by `bike_id` to discern trips. The final clean, processed dataset will be analyzed further with Python & visualized using Python, QGIS and Tableau.

**This is NOT offical trip data from any of the providers.**

## Prerequisites

You must have Python 3 installed.  You can download it
[here](https://www.python.org/downloads/).  

To use AWS Lambda you must have an AWS account with [IAM setup](https://aws.amazon.com/iam/).</br>
To download the files from your s3 bucket you will need [AWS CLI](https://aws.amazon.com/cli/).

Routing requires Graphhopper, OpenStreetMap layer.

## Usage

Code is run from the command line with one requirement **--provider** a bike/scooter company.<br>
Optional **--sync** to download newer files from s3 bucket.

providers list: jump | bird | lime | veoride | spin

example: `python main.py --provider jump`

example: `python main.py --provider lime --sync`

## AWS Cloud

I chose to use AWS Lambda to run the Python code collecting the data. The Lambda script ran every 5 minutes by a CloudWatch Events trigger. The Python script would then process the location gbfs feed api into a `.json` file and add it into an AWS S3 Bucket.

## GBFS Location API

- [Spin Scooters](https://web.spin.pm/api/gbfs/v1/providence/gbfs)
- [VeoRide Scooters](https://share.veoride.com/api/share/gbfs/free_bike_status?area_name=providence)
- ~~[Jump Bikes](https://pvd.jumpbikes.com/opendata/gbfs.json)~~ (removed)
- ~~[Lime Scooters](https://data.lime.bike/api/partners/v1/gbfs/providence/gbfs.json)~~ (removed)
- ~~[Bird Scooters](https://mds.bird.co/gbfs/providence/free_bikes)~~ (removed)

## References

- Weather Data [(NOAA)](https://www.ncdc.noaa.gov/cdo-web/)
- Routing Engine [Graphhopper](https://www.graphhopper.com/)
- Routing Map [OpenStreetMap](https://download.geofabrik.de/)
