# Providence Bike Share

The goal of this project is to collect data from different bikeshare/scooter programs in Providence, RI through the gbfs location API. Then the data will be cleaned in Python and processed to track changes in location by `bike_id` to discern trips. The final clean, processed dataset will be analyzed further with Python & visualized using Python, QGIS and Tableau.

**This is NOT offical trip data from any of the providers.**

## Prerequisites

You must have **Python 3** installed.  You can download it
[here](https://www.python.org/downloads/).  
Routing requires **Graphhopper**, **OpenStreetMap**.

To use AWS Lambda you must have an AWS account with [IAM setup](https://aws.amazon.com/iam/).</br>
To download the files from your s3 bucket you will need [AWS CLI](https://aws.amazon.com/cli/).

## Usage

Code is run from the command line with one requirement<br>
providers list: jump | bird | lime | veoride | spin

**--provider** a bike/scooter company.<br>
Optional **-s** or **--sync** to download newer files from s3 bucket.<br>
Optional **-t** or **--test** runs a small sample of 2,000 files through the pipeline.

example: `python main.py --provider jump`

example: `python main.py --provider lime --sync`

## AWS Cloud

I chose to use AWS Lambda to run the Python code collecting the data. The Lambda script ran every 5 minutes by a CloudWatch Events trigger. The Python script would then process the location gbfs feed api into a `.json` file and add it into an AWS S3 Bucket.

## Data Features

<details>
    <summary><b>clean_trips.csv</b></summary>

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- trip_id<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- battery_end<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- battery_start<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- lat_end<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- lat_start<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- lon_end<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- lon_start<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- timestamp_end<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- timestamp_start<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- neghbor_end<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- neghbor_start<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- ward_end<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- ward_start<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- bike_id<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- type<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- duration<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- duration_min<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- distance<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- time<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- speed_mph<br/>
</details>

<details>
    <summary><b>report_bike_details.csv</b></summary>

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- bike_id<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- trips_count<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- charges_count<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- min_date<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- max_date<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- date_diff<br/>
</details>

<details>
    <summary><b>report_daily_trips.csv</b></summary>

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- date<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- count<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- avg_wind<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- precipitation<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- max_temp<br/>
</details>

<details>
    <summary><b>report_ward_trips.csv</b></summary>

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- ward_start<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- ward_end<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- count<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- percent<br/>
</details>

<details>
    <summary><b>report_neighborhood_trips.csv</b></summary>

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- neghbor_start<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- neghbor_end<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- count<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- percent<br/>
</details>

<details>
    <summary><b>clean_bike_routes.geojson</b></summary>

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- trip_id<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- geometry (LINESTRING)<br/>
</details>

<details>
    <summary><b>report_trip_lines_straight.geojson</b></summary>

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- trip_id<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- lon_end<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- lon_start<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- lat_end<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- lat_start<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- battery_end<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- battery_start<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- timestamp_end<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- timestamp_start<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- neghbor_end<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- neghbor_start<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- ward_end<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- bike_id<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- duration<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- duration_min<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- type<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- distance<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- time<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- speed_mph<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- geometry (LINESTRING)<br/>
</details>

## GBFS Location API

- [Spin Scooters](https://web.spin.pm/api/gbfs/v1/providence/gbfs)
- ~~[VeoRide Scooters](https://share.veoride.com/api/share/gbfs/free_bike_status?area_name=providence)~~ (removed)
- ~~[Jump Bikes](https://pvd.jumpbikes.com/opendata/gbfs.json)~~ (removed)
- ~~[Lime Scooters](https://data.lime.bike/api/partners/v1/gbfs/providence/gbfs.json)~~ (removed)
- ~~[Bird Scooters](https://mds.bird.co/gbfs/providence/free_bikes)~~ (removed)

## Creations With Data

- [JUMP Tableau Dashboard](https://public.tableau.com/app/profile/smirkygraphs/viz/JumpBikeTrips-ProvidenceRI/pvd-jump-bikes)
- [Collection of Maps](https://ivizri.com/posts/2019/09/jump-bike-maps/)
    - [all routed trips](https://i.imgur.com/yKpTmQh.png)
    - [all straight lines](https://imgur.com/JQDJYLO.png)
    - [pvd-fest routed trips](https://imgur.com/TtbMAmA.png)

## References

- Weather Data [(NOAA)](https://www.ncdc.noaa.gov/cdo-web/)
- Routing Engine [Graphhopper](https://www.graphhopper.com/)
- Routing Map [OpenStreetMap](https://download.geofabrik.de/)
