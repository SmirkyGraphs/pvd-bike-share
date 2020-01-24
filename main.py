import os
import argparse
from pathlib import Path
from src import cleaner
from src.routing import routing

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--provider', dest='provider', required=True, 
                        help='choose providers: jump | bird | lime | veoride | spin')

    parser.add_argument('-s', '--sync', dest='sync', action='store_true', default=False,
                        help='pass this to sync the s3 bucket')

    args = parser.parse_args()

    return args

def sync_buckets(bucket):
    print(f'[status] syncing {bucket}')
    cmd = f'aws s3 sync s3://{bucket} ./data/raw/{bucket}'
    os.system(cmd) 

def start_server(server_name):
    cmd = []
    cmd.append(server_name)
    cmd.append("start cmd /k java -Xmx6g -Xms6g -Dgraphhopper.datareader.file=ri.osm.pbf -jar graphhopper-server.jar server config.yml")
    cmd = " & ".join(cmd)
    os.system(cmd) 

def clean_trips(company, veh_type, drop_cols=None):
    all_files = list(Path(f'./data/raw/{company}/{veh_type}/').rglob('*.json'))
    df = cleaner.create_trips(all_files)

    return df

# map provider to folder
map_folder = {
    'jump': 'pvd-jump-bikes',
    'bird': 'pvd-bird-scooters',
    'lime': 'pvd-lime-scooters',
    'spin': 'pvd-spin-scooters',
    'veoride': 'pvd-veoride-scooters'
}

# graphhopper location
graphhopper = r'cd D:/Files/GIS/GraphHopper/graphhopper-0.12.0/'

# number of multiprocess workers
workers = 20

def main():
    # parse args
    args = get_args()
    sync = args.sync
    provider = args.provider
    provider = map_folder[provider]
    vehicle = provider.split('-')[-1:][0]

    # download all new files from s3
    if sync == True:
        sync_buckets(provider)

    # clean trips & save
    df = clean_trips(provider, vehicle)
    df.to_csv(f'./data/interim/{provider}/trips_clean.csv', index=False)

    # start server
    start_server(graphhopper)

    # route trips & save geojson
    gdf = routing(df, 'gpx', 'bike', workers)
    gdf.to_file(f"./data/clean/{provider}/full_bike_routes.geojson", driver='GeoJSON')

    # route trip details & save
    df = routing(df, 'json', 'bike', workers)
    df.to_csv(f'./data/interim/{provider}/trip_route_details.csv', index=False)

if __name__ == '__main__':
    main()