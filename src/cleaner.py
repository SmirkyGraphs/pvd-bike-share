import numpy as np
import pandas as pd

import geopandas as gpd
from shapely.geometry import Point


def combine_json(files):
    print('[status] merging json files')
    frames = []
    for file in files:
        
        # check if file null
        with open(file) as f:
            if 'null' in f.read():
                continue

        # load files & get timestamp
        df = pd.read_json(file, orient='records')

        if 'lastUpdated' in list(df):
            df = df.rename(columns={'lastUpdated':'last_updated'})
        if 'jump_vehicle_type' in list(df):
            df = df.rename(columns={'jump_vehicle_type':'vehicle_type'})

        timestamp = df['last_updated'][0]

        # flatten json data into table & add timestamp
        df = pd.io.json.json_normalize(df.data[0])
        df['timestamp'] = timestamp

        if 'bikeId' in list(df):
            rename = {
                'isDisabled': 'is_disabled',
                'isReserved': 'is_reserved',
                'bikeId': 'bike_id'
            }
            df = df.rename(columns=rename)

        # add file to frames 
        frames.append(df)

    df = pd.concat(frames, sort=True)
    
    return df

def pivot_trips(df):
    print('[status] pivoting trips')
    # sort by bike_id and timestamp
    df = df.sort_values(by=['bike_id', 'timestamp'], ascending=[False, True]).reset_index(drop=True)
    df['timestamp'] = df['timestamp'].apply(lambda x: pd.Timestamp(x, unit='s', tz='US/Eastern'))

    # filter out non-trips & pivot with start & end
    trips = df[df['trip'].notnull()]
    trips = trips.pivot(columns='trip', index='trip_id')

    # rename columns and reset index
    col_one = trips.columns.get_level_values(0).astype(str)
    col_two = trips.columns.get_level_values(1).astype(str)
    trips.columns = col_one + "_" + col_two
    trips = trips.reset_index()
    
    # remove excess bike_id column
    trips['bike_id'] = trips['bike_id_end']
    drop_cols = ['bike_id_start', 'bike_id_end']
    trips = trips.drop(columns=drop_cols)

    return trips

def classify_battery(trips):
    print('[status] adding charged trips')
    # convert battery to float
    trips['battery_start'] = trips['battery_start'].str.rstrip('%').astype('float') / 100.0
    trips['battery_end'] = trips['battery_end'].str.rstrip('%').astype('float') / 100.0

    # mark when battery is charged more then 10%
    trips.loc[(trips['battery_end'] - trips['battery_start']) > 0.1, 'type'] = 'charge'

    return trips

def remove_out_state(gdf):
    print('[status] removing out of state trips')

    # remove trips from outside of ri
    mask = gpd.read_file('./data/files/ri_map.geojson')
    mask = mask['geometry'][0]
    gdf = gdf[gdf['geometry'].within(mask)]
    
    return gdf

def join_neighbor(gdf):
    print('[status] adding neighborhood')
    
    # adds neighborhood column
    mask = gpd.read_file('./data/files/neighborhoods.geojson')
    mask = mask[['lname', 'geometry']]
    mask = mask.rename(columns={'lname': 'neghbor'})

    # join and remove index_right
    gdf = gpd.tools.sjoin(gdf, mask, how="left")
    df.drop('index_right', axis=1, inplace=True)

    return gdf

def join_ward(gdf):
    print('[status] adding ward')
    
    # adds ward column
    mask = gpd.read_file('./data/files/wards.geojson')
    mask = mask[['ward', 'geometry']]
    mask['ward'] = 'ward_' + mask['ward']

    # join and remove index_right
    gdf = gpd.tools.sjoin(gdf, mask, how="left")
    df.drop('index_right', axis=1, inplace=True)
    
    return gdf

def create_trips(files, remove=None):
    # combine json files
    df = combine_json(files)

    # sort by id and convert timestamp
    df = df.sort_values(by=['bike_id', 'timestamp'], ascending=[False, True]).reset_index(drop=True)
    if len(str(int(df.iloc[0]['timestamp']))) > 10:
        units = 'ms'
    else:
        units = 's'
    df['timestamp'] = df['timestamp'].apply(lambda x: pd.Timestamp(x, unit=units, tz='US/Eastern'))
    
    # create trip data by finding movements in bike in lat or lon
    df['end'] = (df['bike_id'] == df['bike_id'].shift())  \
               & (df['lat'].ne(df['lat'].shift())         \
               | (df['lon'].ne(df['lon'].shift())))       \
               & (df['timestamp'].ne(df['timestamp'].shift()))

    df['start'] = (df['bike_id'] == df['bike_id'].shift(-1)) \
                & (df['end'].shift(-1) == True)

    df.loc[df['start'] == True, 'trip'] = 'start'
    df.loc[df['end'] == True, 'trip'] = 'end'
    
    # get timestamps that are both a start & stop to duplicate
    df['dupe'] = (
             (df['bike_id'] == df['bike_id'].shift())
           & (df['bike_id'] == df['bike_id'].shift(-1))
           & (df['trip'] == 'end')
           & (df['trip'].shift(-1) == 'end')
    )

    dupe = df[df['dupe'] == True].copy()
    dupe['trip'] = 'start'
    
    # merge rows that needed to be duplicated
    df = df.append(dupe, sort=False)

    # re sort & reset index
    df = df.sort_values(by=['bike_id', 'timestamp'], ascending=[False, True])
    df = df.reset_index(drop=True)

    # adding a trip_id to join start & stop data
    df['trip_id'] = df.groupby(['trip']).cumcount() + 1

    # remove useless columns & temp columns
    alt_drop = ['vehicle_type', 'name']
    drop_cols = ['is_reserved', 'is_disabled', 'start', 'end', 'dupe']
    for name in alt_drop:
        if name in list(df):
            drop_cols.append(name)
    df = df.drop(columns=drop_cols)

    # rename really long column header
    if 'jump_ebike_battery_level' in list(df):
        df = df.rename(columns={'jump_ebike_battery_level':'battery'})

    # specify type
    df['lat'] = df['lat'].astype(float)
    df['lon'] = df['lon'].astype(float)

    # get point geom for spatial work
    points = [Point(xy) for xy in zip(df['lon'], df['lat'])]
    df = gpd.GeoDataFrame(df, geometry=points)
    df.crs = {"init": "epsg:4326"}

    # perform spatial joins
    df = remove_out_state(df)
    df = join_neighbor(df)
    df = join_ward(df)

    # remove geometry
    df.drop('geometry', axis=1, inplace=True)
    
    # pivot on trip_id (starts -> end) 
    df = pivot_trips(df)

    # add battery info (jump only)
    if 'battery_start' in list(df):
        df = classify_battery(df)

    # get difference in time from start & end
    df['duration'] = df['timestamp_end'] - df['timestamp_start']
    df['duration_min'] = round(df['duration'].dt.total_seconds().div(60))

    # get trip type - long trip, trip
    df.loc[(df['duration'] > '2:00:00'), 'type'] = 'long_trip'
    df['type'] = df['type'].fillna('trip')

    return df