import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from geopy.distance import distance

from src.logger import log_pipeline
from src.router import routing_pipeline

def straight_distance(row):
    start = (row['lat_start'], row['lon_start'])
    end = (row['lat_end'], row['lon_end'])
    dist = distance(start, end).m
    
    return dist

@log_pipeline
def start_pipeline(df, copy=False):
    if copy:
        df = df.copy()
    df = df.sort_values(by=['bike_id', 'timestamp'], ascending=[False, True])

    return df.reset_index(drop=True)

@log_pipeline
def drop_nulls(df):
    return df[df['trip'].notnull()]

@log_pipeline
def convert_time(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    return df

@log_pipeline
def make_trips(df):
    # create trip data by finding movements in bike in lat or lon
    df['end'] = (df['bike_id'] == df['bike_id'].shift())      \
              & (df['lat'].ne(df['lat'].shift())              \
              | (df['lon'].ne(df['lon'].shift())))            \
              & (df['timestamp'].ne(df['timestamp'].shift()))
    
    df['start'] = (df['bike_id'] == df['bike_id'].shift(-1))  \
                & (df['end'].shift(-1) == True)

    df.loc[df['start'] == True, 'trip'] = 'start'
    df.loc[df['end'] == True, 'trip'] = 'end'
    return df

@log_pipeline
def duplicate_start_end(df):
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
        df = df.sort_values(by=['bike_id', 'timestamp'], ascending=[False, True]).reset_index(drop=True)

        return df

@log_pipeline
def add_trip_id(df):
    df = df.sort_values(by=['bike_id', 'timestamp'], ascending=[False, True])
    df = df.reset_index(drop=True)
    
    # adding a trip_id to join start & stop data
    df['trip_id'] = df.groupby(['trip']).cumcount() + 1

    return df

@log_pipeline
def clean_columns(df):
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

    return df

@log_pipeline
def make_points(df):
    # specify type
    df['lat'] = df['lat'].astype(float)
    df['lon'] = df['lon'].astype(float)
    
    # get point geom for spatial work
    points = [Point(xy) for xy in zip(df['lon'], df['lat'])]
    gdf = gpd.GeoDataFrame(df, geometry=points)
    gdf.crs = "epsg:4326"

    return gdf

@log_pipeline
def remove_out_state(gdf):
    print('[status] removing out of state trips')
    # remove trips from outside of ri
    mask = gpd.read_file('./data/files/ri_map.geojson')
    mask = mask['geometry'][0]

    # start location
    gdf['geometry'] = gdf['geometry_start']
    gdf = gdf[gdf['geometry'].within(mask)]
    
    # end location
    gdf['geometry'] = gdf['geometry_end']
    gdf = gdf[gdf['geometry'].within(mask)]

    return gdf

@log_pipeline
def join_neighbor(gdf):
    # adds neighborhood column
    mask = gpd.read_file('./data/files/neighborhoods.geojson')
    mask = mask[['lname', 'geometry']]
    mask = mask.rename(columns={'lname': 'neghbor'})

    # join and remove index_right
    gdf = gpd.tools.sjoin(gdf, mask, how="left", predicate='within')
    gdf.drop('index_right', axis=1, inplace=True)

    return gdf

@log_pipeline
def join_ward(gdf):
    # adds ward column
    mask = gpd.read_file('./data/files/wards.geojson')
    mask = mask[['ward', 'geometry']]

    # join and remove index_right
    gdf = gpd.tools.sjoin(gdf, mask, how="left", predicate='within')
    gdf.drop('index_right', axis=1, inplace=True)

    return pd.DataFrame(gdf)

@log_pipeline
def pivot_trips(df):
    # pivot on trip_id (starts -> end) 
    df = df.sort_values(by=['bike_id', 'timestamp'], ascending=[False, True]).reset_index(drop=True)

    # pivot with start & end
    trips = df.pivot(columns='trip', index='trip_id')

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

@log_pipeline
def remove_out_state(gdf):
    # remove trips from outside of ri
    mask = gpd.read_file('./data/files/ri_map.geojson')
    mask = mask['geometry'][0]

    gdf = gpd.GeoDataFrame(gdf)
    # start location
    gdf['geometry'] = gdf['geometry_start']
    gdf = gdf[gdf['geometry'].within(mask)]
    
    # end location
    gdf['geometry'] = gdf['geometry_end']
    gdf = gdf[gdf['geometry'].within(mask)]

    return pd.DataFrame(gdf)

@log_pipeline
def drop_geom(df):
    # remove geometry
    geom = ['geometry_start', 'geometry_end', 'geometry']
    df.drop(geom, axis=1, inplace=True)

    return df

@log_pipeline
def classify_battery(df):
    # rename really long column header
    if 'jump_ebike_battery_level' in list(df):
        df = df.rename(columns={'jump_ebike_battery_level':'battery'})
    
        print('[status] adding charged trips')
        # convert battery to float
        df['battery_start'] = df['battery_start'].str.rstrip('%').astype('float') / 100.0
        df['battery_end'] = df['battery_end'].str.rstrip('%').astype('float') / 100.0

        # mark when battery is charged more then 10%
        df.loc[(df['battery_end'] - df['battery_start']) > 0.1, 'type'] = 'charge'
    
    return df

@log_pipeline
def get_duration(df):
    # get difference in time from start & end
    df['duration'] = df['timestamp_end'] - df['timestamp_start']
    df['duration_min'] = round(df['duration'].dt.total_seconds().div(60))

    # get trip type - long trip, short trip
    df.loc[(df['duration'] > '2:00:00'), 'type'] = 'long_trip'
    df.loc[(df['duration'] < '0:10:00'), 'type'] = 'short_trip'
    df['type'] = df['type'].fillna('trip')

    return df

@log_pipeline
def missing_distance(df):
    # fill un-routable paths with straight line distance
    df.loc[df['distance']==0, 'distance'] = df.apply(straight_distance, axis=1)

    # convert distance from meters to feet
    df['distance'] = df['distance'] * 3.2808

    return df

@log_pipeline
def get_estimate_speed(df):
    # get speed (mph)
    df['speed_mph'] = (df['distance'] / df['duration_min'])/88

    return df

@log_pipeline
def routing_details(df):
    routed = routing_pipeline(df, 'json', 'car', 30)
    df = df.merge(routed, how='left', on='trip_id')

    return df

@log_pipeline
def make_gpx_routes(df, provider):
    file_path = f'./data/clean/{provider}/clean_bike_routes.geojson'
    gdf = routing_pipeline(df, 'gpx', 'car', 30)
    gdf.to_file(file_path, driver='GeoJSON')

    return df

def data_pipeline(df, provider):
    clean = (df
        .pipe(start_pipeline, copy=False)
        .pipe(make_trips)
        .pipe(duplicate_start_end)
        .pipe(add_trip_id)
        .pipe(clean_columns)
        .pipe(drop_nulls)
        .pipe(convert_time)
        .pipe(make_points)
        .pipe(join_neighbor)
        .pipe(join_ward)
        .pipe(pivot_trips)
        .pipe(remove_out_state)
        .pipe(drop_geom)
        .pipe(classify_battery)
        .pipe(get_duration)
        .pipe(routing_details)
        .pipe(missing_distance)
        .pipe(get_estimate_speed)
        .pipe(make_gpx_routes, provider)
    )

    file_path = f'./data/clean/{provider}/clean_trips.csv'
    clean.to_csv(file_path, index=False)
    return clean

