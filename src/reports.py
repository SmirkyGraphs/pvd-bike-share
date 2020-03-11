import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point, LineString, shape
from geopy.distance import distance

def bike_details(df):
    # filter just trips (no long/charge)
    bike_mm = df[df['type'] == 'trip']
    # min date/max date for each bike
    bike_mm = bike_mm.groupby('bike_id')['timestamp_start'].agg(['min', 'max']).reset_index()
    bike_mm['date_diff'] = bike_mm['max'].dt.date - bike_mm['min'].dt.date

    cols = {'min':'min_date', 'max':'max_date'}
    bike_mm = bike_mm.rename(columns=cols)
    
    # get number of trips and charge
    charge = df[df['type'] == 'charge']
    print(charge.shape)
    charge = charge.groupby('bike_id').size().reset_index(name='charges_count')
    
    trips = df[df['type'] == 'trip']
    print(trips.shape)
    trips = trips.groupby('bike_id').size().reset_index(name='trips_count')
    
    df = trips.merge(charge, how='left', on='bike_id')
    df = df.merge(bike_mm, how='left', on='bike_id')
    
    return df

def daily_trips(df, weather):
    # filter just trips (no long/charge)
    df = df[df['type'] == 'trip']

    # total daily trips
    daily_trips = df.groupby(df['timestamp_start'].dt.date)['trip_id'].count()
    daily_trips = daily_trips.reset_index()

    # rename column and set date format
    daily_trips = daily_trips.rename(columns={'timestamp_start':'date'})
    daily_trips['date'] = pd.to_datetime(daily_trips['date']).dt.strftime('%#m/%#d/%Y')

    # merge with weather
    daily_trips = daily_trips.merge(weather, on='date')

    return daily_trips

def make_lines(df):
    for i, row in df.iterrows():
        start = Point(row['lon_start'], row['lat_start'])
        end = Point(row['lon_end'], row['lat_end'])
        line = LineString([Point(start), Point(end)]).wkt
        df.at[i, 'geometry'] = line

    df['geometry'] = df['geometry'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='geometry')

    gdf.crs = {"init": "epsg:4326"}
    gdf = gdf.to_crs({'init': 'epsg:3857'})

    gdf['timestamp_start'] = gdf['timestamp_start'].astype(str)
    
    return gdf

def straight_distance(row):
    start = (row['lat_start'], row['lon_start'])
    end = (row['lat_end'], row['lon_end'])
    dist = distance(start, end).m
    
    return dist

def merge_details(provider):
    details = pd.read_csv(f'./data/interim/{provider}/trip_route_details.csv')
    trips = pd.read_csv(f'./data/interim/{provider}/trips_clean.csv')
    df = trips.merge(details, on='trip_id')

    # fill un-routable paths with straight line distance
    df.loc[df['distance']==0, 'distance'] = df.apply(straight_distance, axis=1)

    # convert distance from meters to feet
    df['distance'] = df['distance'] * 3.2808

    # get speed (mph)
    df['speed_mph'] = (df['distance'] / df['duration_min'])/88

    # save file
    df.to_csv(f'./data/clean/{provider}/trips_clean.csv', index=False)

    return df

def daily_weather(df):
    rename_cols = {
        'DATE': 'date',
        'AWND': 'avg_wind',
        'PRCP': 'precipitation',
        'TMAX': 'max_temp'
    }

    df = df.rename(columns=rename_cols)
    cols = ['date', 'avg_wind', 'precipitation', 'max_temp']
    df = df[cols]

    return df

def neighborhood_trips(df):
    # get neighborhood start/end and %
    s1 = df.groupby(['neghbor_start', 'neghbor_end']).size()
    s2 = (s1 / s1.groupby(level=0).sum())

    neighbor = pd.concat([s1, s2], axis=1).reset_index()
    neighbor = neighbor.rename(columns={0:'count', 1:'percent'})

    return neighbor

def ward_trips(df):
    # get ward start/end and %
    s1 = df.groupby(['ward_start', 'ward_end']).size()
    s2 = (s1 / s1.groupby(level=0).sum())

    ward = pd.concat([s1, s2], axis=1).reset_index()
    ward = ward.rename(columns={0:'count', 1:'percent'})

    return ward

def run_reports(df, provider):
    df['timestamp_start'] = pd.to_datetime(df['timestamp_start'], utc=True).dt.tz_convert('US/Eastern')
    weather = pd.read_csv('./data/files/daily_weather.csv')
    weather = daily_weather(weather)

    lines = make_lines(df)
    daily = daily_trips(df, weather)
    bike_info = bike_details(df)
    neighbor = neighborhood_trips(df)
    ward = ward_trips(df)

    # save files
    bike_info.to_csv(f'./data/clean/{provider}/bike_details_clean.csv', index=False)
    daily.to_csv(f'./data/clean/{provider}/daily_trips_clean.csv', index=False)
    ward.to_csv(f'./data/clean/{provider}/ward_trips_clean.csv', index=False)
    neighbor.to_csv(f'./data/clean/{provider}/neighborhood_trips_clean.csv', index=False)
    lines.to_file(f"./data/clean/{provider}/straight_lines.geojson", driver='GeoJSON')