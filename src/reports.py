import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point, LineString

from src.logger import log_pipeline

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

@log_pipeline
def neighborhood_trips(df, provider):
    # get neighborhood start/end and %
    s1 = df.groupby(['neghbor_start', 'neghbor_end']).size()
    s2 = (s1 / s1.groupby(level=0).sum())

    # combine & save output
    neighbor = pd.concat([s1, s2], axis=1).reset_index()
    neighbor = neighbor.rename(columns={0:'count', 1:'percent'})
    neighbor.to_csv(f'./data/clean/{provider}/report_neighborhood_trips.csv', index=False)

    return df

@log_pipeline
def ward_trips(df, provider):
    # get ward start/end and %
    s1 = df.groupby(['ward_start', 'ward_end']).size()
    s2 = (s1 / s1.groupby(level=0).sum())

    # combine & save output
    ward = pd.concat([s1, s2], axis=1).reset_index()
    ward = ward.rename(columns={0:'count', 1:'percent'})
    ward.to_csv(f'./data/clean/{provider}/report_ward_trips.csv', index=False)

    return df

@log_pipeline
def bike_details(df, provider):
    # filter just trips (no long/charge)
    bike_mm = df[df['type'] == 'trip']
    # min date/max date for each bike
    bike_mm = bike_mm.groupby('bike_id')['timestamp_start'].agg(['min', 'max']).reset_index()
    bike_mm['date_diff'] = bike_mm['max'].dt.date - bike_mm['min'].dt.date

    cols = {'min':'min_date', 'max':'max_date'}
    bike_mm = bike_mm.rename(columns=cols)
    
    # get number of trips and charge
    charge = df[df['type'] == 'charge']
    charge = charge.groupby('bike_id').size().reset_index(name='charges_count')
    
    trips = df[df['type'] == 'trip']
    trips = trips.groupby('bike_id').size().reset_index(name='trips_count')
    
    # merge files and save output
    det = trips.merge(charge, how='left', on='bike_id')
    det = det.merge(bike_mm, how='left', on='bike_id')
    det.to_csv(f'./data/clean/{provider}/report_bike_details.csv', index=False)
    
    return df

@log_pipeline
def daily_trips(df, weather, provider):
    # filter just trips (no long/charge)
    trips = df[df['type'] == 'trip']

    # total daily trips
    daily_trips = trips.groupby(trips['timestamp_start'].dt.date)['trip_id'].count().reset_index()

    # rename column and set date format
    daily_trips = daily_trips.rename(columns={'timestamp_start':'date'})
    daily_trips['date'] = pd.to_datetime(daily_trips['date']).dt.strftime('%#m/%#d/%Y')

    # merge with weather and save
    daily_trips = daily_trips.merge(weather, on='date')
    daily_trips.to_csv(f'./data/clean/{provider}/report_daily_trips.csv', index=False)

    return df

@log_pipeline
def make_lines(df, provider):
    for i, row in df.iterrows():
        start = Point(row['lon_start'], row['lat_start'])
        end = Point(row['lon_end'], row['lat_end'])
        line = LineString([Point(start), Point(end)]).wkt
        df.at[i, 'geometry'] = line

    df['geometry'] = df['geometry'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='geometry')

    gdf.crs = {"init": "epsg:4326"}
    gdf = gdf.to_crs({'init': 'epsg:3857'})

    gdf['duration'] = gdf['duration'].astype(str)
    gdf['timestamp_start'] = gdf['timestamp_start'].astype(str)
    gdf.to_file(f'./data/clean/{provider}/report_trip_lines_straight.geojson', driver='GeoJSON')
    
    return df

def report_pipeline(df, prov):

    # get the daily weather data
    weather = pd.read_csv('./data/files/daily_weather.csv')
    weather = daily_weather(weather)

    report = (df
        .pipe(bike_details, prov)
        .pipe(daily_trips, weather, prov)
        .pipe(neighborhood_trips, prov)
        .pipe(ward_trips, prov)
        .pipe(make_lines, prov)
    )

    return report