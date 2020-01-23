import pandas as pd
import geopandas as gpd

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

def daily_trips(df):
    # filter just trips (no long/charge)
    df = df[df['type'] == 'trip']
    # total daily trips
    daily_trips = df.groupby(df['timestamp_start'].dt.date)['trip_id'].count()
    daily_trips = daily_trips.reset_index()

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
    
    return gdf