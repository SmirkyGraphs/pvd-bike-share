# requests to local server
import json
import requests
import pandas as pd

# for spatial handling
import gpxpy
import geopandas as gpd
from shapely.geometry import Point, LineString, shape

# for multi-threading
from queue import Queue
from threading import Thread


def generate_route_requests(df, req_type, veh_type):
    """ generates url to request a route between 2 points

    Args:
        req_type: "json" for details "gpx" for spatial route
        veh_type: car, bike or foot 

    Returns:
        string: a url to send to local server
    """
    url = 'http://localhost:8989/route?'
    url_end = f'&type={req_type}&instructions=false&vehicle={veh_type}'

    route_requests = {}
    for i, row in df.iterrows():

        # get trip id
        trip_id = str(row['trip_id'])

        # get start location
        start_lat = str(row['lat_start'])
        start_lon = str(row['lon_start'])
        start_loc = f'point={start_lat}%2C{start_lon}'

        # get end location
        end_lat = str(row['lat_end'])
        end_lon = str(row['lon_end'])
        end_loc = f'point={end_lat}%2C{end_lon}'

        # sending request to graphhopper
        req = url + start_loc + '&' + end_loc + url_end

        # create dict
        route_requests[trip_id] = req
        
    return route_requests

def request_routes(routes):
    """ sends generated url to server and gets dict of gpx information.

    Args:
        routes: url of route between 2 points

    Returns:
        dictioanry of gpx information
    """
    gpx_data = {}
    for trip_id, req in routes.items():
        r = requests.get(req)
        gpx_data[trip_id] = (r.content).decode('utf-8')
    
    return gpx_data

def process_routes(gpx_data, trip_id):
    """ processes gpx routes into linestrings

    Args:
        gpx_data: the input info you wish to process
        trip_id: the corosponding trip attached to the route

    Returns:
        pandas GeoDataFrame
    """
    tracks_frame = []
    gpx = gpxpy.parse(gpx_data)
    tracks = [track for track in gpx.tracks][0]
    segments = [segment for segment in tracks.segments][0]

    for point in segments.points:
        data = {}
        data['Y'] = point.latitude
        data['X'] = point.longitude
        data['T'] = point.time
        tracks_frame.append(data)

    df = pd.DataFrame(tracks_frame)
    df['trip_id'] = str(trip_id)
    
    #zip the coordinates into a point object and convert to gdf
    geometry = [Point(xy) for xy in zip(df.X, df.Y)]
    gdf = gpd.GeoDataFrame(df, geometry=geometry)

    gdf = gdf.groupby(['trip_id'])['geometry'].apply(lambda x: LineString(x.tolist()))
    gdf = gpd.GeoDataFrame(gdf, geometry='geometry')
    
    return gdf

def process_details(data, trip_id):
    """ processes route returning distance & estimated time

    Args:
        data: the input info you wish to process
        trip_id: the corosponding trip attached to the route
    
    Returns:
        pandas dataframe containing trip, distance and time
    """

    data = json.loads(data)
    data = data['paths']

    df = pd.DataFrame.from_dict(data)
    df['trip_id'] = trip_id

    keep_cols = ['trip_id', 'distance', 'time']
    df = df[keep_cols]

    return df

class process_worker(Thread, req_type):
    def __init__(self, queue, req_type):
        Thread.__init__(self)
        self.queue = queue
        self.req_type = req_type

    def run(self):
        while True:
            # Get the work from the queue and expand the tuple
            routes = self.queue.get()
            print(f'[status] queue size: {self.queue.qsize()}' + ' '*10, end='\r')
            try:
                data = request_routes(routes)
                for trip_id, json_data in data.items():
                    if self.req_type == 'json':
                        df = process_details(json_data, trip_id)
                        frames.append(df)
                    else:
                        gdf = process_routes(gpx_data, trip_id)
                        frames.append(gdf)
            except:
                continue
                
            finally:
                self.queue.task_done()

def routing(df, req_type, veh_type):
    # make list for data
    frames = []

    # load data & chunk
    df = pd.read_csv('../data/interim/bike_trips_clean.csv')
    df_chunks = [df[i::200] for i in range(200)]
    print(f'[status] total chunks: {len(df_chunks)}')

    # Create a queue to communicate with the worker threads
    queue = Queue()

    # Create worker threads
    for x in range(20):
        print(f'[status] worker: {x + 1}')
        worker = process_worker(queue, req_type)
        worker.daemon = True
        worker.start()

    # Put the tasks into the queue as a tuple
    for chunk in df_chunks:
        routes = generate_route_requests(chunk, req_type, veh_type)

        for trip_id, route in routes.items():
            route_requests = {}
            route_requests[trip_id] = route
            queue.put(route_requests)

    # Causes the main thread to wait for the queue to finish processing all the tasks
    queue.join()

    df = pd.concat(frames)

    if req_type == 'json':
        df = df.reset_index(drop=True)
        df.to_csv('../data/clean/jump_bike_route_details.csv', index=False)

        return df
    else:
        gdf = gpd.GeoDataFrame(df, geometry=df['geometry']).reset_index()
        gdf.to_file("../data/spatial/full_bike_routes.geojson", driver='GeoJSON')

        return gdf