# requests to local server
import json
import requests
import pandas as pd
import subprocess
import time

# for spatial handling
import gpxpy
import geopandas as gpd
from shapely.geometry import Point, LineString

# for multi-threading
from queue import Queue
from threading import Thread

# ignore sapely 2.0 depricated warning
from warnings import filterwarnings
filterwarnings("ignore")

# path to graphhopper server file
path = 'j:/files/gis/graphhopper/graphhopper-0.12.0/'

# list to hold requests
frames = []

def start_graphhopper():
    cmd = 'cmd.exe /c start run_server.bat'
    res = subprocess.Popen(cmd, cwd=path, shell=True)
    time.sleep(15)

class RouteWorker(Thread):
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
                for trip_id, route_data in data.items():
                    if self.req_type == 'json':
                        df = process_details(route_data, trip_id)
                        frames.append(df)
                    else:
                        gdf = process_routes(route_data, trip_id)
                        frames.append(gdf)
            except:
                continue
                
            finally:
                self.queue.task_done()

def generate_route_requests(df, req_type, veh_type):
    """ generates url to request a route between 2 points

    Args:
        req_type: "json" for details "gpx" for spatial route
        veh_type: car, bike or foot 

    Returns:
        string: a url to send to local server
    """
    url = 'http://127.0.0.1:8989/route?'
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
        routes: url of route between 2 pointss

    Returns:
        dictioanry of gpx information
    """
    gpx_data = {}
    for trip_id, req in routes.items():
        try:
            r = requests.get(req)
        except Exception as e:
            print(e)
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
    
    # zip the coordinates into a point object and convert to gdf
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
    df['trip_id'] = int(trip_id)

    keep_cols = ['trip_id', 'distance', 'time']
    df = df[keep_cols]

    return df

def routing_pipeline(df, req_type, veh_type, workers):
    global frames
    if len(frames) == 0:
        start_graphhopper()

    frames = []

    # load data & chunk
    df_chunks = [df[i::200] for i in range(200)]
    
    # Create a queue to communicate with the worker threads
    queue = Queue()

    # Create worker threads
    for x in range(workers):   
        worker = RouteWorker(queue, req_type)
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
        return df
    else:
        gdf = gpd.GeoDataFrame(df, geometry=df['geometry']).reset_index()
        return gdf