# for data
import json
import pandas as pd
from pathlib import Path

# for multi-threading
from queue import Queue
from threading import Thread

frames = []

class process_worker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # Get the work from the queue and expand the tuple
            file = self.queue.get()
            print(f'[status] queue size: {self.queue.qsize()}' + ' '*10, end='\r')
            
            try:
                # input json file
                df = convert_json(file)
                frames.append(df)
                
            except:
                print('error')
                continue
            
            finally:
                self.queue.task_done()

def clean_columns(df):
    if 'lastUpdated' in list(df):
        df = df.rename(columns={'lastUpdated':'last_updated'})

    # check if cammelCase (veoride)
    if 'bikeId' in list(df):
        rename = {'isDisabled': 'is_disabled', 'isReserved': 'is_reserved', 'bikeId': 'bike_id'}
        df = df.rename(columns=rename)

    # check if jump & fix naming
    if 'jump_vehicle_type' in list(df):
        df = df.rename(columns={'jump_vehicle_type':'vehicle_type'})

    return df

def convert_json(file):
    with open(file) as f:
        data = json.load(f)

    df = pd.json_normalize(data['data']['bikes'])
    df = clean_columns(df)
    df['timestamp'] = data['last_updated']
    
    return df

def combine_json(files, workers):

    # Create a queue to communicate with the worker threads
    queue = Queue()

    # Create worker threads
    for x in range(workers):   
        worker = process_worker(queue)
        worker.daemon = True
        worker.start()

    # Put the tasks into the queue as a tuple
    for f in files:
        queue.put(f)

    # Causes the main thread to wait for the queue to finish processing all the tasks
    queue.join()

    df = pd.concat(frames)
    
    return df.reset_index(drop=True)

def bike_id_set(bike_id):
    if bike_id.isnumeric():
        return 1
    else:
        return 2

def veh_type(provider):
    if "scooter" in provider:
        return 'scooters'
    else:
        return 'bikes'

def process_json(provider, test=False, workers=100):   
    # load data
    files = list(Path(f'./data/raw/{provider}/{veh_type(provider)}/').glob('*.json'))

    if test:
        files = files[:2000]
    df = combine_json(files, workers)
    
    # spin changed their id, this filters out older ones
    if provider == 'spin-scooters':
        df['bike_set'] = df['bike_id'].apply(bike_id_set)
        df = df[df['bike_set']==2]

    return df