import argparse
import subprocess
from pathlib import Path

from src.process import process_json
from src.cleaner import data_pipeline
from src.reports import report_pipeline

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--provider', dest='provider', required=True, 
                        help='choose providers: jump | bird | lime | veoride | spin')
    parser.add_argument('-t', '--test', dest='test', action='store_true', default=False, 
                        help='runs a small sample test of 2,000 files through the pipeline.')
    parser.add_argument('-s', '--sync', dest='sync', action='store_true', default=False,
                        help='pass this to sync the s3 bucket with new data.')

    args = parser.parse_args()
    return args

def provider_folder(prov):
    # map provider to folder
    map_folder = {
        'jump': 'pvd-jump-bikes',
        'bird': 'pvd-bird-scooters',
        'lime': 'pvd-lime-scooters',
        'spin': 'pvd-spin-scooters',
        'veoride': 'pvd-veoride-scooters'
    }
    return map_folder[prov]

if __name__ == "__main__":
    args = get_args()

    # load provier and create folders
    prov = provider_folder(args.provider)
    Path(f'./data/clean/{prov}/').mkdir(parents=True, exist_ok=True)

    # download all new files from s3
    if args.sync == True:
        print('[status] syncing new files', end='\r')
        cmd = f'cmd.exe /c aws s3 sync s3://{prov} ./data/raw/{prov}'
        proc = subprocess.Popen(cmd, shell=True)
        proc.wait()

    # join data and run the pipeline
    df = process_json(prov, args.test)
    df = data_pipeline(df, prov)
    df = report_pipeline(df, prov)