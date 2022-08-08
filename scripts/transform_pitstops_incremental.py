import json
import pandas as pd
import os
from datetime import datetime
import sys

args = sys.argv
ingestion_date = args[1]

incremental_raw_data_dir = os.getenv('incremental_raw_data_dir')
incremental_processed_data_dir = os.getenv('incremental_processed_data_dir')

source_dir = os.path.join(incremental_raw_data_dir, ingestion_date)
target_dir = os.path.join(incremental_processed_data_dir, ingestion_date)

with open(os.path.join(source_dir, 'pitstops.json'), 'r') as f:
    data = json.load(f)
    f.close()
    if data['RaceTable']['Races']:
        pits_norm = pd.json_normalize(data['RaceTable']['Races'], record_path=['PitStops'], meta=['round', 'season'])


pits_renamed = pits_norm.rename(columns = {'driverId' : 'driver_ref'})

pits_renamed['race_ref'] = pits_renamed['season'].astype(str) + '_' + pits_renamed['round'].astype(str)

pits_final = pits_renamed[['race_ref', 'driver_ref', 'stop', 'lap', 'time', 'duration']]

if not os.path.exists(target_dir):
    os.mkdir(target_dir)

pits_final.to_csv(os.path.join(target_dir,'pitstops.csv'), index=False)