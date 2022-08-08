import json
import pandas as pd
import os
import sys
from datetime import datetime



args = sys.argv
ingestion_date = args[1]

incremental_raw_data_dir = os.getenv('incremental_raw_data_dir')
incremental_processed_data_dir = os.getenv('incremental_processed_data_dir')

source_dir = os.path.join(incremental_raw_data_dir, ingestion_date, 'laps')
target_dir = os.path.join(incremental_processed_data_dir, ingestion_date)

laps = pd.DataFrame()
for file in os.listdir(source_dir):
    with open(os.path.join(source_dir, file), 'r') as f:
        data = json.load(f)
        f.close()
    if data['RaceTable']['Races']:
        laps_norm = pd.json_normalize(data['RaceTable']['Races'], record_path=['Laps', ['Timings']], meta=[['Timings', 'number'], 'season', 'round', ])   
        laps = pd.concat([laps, laps_norm])
                
laps_renamed = laps.rename(columns={'driverId' : 'driver_ref', 'Timings.number' : 'lap_number'})

laps_renamed['race_ref'] = laps_renamed['season'].astype(str) + '_' + laps_renamed['round'].astype(str)

laps_final = laps_renamed[['race_ref', 'driver_ref', 'lap_number', 'position', 'time']]

if not os.path.exists(target_dir):
    os.mkdir(target_dir)
laps_final.to_csv(os.path.join(target_dir, 'laps.csv'), index=False)