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

with open(os.path.join(source_dir, 'qualifying.json'), 'r') as f:
    data = json.load(f)
    f.close()
    q_norm = pd.json_normalize(data['RaceTable']['Races'], record_path=['QualifyingResults'], meta=['round', 'season'])

q_renamed = q_norm.rename(columns = {'Driver.driverId' : 'driver_ref', 'Constructor.constructorId' : 'constructor_ref', 
                                'Driver.permanentNumber' : 'driver_number',
                                'Q1' : 'q1', 'Q2' : 'q2', 'Q3' : 'q3'})

q_renamed['race_ref'] = q_renamed['season'].astype(str) + '_' + q_renamed['round'].astype(str)

q_final = q_renamed[['race_ref', 'driver_ref', 'constructor_ref', 'driver_number', 'position', 'q1', 'q2', 'q3']]

if not os.path.exists(target_dir):
    os.mkdir(target_dir)
q_final.to_csv(os.path.join(target_dir,'qualifying.csv'), index=False)
