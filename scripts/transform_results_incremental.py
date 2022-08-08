import json
import pandas as pd
import os
from datetime import datetime
import sys


args = sys.argv
ingestion_date = args[1]

incremental_raw_data_dir = os.getenv('incremental_raw_data_dir')
incremental_processed_data_dir = os.getenv('processed_raw_data_dir')
source_dir = os.path.join(incremental_raw_data_dir, ingestion_date)
target_dir = os.path.join(incremental_processed_data_dir, ingestion_date)

with open(os.path.join(source_dir, 'results.json'), 'r') as f:
    data = json.load(f)
    f.close()
    results_norm = pd.json_normalize(data['RaceTable']['Races'], record_path='Results', meta=['season', 'round'])


        
results_renamed = results_norm.rename(columns = {
    'Driver.driverId' : 'driver_ref',
    'Constructor.constructorId' : 'constructor_ref',
    'positionText' : 'position_text',
    'Time.time' : 'time_text',
    'Time.millis' : 'time_ms',
    'FastestLap.AverageSpeed.speed' : 'fastest_lap_avg_speed_value',
    'FastestLap.AverageSpeed.units' : 'fastest_lap_avg_speed_unit',
    'FastestLap.Time.time' : 'fastest_lap_time_text',
    'FastestLap.lap' : 'fastest_lap_lap',
    'FastestLap.rank' : 'fastest_lap_rank',
})


results_renamed['race_ref'] = results_renamed['season'].astype(str) + '_' + results_renamed['round'].astype(str)

results_final = results_renamed[['race_ref', 'driver_ref', 'constructor_ref',
                'position', 'position_text', 'points', 'laps', 'grid', 'status', 'time_text', 'time_ms',
   'fastest_lap_avg_speed_value', 'fastest_lap_avg_speed_unit', 'fastest_lap_time_text', 'fastest_lap_lap',
   'fastest_lap_rank']]

if not os.path.exists(target_dir):
    os.mkdir(target_dir)
results_final.to_csv(os.path.join(target_dir,'results.csv'), index=False)