import json
import pandas as pd
import sys
import os

args = sys.argv
num_races_path = args[1]
with open(num_races_path, 'r') as f:
    num_races = json.load(f)
    f.close()
raw_data_dir = os.getenv('raw_data_dir')
processed_data_dir = os.getenv('processed_data_dir')

pits = pd.DataFrame()
for s in num_races.keys():
    for r in range(1, num_races[s] + 1):
        with open(raw_data_dir + f'pitstops/{s}_{r}_pitstops.json') as f:
            data = json.load(f)
            f.close()
        if data['RaceTable']['Races']:
            pits_norm = pd.json_normalize(data['RaceTable']['Races'], record_path=['PitStops'], meta=['round', 'season'])
            pits = pd.concat([pits, pits_norm])


pits_renamed = pits.rename(columns = {'driverId' : 'driver_ref'})

pits_renamed['race_ref'] = pits_renamed['season'].astype(str) + '_' + pits_renamed['round'].astype(str)

pits_final = pits_renamed[['race_ref', 'driver_ref', 'stop', 'lap', 'time', 'duration']]

pits_final.to_csv(processed_data_dir + 'pitstops.csv', index=False)