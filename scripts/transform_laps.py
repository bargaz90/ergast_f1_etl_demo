import json
import pandas as pd
import sys
import os

offset = range(0, 1500, 500)

args = sys.argv
num_races_path = args[1]
with open(num_races_path, 'r') as f:
    num_races = json.load(f)
    f.close()

raw_data_dir = os.getenv('raw_data_dir')
processed_data_dir = os.getenv('processed_data_dir')

laps = pd.DataFrame()
for s in num_races.keys():
    for r in range(1, num_races[s] + 1):
        for o in offset:
            with open(raw_data_dir + f'/laps/{s}_{r}_{o}_laps.json') as f:
                data = json.load(f)
                f.close()
            if data['RaceTable']['Races']:
                laps_norm = pd.json_normalize(data['RaceTable']['Races'], record_path=['Laps', ['Timings']], meta=[['Timings', 'number'], 'season', 'round', ])   
                laps = pd.concat([laps, laps_norm])
                
laps_renamed = laps.rename(columns={'driverId' : 'driver_ref', 'Timings.number' : 'lap_number'})

laps_renamed['race_ref'] = laps_renamed['season'].astype(str) + '_' + laps_renamed['round'].astype(str)

laps_final = laps_renamed[['race_ref', 'driver_ref', 'lap_number', 'position', 'time']]

laps_final.to_csv(processed_data_dir + 'laps.csv', index=False)