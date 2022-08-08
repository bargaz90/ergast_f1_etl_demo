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

q = pd.DataFrame()
for s in num_races.keys():
    for r in range(1, num_races[s] + 1):
        with open(raw_data_dir + f'qualifying/{s}_{r}_qualifying.json') as f:
            data = json.load(f)
            f.close()
        q_norm = pd.json_normalize(data['RaceTable']['Races'], record_path=['QualifyingResults'], meta=['round', 'season'])
        q = pd.concat([q, q_norm])

q_renamed = q.rename(columns = {'Driver.driverId' : 'driver_ref', 'Constructor.constructorId' : 'constructor_ref', 
                                'Driver.permanentNumber' : 'driver_number',
                                'Q1' : 'q1', 'Q2' : 'q2', 'Q3' : 'q3'})

q_renamed['race_ref'] = q_renamed['season'].astype(str) + '_' + q_renamed['round'].astype(str)

q_final = q_renamed[['race_ref', 'driver_ref', 'constructor_ref', 'driver_number', 'position', 'q1', 'q2', 'q3']]

q_final.to_csv(processed_data_dir + 'qualifying.csv', index=False)
