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

circuits = pd.DataFrame()
for s in num_races.keys():
    with open(raw_data_dir + f'circuits/{s}_circuits.json') as f:
        data = json.load(f)
        f.close()
        circuits_norm = pd.json_normalize(data['CircuitTable']['Circuits'])
        circuits = pd.concat([circuits, circuits_norm])

circuits_deduped = circuits.drop_duplicates()

circuits_renamed = circuits_deduped.rename(columns = {
    'circuitId' : 'circuit_ref',
    'circuitName' : 'circuit_name',
    'Location.country' : 'country',
    'Location.lat' : 'latitude',
    'Location.locality' : 'city',
    'Location.long' : 'longitude',
    'Location.alt' : 'altitude'
    })

circuits_final = circuits_renamed[['circuit_ref', 'circuit_name',
                           'country', 'city', 'latitude', 'longitude','url']]

circuits_final.to_csv(processed_data_dir + 'circuits.csv', index=False)