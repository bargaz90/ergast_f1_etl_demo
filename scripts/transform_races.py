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

races = pd.DataFrame()
for s in num_races.keys():
    with open(raw_data_dir + f'races/{s}_races.json') as f:
        data = json.load(f)
        f.close()

    races_norm = pd.json_normalize(data['RaceTable']['Races'])
    races = pd.concat([races, races_norm])

races['race_ref'] = races['season'].astype(str) + '_' + races['round'].astype(str)

try:
    races_subset = races[['race_ref', 'season', 'round', 'Circuit.circuitId', 'raceName', 'date', 'time', 'url',
                        'FirstPractice.date', 'FirstPractice.time', 'SecondPractice.date', 'SecondPractice.time',
                        'ThirdPractice.date', 'ThirdPractice.time', 'Qualifying.date', 'Qualifying.time',
                         'Sprint.date', 'Sprint.time']]
    races_final = races_subset.rename(columns={'Circuit.circuitId' : 'circuit_ref', 'raceName' : 'race_name',
                                              'FirstPractice.date' : 'fp1_date', 'FirstPractice.time' : 'fp1_time',
                                               'SecondPractice.date' : 'fp2_date', 'SecondPractice.time' : 'fp2_time',
                                               'ThirdPractice.date' : 'fp3_date', 'ThirdPractice.time' : 'fp3_time',
                                               'Qualifying.date' : 'quali_date', 'Qualifying.time' : 'quali_time',
                                               'Sprint.date' : 'sprint_date', 'Sprint.time' : 'sprint_time'
                                              })

except Exception:
    races_subset = races[['race_ref', 'season', 'round', 'Circuit.circuitId', 'raceName', 'date', 'time', 'url']]
    races_final = races_subset.rename(columns={'Circuit.circuitId' : 'circuit_ref', 'raceName' : 'race_name'})

races_final.to_csv(processed_data_dir + 'races.csv', index=False)