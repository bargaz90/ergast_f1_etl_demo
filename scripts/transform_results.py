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

results = pd.DataFrame()
for s in num_races.keys():
    with open(raw_data_dir + f'results/{s}_results.json') as f:
        data = json.load(f)
        f.close()
        results_norm = pd.json_normalize(data['RaceTable']['Races'], record_path='Results', meta=['season', 'round'])
        results = pd.concat([results, results_norm])

        
results_renamed = results.rename(columns = {
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


results_final.to_csv(processed_data_dir + 'results.csv', index=False)