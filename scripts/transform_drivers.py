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

drivers = pd.DataFrame()
for s in num_races.keys():
    with open(raw_data_dir + f'drivers/{s}_drivers.json') as f:
        data = json.load(f)
        f.close()
        drivers_norm = pd.json_normalize(data['DriverTable']['Drivers'])
        drivers = pd.concat([drivers_norm, drivers])

drivers_deduped = drivers.drop_duplicates()

drivers_renamed = drivers_deduped.rename(columns = {
    'dateOfBirth' : 'date_of_birth',
    'driverId' : 'driver_ref',
    'familyName' : 'last_name',
    'givenName' : 'first_name',
    'permanentNumber' : 'number'
})

drivers_renamed['name'] = drivers_renamed['first_name'] + ' ' + drivers_renamed['last_name']

drivers_final = drivers_renamed[['driver_ref', 'name', 'first_name',
                         'last_name', 'code', 'nationality', 'date_of_birth', 'number','url']]

drivers_final.to_csv(processed_data_dir + 'drivers.csv', index=False)