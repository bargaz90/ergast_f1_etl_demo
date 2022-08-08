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

constructors = pd.DataFrame()
for s in num_races.keys():
    with open(raw_data_dir + f'constructors/{s}_constructors.json') as f:
        data = json.load(f)
        f.close()
        constructors_norm = pd.json_normalize(data['ConstructorTable']['Constructors'])
        constructors = pd.concat([constructors_norm, constructors])


constructors_deduped = constructors.drop_duplicates()

constructors_renamed = constructors_deduped.rename(columns = {
    'constructorId' : 'constructor_ref',
    'name' : 'constructor_name',
})


constructors_final = constructors_renamed[['constructor_ref', 'constructor_name', 'nationality', 'url']]

constructors_final.to_csv(processed_data_dir + 'constructors.csv', index=False)