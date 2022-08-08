def get_num_races(ti, start_season, end_season):
	import requests
	import json
	from datetime import datetime
	import os

	data_root_dir = os.getenv('data_root_dir')
	seasons = range(int(start_season), int(end_season) + 1)
	num_races = {}
	for s in seasons:
		url = f'https://ergast.com/api/f1/{s}/races.json?limit=30'
		response = requests.get(url).json()['MRData']
		races_planned = int(response['total'])
		date_today = datetime.today().date()
		for r in range(0, races_planned):
			race_date_str =  response['RaceTable']['Races'][r]['date']
			race_date = datetime.strptime(race_date_str, '%Y-%m-%d').date()
			if date_today > race_date:
				num_races[s] = r + 1
			else:
				break
	num_races_path = data_root_dir + 'num_races.json'
	with open(num_races_path, 'w') as fp:
		json.dump(num_races, fp, sort_keys=True, indent=4)
	return num_races_path
	

def create_endpoint_url(season, table, race=None, limit=30, offset=None):

	if race is not None:
		endpoint = f'{season}/{race}/{table}'
	else:
		endpoint = f'{season}/{table}' 

	url = f'https://ergast.com/api/f1/{endpoint}.json?limit={limit}'

	if offset is not None:
		url = url + f'&offset={offset}'

	return url

def get_season_data(start_season, end_season, tables, limits):
	import requests
	import json
	import os


	raw_data_dir = os.getenv('raw_data_dir')
	seasons = range(int(start_season), int(end_season) + 1)

	for t, l in zip(tables, limits):
		for s in seasons:
			url = create_endpoint_url(s, t, limit = l)
			response = requests.get(url).json()['MRData']
			with open(raw_data_dir + f'{t}/{s}_{t}.json', 'w') as fp:
				json.dump(response, fp, sort_keys=True, indent=4)

def get_race_data(tables, limits):
	import requests
	import json
	import os

	raw_data_dir = os.getenv('raw_data_dir')
	data_root_dir = os.getenv('data_root_dir')
	
	with open(data_root_dir + 'num_races.json') as f:
		num_races = json.load(f)
		f.close()

	for t, l in zip(tables, limits):
		for s in num_races.keys():
			races = range(1, num_races[s] + 1)
			for r in races:
				url = create_endpoint_url(s, t, r, l)
				response = requests.get(url).json()['MRData']
				with open(raw_data_dir + f'{t}/{s}_{r}_{t}.json', 'w') as fp:
					json.dump(response, fp, sort_keys=True, indent=4)

def get_lap_times():
	import requests
	import json
	import os

	raw_data_dir = os.getenv('raw_data_dir')
	data_root_dir = os.getenv('data_root_dir')
	offset = range(0, 1500, 500)
	
	with open(data_root_dir + 'num_races.json') as f:
		num_races = json.load(f)
		f.close()

	for s in num_races.keys():
		for r in range(1, num_races[s] + 1):
			for o in offset:
				url = create_endpoint_url(s, 'laps', r, limit=500, offset=o)
				response = requests.get(url).json()['MRData']
				with open(raw_data_dir + f'laps/{s}_{r}_{o}_laps.json', 'w') as fp:
					json.dump(response, fp, sort_keys=True, indent=4)

def extract_incremental(ti):
	import requests
	import json
	import pandas as pd
	import os
	from datetime import datetime

	base_url = 'https://ergast.com/api/f1/current/last/'
	ingestion_date = datetime.today().strftime("%Y%m%d")

	incremental_raw_data_dir = os.getenv('incremental_raw_data_dir')
	incremental_processed_data_dir = os.getenv('incremental_processed_data_dir')

	tables = ['results', 'qualifying', 'pitstops', 'laps']
	limits = [30, 30, 100, 100]

	target_dir = os.path.join(incremental_raw_data_dir, ingestion_date)
	target_dir_laps = os.path.join(target_dir, 'laps')

	if not os.path.exists(target_dir):
		os.mkdir(target_dir)
		os.mkdir(target_dir_laps)
	for t, l in zip(tables, limits):
		url = base_url + t + '.json?limit=' + str(l)
		if not t == 'laps':
			response = requests.get(url).json()['MRData']
			with open(target_dir + f'/{t}.json', 'w') as fp:
				data = json.dump(response, fp, sort_keys=True, indent=4)
		else:
			num_results = int(requests.get(url).json()['MRData']['total'])
			offset = range(0, num_results, 100)
			for o in offset:
				url = url + f'&offset={o}'
				response = requests.get(url).json()['MRData']
				with open(target_dir_laps + f'/{t}_{o}.json', 'w') as fp:
					data = json.dump(response, fp, sort_keys=True, indent=4)
	return ingestion_date

def check_if_new_rows_exist(ti, postgres_conn_id):
	from airflow.providers.postgres.hooks.postgres import PostgresHook
	import pandas as pd
	import os
	
	ingestion_date = ti.xcom_pull(task_ids='extract_incremental')
	target_dir = os.path.join(os.getenv('incremental_processed_data_dir'), ingestion_date)
	

	hook = PostgresHook(postgres_conn_id=postgres_conn_id)
	cursor = hook.get_conn().cursor()

	num_new_rows = {}
	for file in os.listdir(target_dir):
		table_name = os.path.splitext(file)[0]
		if table_name in ['qualifying', 'laps']:
			df = pd.read_csv(os.path.join(target_dir, file), keep_default_na=False).replace({'' : None})
		elif table_name in ['pitstops']:
			df = pd.read_csv(os.path.join(target_dir, file), keep_default_na=False).replace({'' : None}).astype({'duration' : str})
		else:
			df = pd.read_csv(os.path.join(target_dir, file), keep_default_na=False).replace({'' : None}).astype({'position_text' : str})
		new_rows = df.apply(tuple, 1)
		query = f'''SELECT * FROM ergast_f1.{table_name}'''
		old_rows = cursor.execute(query)
		old_rows = cursor.fetchall()
		old_rows = pd.DataFrame(old_rows).apply(tuple, 1)
		change = ~new_rows.isin(old_rows)
		num_new_rows[table_name] = change.sum()
	
	tables_to_insert_into = [k for k, v in num_new_rows.items() if v > 0]

	if not tables_to_insert_into:
		return False
	return True

def insert_into_pg(ti, postgres_conn_id, table, file):
        import os
        
        check_load_type = ti.xcom_pull(task_ids='check_load_type')
        if check_load_type == "extract_incremental":
            target_dir = os.path.join(os.getenv('incremental_processed_data_dir'), ti.xcom_pull(task_ids='extract_incremental'))
        target_dir = os.getenv('processed_data_dir')
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        hook.copy_expert(f'''COPY {table} FROM STDIN WITH CSV HEADER DELIMITER ',' ''', os.path.join(target_dir, file))