from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.models.param import Param
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from includes import get_num_races, create_endpoint_url, get_season_data, get_race_data, get_lap_times, extract_incremental, check_if_new_rows_exist, insert_into_pg
import os


default_args = {
	'start_date' : datetime(2022, 8, 1, 10, 0),
	'schedule_interval' : '@once',
	'catchup' : False,
    'params' : {
        'load_type' : Param("incremental", type='string'),
        'start_season' : Param(2018, min=2015, max=2022),
        'end_season' : Param(2022, min=2015, max=2022)
    }
	}

s_tables = ['circuits', 'drivers', 'constructors', 'races', 'results']
s_limits = [25, 30, 25, 30, 800]

r_tables = ['pitstops', 'qualifying']
r_limits = [80, 25]


with DAG(dag_id='ergast_f1_etl_demo', default_args = default_args) as dag:

    
    def check_load_type(load_type):
        if load_type == "full":
            return "check_num_races"
        return "extract_incremental"

    check_load_type = BranchPythonOperator(
        task_id = 'check_load_type',
        python_callable=check_load_type,
        op_kwargs={'load_type' : "{{ params.load_type }}"}
    )

    with TaskGroup(group_id='extract', prefix_group_id=False) as extract:

        check_num_races = PythonOperator(
        task_id='check_num_races',
        python_callable=get_num_races,
        op_kwargs={'start_season' : "{{ params.start_season }}", 'end_season' : "{{ params.end_season }}"}
        )

        extract_incremental = PythonOperator(
            task_id='extract_incremental',
            python_callable=extract_incremental
        )


        with TaskGroup(group_id='extract_season_data', prefix_group_id=False) as extract_season_data:
            for t in s_tables:
                task = PythonOperator(
					task_id='extract_' + t,
					python_callable=get_season_data,
					op_kwargs={'start_season' : "{{ params.start_season }}", 'end_season' : "{{ params.end_season }}", 'tables' : s_tables, 'limits' : s_limits}
				)
        
        with TaskGroup(group_id='extract_race_data', prefix_group_id=False) as extract_race_data:
            for t in r_tables:
                task = PythonOperator(
					task_id='extract_' + t,
					python_callable=get_race_data,
					op_kwargs={'tables' : r_tables, 'limits' : r_limits}
				)
            
        extract_lap_times = PythonOperator(
			task_id='extract_lap_times',
			python_callable=get_lap_times
			)
    
    with TaskGroup(group_id='transform', prefix_group_id=False) as transform:
        
        with TaskGroup(group_id='transform_incremental', prefix_group_id=False) as transform_incremental:
            for t in ['results', 'qualifying', 'pitstops', 'laps']:
                
                bash_cmd_template = f"""
                python3 "$AIRFLOW_HOME/dags/scripts/transform_{t}_incremental.py" {{{{ ti.xcom_pull(task_ids='extract_incremental') }}}}
                """
                transform_incremental_task = BashOperator(
                    task_id=f'transform_{t}_incremental',
                    bash_command=bash_cmd_template
            )

        check_for_new_rows = ShortCircuitOperator(
                task_id='check_for_new_rows',
                python_callable=check_if_new_rows_exist,
                op_kwargs={'postgres_conn_id' : 'azure_pg_conn_id'}
        )
        
        
        with TaskGroup(group_id='transform_and_validate', prefix_group_id=False) as transform_and_validate:
        
            for t in ['circuits', 'constructors', 'drivers', 'races', 'results', 'pitstops', 'qualifying', 'laps']:  
                bash_cmd_template = f"""
                    python3 "$AIRFLOW_HOME/dags/scripts/transform_{t}.py" {{{{ ti.xcom_pull(task_ids='check_num_races') }}}}
                    """
                transform = BashOperator(
			        task_id=f'transform_{t}',
			        bash_command=bash_cmd_template
			        )

                ge_checkpoint_pass= GreatExpectationsOperator(
				    task_id=f'validate_{t}',
    			    data_context_root_dir=os.getenv('ge_root_dir'),
    			    checkpoint_name=f'{t}',
				    fail_task_on_validation_failure=True,
				    do_xcom_push=False
			    )
                transform >> ge_checkpoint_pass
    
    with TaskGroup(group_id='load', prefix_group_id=False) as load:
        
        create_tables = PostgresOperator(
			task_id='create_tables',
			postgres_conn_id='azure_pg_conn_id',
			sql="sql/create_tables.sql"
		)
     
        with TaskGroup(group_id='insert_full', prefix_group_id=False) as insert_full:
            for t in s_tables + r_tables + ['laps']:
                task = PythonOperator(
					task_id='insert_into_' + t,
					python_callable=insert_into_pg,
					op_kwargs={'postgres_conn_id' : 'azure_pg_conn_id', 'table' : f'ergast_f1.{t}', 'file' : f'{t}.csv'}
				)
        
        with TaskGroup(group_id='insert_incremental', prefix_group_id=False) as insert_incremental_grp:
            for t in ['results', 'pitstops', 'qualifying', 'laps']:
                insert_incremental = PythonOperator(
                    task_id=f'insert_incremental_{t}',
                    python_callable=insert_into_pg,
                    op_kwargs={'postgres_conn_id' : 'azure_pg_conn_id', 'table' : f'ergast_f1.{t}', 'file' : f'{t}.csv'}
                )


        create_views = PostgresOperator(
			task_id="create_views",
			postgres_conn_id='azure_pg_conn_id',
			sql="sql/create_views.sql",
            trigger_rule='one_success'
		)
                
check_load_type >> check_num_races >> extract_season_data >> extract_race_data >> extract_lap_times >> transform_and_validate >> create_tables >> insert_full >> create_views
check_load_type >> extract_incremental >> transform_incremental >> check_for_new_rows >> insert_incremental_grp >> create_views