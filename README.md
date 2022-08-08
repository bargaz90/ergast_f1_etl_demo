# ergast_f1_etl_demo

This is a demo project to learn how to use Airflow and Great Expectations.

It pulls data from Ergast F1 API https://ergast.com/mrd/ and saves it as json files, does some basic transforms using pandas (combining multiple files into tables, setting data types and saving the output as csv files), validates csv files using Great Expectations and loads the data to PostgreSQL database hosted on Azure.

All the transform are done locally. Airflow runs on WSL (ubuntu 20.04) on win10.

The goal of the project is to explore various Airflow features, such as different operators, hooks, xcoms. DAG accepts three parameters: load_type that can be either of "full" or "incremental" and start_season and end_season that specify boundaries for the full load. In order not to exceed API limits, I've tested full load by loading 5 recent seasons of data (for same reason I chose not to pull some less relevant tables).

Since I have no knowledge whether data available via API is revised (i.e. results, lap times or pit stops duration), incremental load brings only relevant data from latest race. Separate function detects changes in those tables by pulling all rows and comparing them to latest data in order to prevent latest data being loaded into the target twice.

The transform part is pretty nasty, there is some code duplication, but it was a deliberate choice to keep each table's transforms in a separate script in order to facilitate debugging and avoid some giant function with multiple if-then statements and long dicts containing column names. Plus, I couldn't find any info about best practices or how it is done in real world.
