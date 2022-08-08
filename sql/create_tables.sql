CREATE SCHEMA IF NOT EXISTS ergast_f1;
            
CREATE TABLE IF NOT EXISTS ergast_f1.circuits (
	circuit_ref VARCHAR(255)
,	circuit_name VARCHAR(255)
,	country VARCHAR(255)
,	city VARCHAR(255)
,	latitude NUMERIC
,	longitude NUMERIC
,	url VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS ergast_f1.constructors (
	constructor_ref VARCHAR(255)
,	constructor_name VARCHAR(255)
,	nationality VARCHAR(255) 
,	url VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS ergast_f1.drivers (
	driver_ref VARCHAR(255)
,	name VARCHAR(255)
,	first_name VARCHAR(255)
,	last_name VARCHAR(255)
,	code VARCHAR(3)
,	nationality VARCHAR(255)
,	date_of_birth DATE
,	"number" smallint
,	url VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS ergast_f1.laps (
	race_ref VARCHAR(7)
,	driver_ref VARCHAR(255)
,	lap_number smallint
,	position smallint
,	"time" VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS ergast_f1.pitstops (
	race_ref VARCHAR(7)
,	driver_ref VARCHAR(255)
,	stop smallint
,	lap smallint
,	"time" VARCHAR(30)
,	duration VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS ergast_f1.qualifying (
	race_ref VARCHAR(7)
,	driver_ref VARCHAR(255)
,	constructor_ref VARCHAR(255)
,	driver_number smallint
,	position smallint
,	q1 VARCHAR(30)
,	q2 VARCHAR(30)
,	q3 VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS ergast_f1.races (
	race_ref VARCHAR(7)
,	season smallint
,	"round" smallint
,	circuit_ref VARCHAR(255)
,	race_name VARCHAR(255)
,	"date" DATE
,	"time" TIME
,	url VARCHAR(255)
,	fp1_date DATE
,	fp1_time TIME
,	fp2_date DATE
,	fp2_time TIME
,	fp3_date DATE
,	fp3_time TIME
,	quali_date DATE
,	quali_time TIME
,	sprint_date DATE
,	sprint_time TIME
);

CREATE TABLE IF NOT EXISTS ergast_f1.results (
	race_ref VARCHAR(7)
,	driver_ref VARCHAR(255)
,	constructor_ref VARCHAR(255)
,	position smallint
,	position_text VARCHAR(40)
,	points NUMERIC
,	laps NUMERIC
,	grid smallint
,	status VARCHAR(40)
,	time_text VARCHAR(40)
,	time_ms NUMERIC
,	fastest_lap_avg_speed_value FLOAT
,	fastest_lap_avg_speed_unit VARCHAR(3)
,	fastest_lap_time_text VARCHAR(40)
,	fastest_lap_lap smallint
,	fastest_lap_rank smallint
);
TRUNCATE TABLE ergast_f1.circuits;
TRUNCATE TABLE ergast_f1.constructors;
TRUNCATE TABLE ergast_f1.drivers;
TRUNCATE TABLE ergast_f1.laps;
TRUNCATE TABLE ergast_f1.pitstops;
TRUNCATE TABLE ergast_f1.qualifying;
TRUNCATE TABLE ergast_f1.races;
TRUNCATE TABLE ergast_f1.results;
