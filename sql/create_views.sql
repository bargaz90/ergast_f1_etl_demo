create or replace view ergast_f1.driver_standings as
(
	select
		cast(LEFT(r.race_ref, 4) as integer) as "season"
	,	row_number() OVER(partition by left(r.race_ref, 4) order by SUM(points) desc) "position"
	,	r.driver_ref
	,	d.nationality "nationality"
	,	r.constructor_ref
	,	SUM(points) "total_points"
	,	COUNT(case when "position" = 1 then 1 else null end) "num_wins"
	,	COUNT(case when "fastest_lap_rank" = 1 then 1 else null end) "num_fastest_laps"
	from ergast_f1.results r 
	left join ergast_f1.drivers d
	on r.driver_ref = d.driver_ref 
	group by LEFT(race_ref, 4), r.driver_ref, d.nationality, constructor_ref
	order by season desc, total_points desc, num_wins desc, MAX(r."position")
);

create or replace view ergast_f1.constructor_standings as
(
	select
		cast(LEFT(r.race_ref, 4) as integer) as "season"
	,	row_number() OVER(partition by left(r.race_ref, 4) order by SUM(points) desc) "position"
	,	r.constructor_ref
	,	c.nationality 
	,	SUM(points) "total_points"
	,	COUNT(case when "position" = 1 then 1 else null end) "num_wins"
	from ergast_f1.results r 
	left join ergast_f1.constructors c 
	on r.constructor_ref = c.constructor_ref 
	group by LEFT(race_ref, 4), r.constructor_ref, c.nationality
	order by season desc, total_points desc, num_wins desc, MAX(r."position")
);

