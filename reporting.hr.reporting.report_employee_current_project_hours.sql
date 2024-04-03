--DROP VIEW hr.reporting.report_employee_current_project_hours;
CREATE VIEW  hr.reporting.report_employee_current_project_hours as
With final as (
Select
personalnumber_id,
planningfield,
solution,
projektid project_id,
projekt project,
jahr as year,
CASE
WHEN (monat = 1) THEN to_date(jahr||'-01-01':: character varying,'YYYY-MM-DD':: text)
WHEN (monat = 2) THEN to_date(jahr||'-02-01':: character varying,'YYYY-MM-DD':: text)
WHEN (monat = 3) THEN to_date(jahr||'-03-01':: character varying,'YYYY-MM-DD':: text)
WHEN (monat = 4) THEN to_date(jahr||'-04-01':: character varying,'YYYY-MM-DD':: text)
WHEN (monat = 5) THEN to_date(jahr||'-05-01':: character varying,'YYYY-MM-DD':: text)
WHEN (monat = 6) THEN to_date(jahr||'-06-01':: character varying,'YYYY-MM-DD':: text)
WHEN (monat = 7) THEN to_date(jahr||'-07-01':: character varying,'YYYY-MM-DD':: text)
WHEN (monat = 8) THEN to_date(jahr||'-08-01':: character varying,'YYYY-MM-DD':: text)
WHEN (monat = 9) THEN to_date(jahr||'-09-01':: character varying,'YYYY-MM-DD':: text)
WHEN (monat = 10) THEN to_date(jahr||'-10-01':: character varying,'YYYY-MM-DD':: text)
WHEN (monat = 11) THEN to_date(jahr||'-11-01':: character varying,'YYYY-MM-DD':: text)
WHEN (monat = 12) THEN to_date(jahr||'-12-01':: character varying,'YYYY-MM-DD':: text)
ELSE to_date(booked_project_date, 'YYYY-MM-DD':: text)
END AS "month",  
importdate,
projektstatus,
project_hierarchy_level_1,
project_hierarchy_level_2,
project_hierarchy_level_3,
project_hierarchy_level_4,
project_hierarchy_level_5,
project_hierarchy_level_6,
internal_external,
bcs_role,
fc_role,
plan_fte,
is_forecast,
SUM(case when istt = 0 then null else istt end ) booked_hours,
SUM(case when buplant = 0 then null else buplant end ) planned_hours

from core.projecthours
  where (istt <> 0 or buplant <> 0) and personalnumber_id is not null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20

), total as (
Select 
personalnumber_id,
planningfield,
solution,
project_id,
project,
year,
month,
importdate,
projektstatus,
project_hierarchy_level_1,
project_hierarchy_level_2,
project_hierarchy_level_3,
project_hierarchy_level_4,
project_hierarchy_level_5,
project_hierarchy_level_6,
internal_external,
bcs_role,
fc_role,
plan_fte,
is_forecast,
booked_hours,
planned_hours,
sum(booked_hours) OVER(
PARTITION BY personalnumber_id,
importdate,
"month" ROWS BETWEEN UNBOUNDED PRECEDING
AND UNBOUNDED FOLLOWING) AS total_booked_hours,
sum(booked_hours) OVER(
PARTITION BY personalnumber_id,
importdate,
"month",
solution ROWS BETWEEN UNBOUNDED PRECEDING
AND UNBOUNDED FOLLOWING) AS total_booked_hours_solution,
sum(booked_hours) OVER(
PARTITION BY personalnumber_id,
importdate,
"month",
planningfield ROWS BETWEEN UNBOUNDED PRECEDING
AND UNBOUNDED FOLLOWING) AS total_booked_hours_planningfield,
sum(
CASE
WHEN (
(
(importdate):: text >= "month"
)
AND (
importdate <= add_months(
(to_date("month", 'YYYY-MM-DD':: text)):: timestamp without time zone,
(3):: bigint
)
)
) THEN booked_hours
ELSE NULL:: double precision
END
) OVER(
PARTITION BY personalnumber_id,
importdate,
project ROWS BETWEEN UNBOUNDED PRECEDING
AND UNBOUNDED FOLLOWING) AS total_booked_hours_project_year,
sum(
CASE
WHEN (
(
(importdate):: text >= "month"
)
AND (
importdate <= add_months(
(to_date("month", 'YYYY-MM-DD':: text)):: timestamp without time zone,
(3):: bigint
)
)
) THEN booked_hours
ELSE NULL:: double precision
END
) OVER(
PARTITION BY personalnumber_id,
importdate ROWS BETWEEN UNBOUNDED PRECEDING
AND UNBOUNDED FOLLOWING
) AS total_booked_hours_year,
sum(
CASE
WHEN (
(
(importdate):: text >= "month"
)
AND (
importdate <= add_months(
(to_date("month", 'YYYY-MM-DD':: text)):: timestamp without time zone,
(3):: bigint
)
)
) THEN booked_hours
ELSE NULL:: double precision
END
) OVER(
PARTITION BY personalnumber_id,
importdate,
solution ROWS BETWEEN UNBOUNDED PRECEDING
AND UNBOUNDED FOLLOWING
) AS total_booked_hours_solution_year,
sum(
CASE
WHEN (
(
(importdate):: text >= "month"
)
AND (
importdate <= add_months(
(to_date("month", 'YYYY-MM-DD':: text)):: timestamp without time zone,
(3):: bigint
)
)
) THEN booked_hours
ELSE NULL:: double precision
END
) OVER(
PARTITION BY personalnumber_id,
importdate,
planningfield ROWS BETWEEN UNBOUNDED PRECEDING
AND UNBOUNDED FOLLOWING
) AS total_booked_hours_planningfield_year,
sum(booked_hours) OVER(
PARTITION BY personalnumber_id,
importdate,
"month",
project_hierarchy_level_3 ROWS BETWEEN UNBOUNDED PRECEDING
AND UNBOUNDED FOLLOWING) AS total_booked_hours_project_hierarchy_level_3,  
sum(
CASE
WHEN (
(
(importdate):: text >= "month"
)
AND (
importdate <= add_months(
(to_date("month", 'YYYY-MM-DD':: text)):: timestamp without time zone,
(3):: bigint
)
)
) THEN booked_hours
ELSE NULL:: double precision
END
) OVER(
PARTITION BY personalnumber_id,
importdate,
project_hierarchy_level_3 ROWS BETWEEN UNBOUNDED PRECEDING
AND UNBOUNDED FOLLOWING
) AS total_booked_hours_project_hierarchy_level_3_year  
from final 

  group by 
  personalnumber_id,
planningfield,
solution,
project_id,
project,
year,
month,
importdate,
projektstatus,
project_hierarchy_level_1,
project_hierarchy_level_2,
project_hierarchy_level_3,
project_hierarchy_level_4,
project_hierarchy_level_5,
project_hierarchy_level_6,
internal_external,
bcs_role,
fc_role,
plan_fte,
is_forecast,
booked_hours,
planned_hours
  )
  
  Select 
personalnumber_id	
,planningfield	
,solution	
,project_id	
,project	
,year
,month
,importdate
,booked_hours		
,planned_hours		
,total_booked_hours		
,total_booked_hours_solution		
,total_booked_hours_planningfield		
,total_booked_hours_project_year		
,total_booked_hours_year		
,total_booked_hours_solution_year
,total_booked_hours_planningfield_year
,total_booked_hours_project_hierarchy_level_3
,total_booked_hours_project_hierarchy_level_3_year,  
  (
total_booked_hours_project_year / total_booked_hours_year
) AS avg_project_perc,
(
total_booked_hours_solution_year / total_booked_hours_year
) AS avg_solution_perc,
(
total_booked_hours_planningfield_year / total_booked_hours_year
) AS avg_planningfield_perc,
(booked_hours / total_booked_hours) AS project_perc,
(
total_booked_hours_solution / total_booked_hours
) AS solution_perc,
(
total_booked_hours_planningfield / total_booked_hours
) AS planningfield_perc
  ,projektstatus
,project_hierarchy_level_1
,project_hierarchy_level_2
,project_hierarchy_level_3
,project_hierarchy_level_4
,project_hierarchy_level_5
,project_hierarchy_level_6
,internal_external
,bcs_role
,fc_role
,plan_fte		
,is_forecast	
,total_booked_hours_project_hierarchy_level_3 / total_booked_hours
 AS project_hierarchy_level_3_perc  ,
total_booked_hours_project_hierarchy_level_3_year / total_booked_hours_year
 AS avg_project_hierarchy_level_3_perc
  from total
  
