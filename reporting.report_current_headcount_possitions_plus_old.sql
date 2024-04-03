--DROP view reporting.report_current_headcount_positions_plus_historical_old cascade; 
Create view reporting.report_current_headcount_positions_plus_historical_old as

With mn as (
SELECT
personalnumber_id pn,
gueltigkeitsbeginn,
gueltigkeitsende,
massnahmenarttext,
statuskundenindividuell,
importdate i_datum
FROM
core.massnahmen
where
gueltigkeitsende >= importdate
and gueltigkeitsbeginn <= importdate
--and statuskundenindividuell = '1'
),

entries_exits_stg as (
SELECT
personalnumber_id personalnummer,
gueltigkeitsbeginn,
gueltigkeitsende,
case
when massnahmenarttext in (
'Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)'
) then gueltigkeitsbeginn
else null
end eintrittsdatum,
case
when massnahmenarttext in ('Austritt (ADÜ)', 'Austritt') then gueltigkeitsbeginn
else null
end austrittsdatum,
massnahmenarttext,
case
when massnahmenarttext in (
'Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)'
) then massnahmenarttext
else null
end eintritt,
case
when massnahmenarttext in ('Austritt (ADÜ)', 'Austritt') then massnahmenarttext
else null
end austritt,
importdate,
case
when date_trunc('year', gueltigkeitsbeginn) = date_trunc('year', importdate)
and gueltigkeitsbeginn <= importdate
and massnahmenarttext in (
'Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)'
) then 1
else 0
end onboarded_ytd,
case
when date_trunc('year', gueltigkeitsbeginn) >= date_trunc('year', importdate)
and massnahmenarttext in (
'Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)'
) then 1
else 0
end hired_ytd,
case
when gueltigkeitsbeginn > importdate
and massnahmenarttext in (
'Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)'
) then 1
else 0
end future_hires,
case
when date_trunc('month', gueltigkeitsbeginn) = date_trunc('month', importdate)
and massnahmenarttext in (
'Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)'
) then 1
else 0
end onboarded_this_month,
case
when date_trunc('year', gueltigkeitsbeginn) = date_trunc('year', importdate)
and massnahmenarttext in ('Austritt (ADÜ)', 'Austritt') then 1
else 0
end exits_ytd,
case
when gueltigkeitsbeginn > importdate
and massnahmenarttext in ('Austritt (ADÜ)', 'Austritt') then 1
else 0
end future_exits,
case
when date_trunc('month', gueltigkeitsbeginn) = date_trunc('month', importdate)
and massnahmenarttext in ('Austritt (ADÜ)', 'Austritt') then 1
else 0
end exits_this_month,
case
when massnahmenarttext in (
'Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)'
)
and gueltigkeitsbeginn > importdate
and gueltigkeitsbeginn <= date_add('month', 6, importdate) then 1
else 0
end hired_next_six_month,
case
when massnahmenarttext in ('Austritt (ADÜ)', 'Austritt')
and gueltigkeitsbeginn > importdate
and gueltigkeitsbeginn <= date_add('month', 6, importdate) then 1
else 0
end exit_next_six_month
FROM
core.massnahmen
where
massnahmenarttext <> ''
),
entries_exits as (
Select
personalnummer perso_nummer,
importdate import_datum,
max(eintritt) eintritt,
max(austritt) austritt,
max(eintrittsdatum) eintrittsdatum,
max(austrittsdatum) austrittsdatum,
max(onboarded_ytd) onboarded_ytd,
max(hired_ytd) hired_ytd,
max(future_hires) future_hires,
max(onboarded_this_month) onboarded_this_month,
max(exits_ytd) exits_ytd,
max(future_exits) future_exits,
max(exits_this_month) exits_this_month,
max(0) hired_this_month,
max(hired_next_six_month) hired_next_six_month,
max(exit_next_six_month) exit_next_six_month
from
entries_exits_stg
group by
personalnummer,
importdate
),
mop as (
Select
planstellen_id,
importdate,
Sum(positions_not_advertised) positions_not_advertised,
Sum(positions_advertised) positions_advertised,
Sum(positions_with_hr_interviews) positions_with_hr_interviews,
Sum(contract_offered) contract_offered,
Sum(contract_signed) contract_signed
from
core.moab_list
group by
1,
2
)
, pgc as (
Select
personal_grading_category
from
reporting.report_current_headcount_positions
where
personal_grading_category is not null
group by
1
)
, current_headount_positions as (
  
Select
is_aktuelle_planstelle,
hp.importdate,
oe_id,
hp.planstellen_id,
planstellen_shorttext,
personalnumber_id,
cariad,
vs,
oe_1,
oe_2,
oe_3,
oe_4,
oe_5,
current_fte_value,
planstellen_relevancy,
planstellen_grading,
planstellen_grading_category,
personal_grading_category,
mitarbeitergruppe,
mitarbeiterkreis,
cost_center_id,
is_manager,
oe_level,
oe,
business_filter,
entry_type,
exit_type,
entry_date,
exit_date,
approved_positions,
cariad_natives,
group_secondments,
open_positions,
inactive_contracts,
occupied_positions,
future_positions,
location,
city,
region

  
from
reporting.report_current_headcount_positions hp
UNION ALL
Select
distinct is_aktuelle_planstelle,
gt.importdate,
null oe_id,
null planstellen_id,
null planstellen_shorttext,
null personalnumber_id,
cariad,
vs,
oe_1,
oe_2,
oe_3,
oe_4,
oe_5,
current_fte_value,
planstellen_relevancy,
planstellen_grading,
planstellen_grading_category,
pgc.personal_grading_category,
mitarbeitergruppe,
mitarbeiterkreis,
-999 cost_center_id,
is_manager,
oe_level,
oe,
business_filter,
null entry_type,
null exit_type,
cast(null as date) entry_date,
cast(null as date) exit_date,
0 approved_positions,
0 cariad_natives,
0 group_secondments,
0 open_positions,
0 inactive_contracts,
0 occupied_positions,
0 future_positions,
location,
city,
region
from
reporting.report_current_headcount_positions gt
cross join pgc
where
business_filter = 'Nicht Rausgefiltert'
and is_aktuelle_planstelle = 'Aktuelle Planstellen'
and planstellen_grading_category is not null
)  
, working_schedule as (
Select 
personalnumber_id
,working_hours_monday
,working_hours_tuesday
,working_hours_wednesday
,working_hours_thursday
,working_hours_friday
,working_hours_saturday
,working_hours_sunday
,working_days_weekly
,working_hours_weekly
,number_realted_working_days_this_month
,importdate
from core.working_schedule 
where valid_until_date >= importdate
and valid_from_date <= importdate
)
, ap_calc as (
Select
planstellen_id id_planstelle,
importdate,
case when count(personalnumber_id) = 0 then 1 else count(personalnumber_id) end nr_of_emp_on_pst
  from reporting.report_current_headcount_positions
  where is_aktuelle_planstelle = 'Aktuelle Planstellen'
--  and planstellen_id = 'e2799c40642f9e44573adb017412ad13486cc0921aa44c290cfbf7112ba055b4'
--  and importdate = '2023-11-30'
  group by 1,2
)
, stg_1 as (
Select
is_aktuelle_planstelle,
hp.importdate,
oe_id,
hp.planstellen_id,
planstellen_shorttext,
hp.personalnumber_id,
cariad,
vs,
oe_1,
oe_2,
oe_3,
oe_4,
oe_5,
current_fte_value,
planstellen_relevancy,
planstellen_grading,
planstellen_grading_category,
personal_grading_category,
mitarbeitergruppe,
mitarbeiterkreis,
hp.cost_center_id,
is_manager,
oe_level,
oe,
business_filter,
coalesce(entry_type, eintritt) entry_type,
coalesce(exit_type, austritt) exit_type,
coalesce(entry_date, eintrittsdatum) entry_date,
coalesce(exit_date, austrittsdatum) exit_date,
approved_positions,
cariad_natives,
group_secondments,
open_positions,
inactive_contracts,
occupied_positions,
future_positions,
hp.location,
hp.city,
hp.region,
onboarded_ytd,
hired_ytd,
entries.future_hires future_hires,
onboarded_this_month,
exits_ytd,
future_exits,
exits_this_month,
hired_this_month,
entries.hired_next_six_month hired_next_six_month,
exit_next_six_month,
case when is_aktuelle_planstelle = 'Future Integrations' then 1 else 0 end future_integrations,
mop.positions_not_advertised,
mop.positions_advertised,
mop.positions_with_hr_interviews,
mop.contract_offered,
mop.contract_signed,
jobprofile, 
category, 
cluster, 
code,
nationality, 
second_nationality, 
third_nationality,
gender, 
age,
personalnumber_original,
last_name,
first_name
,ws.working_hours_monday
,ws.working_hours_tuesday
,ws.working_hours_wednesday
,ws.working_hours_thursday
,ws.working_hours_friday
,ws.working_hours_saturday
,ws.working_hours_sunday
,ws.working_days_weekly
,ws.working_hours_weekly
,ws.number_realted_working_days_this_month,
cci.is_research_and_development,
per.is_project_effort_recording,
per.reason_project_effort_recording,
ea.street AS employee_street,
ea.zipcode AS employee_zipcode,
ea.city AS employee_city,
ea.countrycode AS employee_countrycode,
pd.disability,
sl.salary_level,
sl.salary_section,
uk.vacation_account_total,
uk.transfer_vacation_account_last_year,
uk.unpaid_leave_account,    
plan.planstelle_original planstellen_id_original,
nr_of_emp_on_pst  
from
current_headount_positions hp

left join mn on mn.pn = hp.personalnumber_id
and hp.importdate = mn.i_datum
and is_aktuelle_planstelle = 'Aktuelle Planstellen'
left join entries_exits on hp.personalnumber_id = entries_exits.perso_nummer
and hp.importdate = entries_exits.import_datum
left join mop on hp.planstellen_id = mop.planstellen_id
and hp.importdate = mop.importdate
and is_aktuelle_planstelle = 'Aktuelle Planstellen'
left join reporting.dimension_employee_job_profile_data_historical jp
on hp.importdate = jp.importdate 
and hp.personalnumber_Id = jp.personalnumber_id
and is_aktuelle_planstelle = 'Aktuelle Planstellen'  
left join reporting.dimension_employee_personal_data_historical pd
  on hp.importdate = pd.importdate
  and hp.personalnumber_id = pd.personalnumber_id
  and is_aktuelle_planstelle = 'Aktuelle Planstellen'
left join core.raw_plaintextnames rw
  on hp.personalnumber_id = rw.personalnumber_id
  and hp.importdate = rw.importdate
  and is_aktuelle_planstelle = 'Aktuelle Planstellen'
left join working_schedule ws
  on hp.personalnumber_id = ws.personalnumber_id
  and hp.importdate = ws.importdate
  and is_aktuelle_planstelle = 'Aktuelle Planstellen'
LEFT JOIN core.costcenter_information cci ON 
	hp.cost_center_id = cci.cost_center_id
	AND hp.importdate = cci.importdate
	AND hp.is_aktuelle_planstelle = 'Aktuelle Planstellen'
LEFT JOIN core.project_effort_recording per ON
	hp.personalnumber_id = per.personalnumber_id
	AND hp.importdate = per.importdate
	AND hp.is_aktuelle_planstelle = 'Aktuelle Planstellen'
LEFT JOIN (
  SELECT
  employee_address.personalnumber_id,
  employee_address.street,
  employee_address.zipcode,
  employee_address.city,
  employee_address.countrycode,
  employee_address.valid_from,
  employee_address.valid_until,
  employee_address.importdate
  FROM
  core.employee_address
  WHERE
  employee_address.valid_until >= employee_address.importdate
  AND employee_address.valid_from <= employee_address.importdate
  ) ea ON hp.personalnumber_id = ea.personalnumber_id
  AND hp.importdate = ea.importdate
  AND hp.is_aktuelle_planstelle = 'Aktuelle Planstellen'

LEFT JOIN (
SELECT
employee_salary_level.personalnumber_id,
employee_salary_level.salary_level,
employee_salary_level.salary_section,
employee_salary_level.importdate
FROM
core.employee_salary_level
WHERE
employee_salary_level.valid_until >= employee_salary_level.importdate
AND employee_salary_level.valid_from <= employee_salary_level.importdate
) sl ON hp.personalnumber_id = sl.personalnumber_id
AND hp.importdate = sl.importdate
AND hp.is_aktuelle_planstelle = 'Aktuelle Planstellen'
LEFT JOIN core.urlaubskontingente uk on
  	hp.personalnumber_id = uk.personalnumber_id
	AND hp.importdate = uk.importdate
	AND hp.is_aktuelle_planstelle = 'Aktuelle Planstellen'
LEFT JOIN core.raw_plaintext_planstellen plan on
  	hp.planstellen_id = plan.planstelle_id
	AND hp.importdate = plan.importdate
	AND hp.is_aktuelle_planstelle = 'Aktuelle Planstellen'
LEFT JOIN reporting.dim_planstellen_entries entries on
    	hp.planstellen_id = entries.id_planstelle
	AND hp.importdate = entries.importdate
	AND hp.is_aktuelle_planstelle = 'Aktuelle Planstellen'
left join ap_calc on
      	hp.planstellen_id = ap_calc.id_planstelle
	AND hp.importdate = ap_calc.importdate
	AND hp.is_aktuelle_planstelle = 'Aktuelle Planstellen'
)

Select
is_aktuelle_planstelle,
importdate,
oe_id,
planstellen_id,
planstellen_shorttext,
personalnumber_id,
cariad,
vs,
oe_1,
oe_2,
oe_3,
oe_4,
oe_5,
current_fte_value,
planstellen_relevancy,
planstellen_grading,
planstellen_grading_category,
personal_grading_category,
mitarbeitergruppe,
mitarbeiterkreis,
cost_center_id,
is_manager,
oe_level,
oe,
business_filter,
entry_type,
exit_type,
entry_date,
exit_date,
approved_positions,
cariad_natives,
group_secondments,
open_positions,
inactive_contracts,
occupied_positions,
future_positions,
onboarded_ytd,
hired_ytd,
--case when future_hires is null or future_hires = 0 then 0 else 
cast(cast(future_hires as numeric)/  cast(nr_of_emp_on_pst as numeric) as float)   future_hires,
onboarded_this_month,
exits_ytd,
future_exits,
exits_this_month,
null hired_this_month,
--case when hired_next_six_month is null or hired_next_six_month = 0 then 0 else 
cast(cast(hired_next_six_month as numeric)/ cast( nr_of_emp_on_pst as numeric) as float)   hired_next_six_month,
exit_next_six_month,
--case when hired_next_six_month is null or hired_next_six_month = 0 then 0 else 
cast(cast(hired_next_six_month  as numeric) / cast( nr_of_emp_on_pst as numeric) - cast(exit_next_six_month as numeric) as float)  exits_entries_six_month,
future_integrations,
positions_not_advertised,
positions_advertised,
positions_with_hr_interviews,
contract_offered,
contract_signed,
location
,city
,region
,jobprofile
,category
,cluster
,code
,nationality
,second_nationality
,third_nationality
,gender
,age
,personalnumber_original
,last_name
,first_name
,working_hours_monday
,working_hours_tuesday
,working_hours_wednesday
,working_hours_thursday
,working_hours_friday
,working_hours_saturday
,working_hours_sunday
,working_days_weekly
,working_hours_weekly
,number_realted_working_days_this_month
,is_research_and_development
,is_project_effort_recording
,reason_project_effort_recording
,employee_street
,employee_zipcode
,employee_city
,employee_countrycode
,disability
,salary_level
,salary_section
,vacation_account_total
,transfer_vacation_account_last_year
,unpaid_leave_account
,planstellen_id_original
,cast(case when Mitarbeiterkreis in ( '21', '2G', '27', '28', '29', '31') 
  then 'Nicht Rausgefiltert' else 'Rausgefiltert' end as text) zeus_filter
,cast(0.00 as float) blocked_positions
,cast('n/a' as text) planningfield_name_1
,cast('n/a' as text) planningfield_name_2
,cast('n/a' as text) planningfield_name_3
,cast(0.00 as float) planningfield_hours_1
,cast(0.00 as float) planningfield_hours_2
,cast(0.00 as float) planningfield_hours_3
,cast(0.00 as float) total_booked_hours
,importdate delivery_date
,true is_frozen_sap
,trunc(dateadd('day', -1 , exit_date)) second_exit_date
, 'n/a' planstellen_longtext
from
stg_1
  where is_aktuelle_planstelle = 'Aktuelle Planstellen'
  --and planstellen_id = 'e2799c40642f9e44573adb017412ad13486cc0921aa44c290cfbf7112ba055b4'
and importdate < '2024-01-31'

UNION ALL
Select
'Future Integrations' is_aktuelle_planstelle,
fi.importdate,
oe_id,
fi.planstelle_id,
planstellen_shorttext,
fi.personalnumber_id,
cariad,
vs,
oe_1,
oe_2,
oe_3,
oe_4,
oe_5,
current_fte_value,
planstellen_relevancy,
planstellen_grading,
planstellen_grading_category,
personal_grading_category,
mitarbeitergruppe,
mitarbeiterkreis,
cost_center_id,
is_manager,
oe_level,
oe,
business_filter,
entry_type,
exit_type,
entry_date,
exit_date,
0 approved_positions,
0 cariad_natives,
0 group_secondments,
0 open_positions,
0 inactive_contracts,
0 occupied_positions,
0 future_positions,
0 onboarded_ytd,
0 hired_ytd,
0 future_hires,
0 onboarded_this_month,
0 exits_ytd,
0 future_exits,
0 exits_this_month,
null hired_this_month,
0 hired_next_six_month,
0 exit_next_six_month,
0 exits_entries_six_month,
1 future_integrations,
0 positions_not_advertised,
0 positions_advertised,
0 positions_with_hr_interviews,
0 contract_offered,
0 contract_signed,
 location
,city
,region
,jobprofile
,category
,cluster
,code
,nationality
,second_nationality
,third_nationality
,gender
,age
,personalnumber_original
,last_name
,first_name
,0 working_hours_monday
,0 working_hours_tuesday
,0 working_hours_wednesday
,0 working_hours_thursday
,0 working_hours_friday
,0 working_hours_saturday
,0 working_hours_sunday
,0 working_days_weekly
,0 working_hours_weekly
,0 number_realted_working_days_this_month
,is_research_and_development
,is_project_effort_recording
,reason_project_effort_recording
,employee_street
,employee_zipcode
,employee_city
,employee_countrycode
,disability
,salary_level
,salary_section
,0 vacation_account_total
,0 transfer_vacation_account_last_year
,0 unpaid_leave_account
,planstellen_id_original
,cast(case when Mitarbeiterkreis in ( '21', '2G', '27', '28', '29', '31') 
  then 'Nicht Rausgefiltert' else 'Rausgefiltert' end as text) zeus_filter
,cast(0.00 as float) blocked_positions
,cast('n/a' as text) planningfield_name_1
,cast('n/a' as text) planningfield_name_2
,cast('n/a' as text) planningfield_name_3
,cast(0.00 as float) planningfield_hours_1
,cast(0.00 as float) planningfield_hours_2
,cast(0.00 as float) planningfield_hours_3
,cast(0.00 as float) total_booked_hours
,fi.importdate delivery_date
,true is_frozen_sap
,trunc(dateadd('day', -1 , exit_date)) second_exit_date
, 'n/a' planstellen_longtext
from
core.future_integrations fi
left join stg_1 chp on fi.personalnumber_id = chp.personalnumber_id 
and fi.importdate = chp.importdate
and is_aktuelle_planstelle = 'Aktuelle Planstellen'
where fi.importdate < '2024-01-31'

