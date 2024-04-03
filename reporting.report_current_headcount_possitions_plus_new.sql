-- DROP view reporting.report_current_headcount_positions_plus_historical_new CASCADE; 
Create view reporting.report_current_headcount_positions_plus_historical_new as

with 
-- collect oes and assign accordingly 
oe_attr as (
  Select 
  importdate,
  id_oe,
  oe2 cariad,
coalesce(oe3, 'K/A') vs,
coalesce(oe4, oe3, 'K/A') oe_1,
coalesce(oe5, oe4, oe3, 'K/A') oe_2,
coalesce(oe6, oe5, oe4, oe3, 'K/A') oe_3,
coalesce(oe7, oe6, oe5, oe4, oe3, 'K/A') oe_4,
coalesce(oe8, oe7, oe6, oe5, oe4, oe3, 'K/A') oe_5,
(
case
when oe4 is null THEN 'vs'
when oe5 is null THEN 'oe_1'
when oe6 is null THEN 'oe_2'
when oe7 is null then 'oe_3'
when oe8 is null then 'oe_4'
else 'oe_5'
end
) oe_level,
coalesce(oe8, oe7, oe6, oe5, oe4, oe3, 'K/A') oe
  from (
Select
id_oe,
case
when organisationsebene02_objektkuerzel = '' then null
else organisationsebene02_objektkuerzel
end oe2,
case
when organisationsebene03_objektkuerzel = '' then null
else organisationsebene03_objektkuerzel
end oe3,
case
when organisationsebene04_objektkuerzel = '' then null
else organisationsebene04_objektkuerzel
end oe4,
case
when organisationsebene05_objektkuerzel = '' then null
else organisationsebene05_objektkuerzel
end oe5,
case
when organisationsebene06_objektkuerzel = '' then null
else organisationsebene06_objektkuerzel
end oe6,
case
when organisationsebene07_objektkuerzel = '' then null
else organisationsebene07_objektkuerzel
end oe7,
case
when organisationsebene08_objektkuerzel = '' then null
else organisationsebene08_objektkuerzel
end oe8,
importdate
from
hr.core.oe
where
endedatum >= importdate
and beginndatum <= importdate

  
))
-- collect shortnames of planstelle
,pk as (
Select
id_planstelle,
kuerzel_planstelle,
name_der_planstelle,
importdate
from
hr.core.planstelle_kuerzel
where
endedatum_planstelle >= importdate
and beginndatum_planstelle <= importdate
  )
-- OE to PST connection    
,oexpst as (
Select
id_oe,
id_planstelle,
importdate
from
hr.core.oexpst
where
endedatum_verknuepfung_zu_planstelle >= importdate
and beginndatum_verknuepfung_zu_planstelle <= importdate)
    
-- CREATE FLAT Planstelle with all info possible
,pst_attr as ( Select
pst.id_planstelle,
pk.kuerzel_planstelle planstellen_shorttext,          
ist_fte_wert current_fte_value,
zaehlungsrelevante_planstelle planstellen_relevancy,
stellengrading planstellen_grading,
grading_category planstellen_grading_category,
oe_attr.id_oe oe_id,              
cariad,
vs,
oe_1,
oe_2,
oe_3,
oe_4,
oe_5,
plan.planstelle_original planstellen_id_original,
oe,
oe_level,              
ent.future_hires,
ent.hired_next_six_month,
pst.delivery_date,
pst.is_frozen_sap,
name_der_planstelle planstellen_longtext,
pst.importdate
from
hr.core.planstellen_attribute pst
              
left join oexpst 
on pst.id_planstelle = oexpst.id_planstelle
and pst.importdate = oexpst.importdate
              
left join oe_attr
on oexpst.id_oe = oe_attr.id_oe
and pst.importdate = oe_attr.importdate
              
left join pk
on pst.id_planstelle = pk.id_planstelle 
and pst.importdate = pk.importdate
              
left join core.raw_plaintext_planstellen plan on
pst.id_planstelle = plan.planstelle_id
AND pst.importdate = plan.importdate
              
left join reporting.dim_planstellen_entries ent on
pst.id_planstelle = ent.id_planstelle
AND pst.importdate = ent.importdate   
              
where
endedatum_attribute >= pst.importdate
and beginndatum_attribute <= pst.importdate
and oe_attr.id_oe is not null
                  ),
pstxprn as (
Select
id_planstelle,
personalnumber_id,
importdate
from
hr.core.planstellen_pst_prn
where
planstellen_pst_prn.endedatum >= planstellen_pst_prn.importdate
AND
planstellen_pst_prn.beginndatum <= planstellen_pst_prn.importdate

  )
-- approved positions staging
    
, ap_calc as (
Select
id_planstelle,
importdate,
case when count(personalnumber_id) = 0 then 1 else count(personalnumber_id) end nr_of_emp_on_pst
  from pstxprn
  
  group by 1,2
)
,inactive as (
SELECT
personalnumber_id,
statuskundenindividuell inactive_contracts,
importdate
FROM
hr.core.massnahmen
where
gueltigkeitsende >= importdate
and gueltigkeitsbeginn <= importdate
and statuskundenindividuell = '1'
) 
,entries_exits_stg as (
SELECT
personalnumber_id,
gueltigkeitsbeginn,
gueltigkeitsende,
case
when massnahmenarttext in (
'Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)',
'Wiedereinst./Rückkehr befr. EU'
) then gueltigkeitsbeginn
else null
end eintrittsdatum,
case
when massnahmenarttext in ('Austritt (ADÜ)', 'Austritt', 'Übergang in Rente') then gueltigkeitsbeginn
else null
end austrittsdatum,
massnahmenarttext,
case
when massnahmenarttext in (
'Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)',
'Wiedereinst./Rückkehr befr. EU'
) then massnahmenarttext
else null
end eintritt,
case
when massnahmenarttext in ('Austritt (ADÜ)', 'Austritt','Übergang in Rente') then massnahmenarttext
else null
end austritt,
importdate,
case
when date_trunc('year', gueltigkeitsbeginn) = date_trunc('year', importdate)
and gueltigkeitsbeginn <= importdate
and massnahmenarttext in (
'Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)',
'Wiedereinst./Rückkehr befr. EU'
) then 1
else 0
end onboarded_ytd,
case
when date_trunc('year', gueltigkeitsbeginn) >= date_trunc('year', importdate)
and massnahmenarttext in (
'Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)',
'Wiedereinst./Rückkehr befr. EU'
) then 1
else 0
end hired_ytd,
case
when gueltigkeitsbeginn > importdate
and massnahmenarttext in (
'Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)',
'Wiedereinst./Rückkehr befr. EU'
) then 1
else 0
end future_hires,
case
when date_trunc('month', gueltigkeitsbeginn) = date_trunc('month', importdate)
and massnahmenarttext in (
'Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)',
'Wiedereinst./Rückkehr befr. EU'  
) then 1
else 0
end onboarded_this_month,
case
when date_trunc('year', gueltigkeitsbeginn) = date_trunc('year', importdate)
and massnahmenarttext in ('Austritt (ADÜ)', 'Austritt', 'Übergang in Rente') then 1
else 0
end exits_ytd,
case
when gueltigkeitsbeginn > importdate
and massnahmenarttext in ('Austritt (ADÜ)', 'Austritt', 'Übergang in Rente') then 1
else 0
end future_exits,
case
when date_trunc('month', dateadd('day',-1,gueltigkeitsbeginn)) = date_trunc('month', importdate)
and massnahmenarttext in ('Austritt (ADÜ)', 'Austritt', 'Übergang in Rente') then 1
else 0
end exits_this_month,
case
when massnahmenarttext in (
'Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)',
'Wiedereinst./Rückkehr befr. EU'
)
and gueltigkeitsbeginn > importdate
and gueltigkeitsbeginn <= date_add('month', 6, importdate) then 1
else 0
end hired_next_six_month,
case
when massnahmenarttext in ('Austritt (ADÜ)', 'Austritt','Übergang in Rente')
and gueltigkeitsbeginn > importdate
and gueltigkeitsbeginn <= date_add('month', 6, importdate) then 1
else 0
end exit_next_six_month
FROM
hr.core.massnahmen
where
massnahmenarttext <> ''
),
entries_exits as (
Select
personalnumber_id,
importdate,
max(eintritt) entry_type,
max(austritt) exit_type,
max(eintrittsdatum) entry_date,
max(trunc(dateadd('day',-1,austrittsdatum))) exit_date,
max(onboarded_ytd) onboarded_ytd,
max(hired_ytd) hired_ytd,
max(future_hires) future_hires,
max(onboarded_this_month) onboarded_this_month,
max(exits_ytd) exits_ytd,
max(future_exits) future_exits,
max(exits_this_month) exits_this_month,
max(null) hired_this_month,
max(hired_next_six_month) hired_next_six_month,
max(exit_next_six_month) exit_next_six_month
from
entries_exits_stg

group by
personalnumber_id,
importdate
)
,mzd_attr as (
Select
 staff.personalnumber_id
,mitarbeitergruppe
,mitarbeiterkreis
,staff.personalteilbereich
,kostenstelle cost_center_id
,istmanager is_manager
,personal_grading_category
,pstxprn.id_planstelle
,lt.standort as location
,lt.cluster as city
,lt.region
,inactive_contracts  
,uk.vacation_account_total
,uk.transfer_vacation_account_last_year
,uk.unpaid_leave_account
,jp.jobprofile 
,jp.category 
,jp.cluster 
,jp.code  
,rw.personalnumber_original
,rw.last_name
,rw.first_name
,ws.working_hours_monday
,ws.working_hours_tuesday
,ws.working_hours_wednesday
,ws.working_hours_thursday
,ws.working_hours_friday
,ws.working_hours_saturday
,ws.working_hours_sunday
,ws.working_days_weekly
,ws.working_hours_weekly
,ws.number_realted_working_days_this_month
,cci.is_research_and_development
,per.is_project_effort_recording
,per.reason_project_effort_recording  
,ea.street employee_street
,ea.zipcode employee_zipcode
,ea.city employee_city
,ea.countrycode  employee_countrycode
,diversity.nationalitaet nationality
,diversity.zweitenationalitaet second_nationality
,diversity.drittenationalitaet third_nationality
,diversity.geschlechttext gender
,diversity.alter age  
,diversity.disability  
,1.00/cast( ap_calc.nr_of_emp_on_pst as float) ap
,entry_type			
,exit_type			
,entry_date			
,exit_date		
,onboarded_ytd			
,hired_ytd			
--,future_hires			
,onboarded_this_month			
,exits_ytd			
,future_exits			
,exits_this_month			
,hired_this_month			
--,hired_next_six_month			
,exit_next_six_month			
--,hired_next_six_month - exit_next_six_month exits_entries_six_month	
,case when mitarbeitergruppe = 1 
and inactive_contracts is null then 1
else null end cariad_natives
,case when mitarbeitergruppe = 7
and inactive_contracts is null then 1
else null end group_secondments
,salary_level
,salary_section
,planningfield_name_1
,planningfield_name_2
,planningfield_name_3
,planningfield_hours_1
,planningfield_hours_2
,planningfield_hours_3
,total_booked_hours
,staff.importdate

from
hr.core.mitarbeiterzusatzdatenohnewa staff
 -- add pst to prn connection
left join pstxprn
 on staff.personalnumber_id = pstxprn.personalnumber_id
 and staff.importdate = pstxprn.importdate
-- add region of employement
left join core.locationtranslation lt 
 on staff.personalteilbereich = lt.personalteilbereich
 and staff.importdate = lt.importdate
-- flag inactive contracts
left join inactive  
 on staff.personalnumber_id = inactive.personalnumber_id
 and staff.importdate = inactive.importdate
-- add vacation days
LEFT JOIN core.urlaubskontingente uk on
 staff.personalnumber_id = uk.personalnumber_id
 AND staff.importdate = uk.importdate
-- add job profile
left join reporting.dimension_employee_job_profile_data_historical jp
 on staff.importdate = jp.importdate 
 and staff.personalnumber_Id = jp.personalnumber_id
 and jp.jobprofile is not null
-- add original pn_id  
left join core.raw_plaintextnames rw
  on staff.personalnumber_id = rw.personalnumber_id
  and staff.importdate = rw.importdate
-- add working schedule
left join core.working_schedule ws
 on staff.personalnumber_id = ws.personalnumber_id
 and staff.importdate = ws.importdate
 and ws.valid_until_date >= ws.importdate
 and ws.valid_from_date <= ws.importdate
-- add cost center information
left join core.costcenter_information cci on 
 staff.kostenstelle = cci.cost_center_id
 and staff.importdate = cci.importdate
-- add project effort recording
left join core.project_effort_recording per on
 staff.personalnumber_id = per.personalnumber_id
 and staff.importdate = per.importdate
-- add employee address
left join core.employee_address ea on
 staff.personalnumber_id = ea.personalnumber_id
 and staff.importdate = ea.importdate
 AND ea.valid_until >= ea.importdate
 AND ea.valid_from <= ea.importdate
 -- Add Diversity dat
left join core.personenbezogenedaten diversity on
 staff.personalnumber_id = diversity.personalnumber_id
 and staff.importdate = diversity.importdate
 AND diversity.gueltigkeitsende >= diversity.importdate 
 AND diversity.gueltigkeitsbeginn <= diversity.importdate
 
-- joining ap calculation
left join ap_calc on
 pstxprn.id_planstelle = ap_calc.id_planstelle
 and pstxprn.importdate = ap_calc.importdate
-- adding entries_exits info for aktuelle planstellen
left join entries_exits on
 staff.personalnumber_id = entries_exits.personalnumber_id
 and staff.importdate = entries_exits.importdate
-- adding salary level info
left join core.employee_salary_level sl on
 staff.personalnumber_id = sl.personalnumber_id
 and staff.importdate = sl.importdate  
 AND sl.valid_until >= sl.importdate 
 AND sl.valid_from <= sl.importdate 
-- adding project level details  
LEFT JOIN reporting.dim_projects projects on
  	staff.personalnumber_id = projects.personalnumber_id
	AND staff.importdate = projects.importdate

  
where
  staff.gueltigkeitsende >= staff.importdate
AND staff.gueltigkeitsbeginn <= staff.importdate

  
)
, final as (

Select
 'Aktuelle Planstellen' is_aktuelle_planstelle			
,pst_attr.importdate			
,oe_id			
,pst_attr.id_planstelle planstellen_id			
,planstellen_shorttext			
,personalnumber_id			
,cariad			
,vs			
,oe_1		
,oe_2			
,oe_3			
,oe_4			
,oe_5			
,current_fte_value			
,planstellen_relevancy			
,planstellen_grading			
,planstellen_grading_category			
,personal_grading_category			
,mitarbeitergruppe			
,mitarbeiterkreis			
,cost_center_id			
,is_manager			
,oe_level			
,oe			
,case   when planstellen_shorttext = 'MA spe'
  then 'Rausgefiltert'
  when lower(planstellen_relevancy) = 'x' 
  or mitarbeiterkreis in ('21', '2G', '27', '28', '29', '31') 
  then 'Nicht Rausgefiltert'
else 'Rausgefiltert' end as business_filter
,entry_type			
,exit_type			
,entry_date		
,exit_date				
,cast(case when ap is null then 1 else ap end as float) approved_positions	
,cariad_natives			
,group_secondments			
,inactive_contracts			
,cast ( case when  mzd_attr.personalnumber_id is not null then 1.00 else 0.00 end as float) occupied_positions		
,cast( case when  mzd_attr.personalnumber_id is not null then ap else 0.00 end as float) tb_removed		
,0 future_positions			
,onboarded_ytd			
,hired_ytd			
,future_hires			
,onboarded_this_month			
,exits_ytd			
,future_exits			
,exits_this_month			
,hired_this_month			
,hired_next_six_month			
,exit_next_six_month			
--,exits_entries_six_month			
,0 future_integrations			
,0 positions_not_advertised			
,0 positions_advertised
,0 positions_with_hr_interviews			
,0 contract_offered			
,0 contract_signed			
,location			
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
,case   when planstellen_shorttext = 'MA spe'
  then 'Rausgefiltert'
  when Mitarbeiterkreis in ( '21', '2G', '27', '28', '29', '31') 
  then 'Nicht Rausgefiltert' 
 else 'Rausgefiltert' end zeus_filter    
, ap
,planningfield_name_1
,planningfield_name_2
,planningfield_name_3
,planningfield_hours_1
,planningfield_hours_2
,planningfield_hours_3
,total_booked_hours 
,delivery_date
,is_frozen_sap
,planstellen_longtext
from pst_attr
left join mzd_attr on
 pst_attr.importdate = mzd_attr.importdate
 and pst_attr.id_planstelle = mzd_attr.id_planstelle
) 

Select 

 is_aktuelle_planstelle			
,importdate			
,oe_id			
,planstellen_id			
,planstellen_shorttext			
,personalnumber_id			
,cariad			
,vs			
,oe_1		
,oe_2			
,oe_3			
,oe_4			
,oe_5			
,current_fte_value			
,planstellen_relevancy			
,planstellen_grading			
,planstellen_grading_category			
,personal_grading_category			
,mitarbeitergruppe			
,mitarbeiterkreis			
,cost_center_id			
,is_manager			
,oe_level			
,oe			
,business_filter
,entry_type			
,exit_type			
,entry_date			
,exit_date			
,(case    
when vs||'0'||'-A' = upper(oe_2) then 0.00 
when vs||'0'||'-E' = upper(oe_2) then approved_positions 
when vs||'0'||'-I' = upper(oe_2) then approved_positions 
when vs||'0'||'-Z' = upper(oe_2) then 0.00
when planstellen_relevancy = '' then 0.00   
else approved_positions end) as approved_positions
,cariad_natives			
,group_secondments	
,cast(case    
when vs||'0'||'-A' = upper(oe_2) then 0 
when vs||'0'||'-E' = upper(oe_2) then approved_positions - occupied_positions 
when vs||'0'||'-I' = upper(oe_2) then approved_positions - 0 
when vs||'0'||'-Z' = upper(oe_2) then 0 - occupied_positions
else NVL(cast(approved_positions as float), cast(0.00 as float)) - NVL(cast(occupied_positions as float), cast(0.00 as float)) end as float) as open_positions
,inactive_contracts			
,(case    
when vs||'0'||'-A' = upper(oe_2) then 0 
when vs||'0'||'-E' = upper(oe_2) then occupied_positions 
when vs||'0'||'-I' = upper(oe_2) then 0 
when vs||'0'||'-Z' = upper(oe_2) then occupied_positions
else occupied_positions end) as occupied_positions	
,future_positions			
,onboarded_ytd			
,hired_ytd			
,(cast (future_hires as float) * approved_positions) future_hires			
,onboarded_this_month			
,exits_ytd			
,future_exits			
,exits_this_month			
,hired_this_month			
,(cast(hired_next_six_month as float) * approved_positions) hired_next_six_month		
,(cast(exit_next_six_month as float) * approved_positions) exit_next_six_month
,COALESCE(cast(hired_next_six_month as float) * approved_positions,0) - COALESCE(cast(exit_next_six_month as float) * approved_positions,0) exits_entries_six_month			
,(case    
when vs||'0'||'-A' = upper(oe_2) then 0 
when vs||'0'||'-E' = upper(oe_2) then 0 
when vs||'0'||'-I' = upper(oe_2) then 1 
when vs||'0'||'-Z' = upper(oe_2) then 0
else 0 end) as future_integrations    			
,positions_not_advertised			
,positions_advertised
,positions_with_hr_interviews			
,contract_offered			
,contract_signed			
,location			
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
,zeus_filter        
,(case    
when vs||'0'||'-A' = upper(oe_2) then 0.00 
when vs||'0'||'-E' = upper(oe_2) then 0.00 
when vs||'0'||'-I' = upper(oe_2) then approved_positions 
when vs||'0'||'-Z' = upper(oe_2) then 0.00
else approved_positions end) as blocked_positions
,planningfield_name_1
,planningfield_name_2
,planningfield_name_3
,planningfield_hours_1
,planningfield_hours_2
,planningfield_hours_3
,total_booked_hours
,delivery_date
,is_frozen_sap
,trunc(dateadd('day', -1 , exit_date)) second_exit_date
,planstellen_longtext
    from final
                                                                       
                                                                       where importdate >= '2024-01-31'