-- DROP VIEW reporting.dim_planstellen_entries;
--CREATE VIEW reporting.dim_planstellen_entries as 

with entries as (
Select 
personalnumber_Id,
importdate,
gueltigkeitsbeginn,
case when massnahmenarttext in
('Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)',
'Wiedereinst./Rückkehr befr. EU' 
) then date(gueltigkeitsbeginn) else 
trunc(dateadd(day, -1, date(gueltigkeitsbeginn))) end massnahmenart_date_join,
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
when massnahmenarttext in (
  'Austritt (ADÜ)', 
  'Austritt', 
  'Übergang in Rente'
) then gueltigkeitsbeginn
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
when massnahmenarttext in (
  'Austritt (ADÜ)', 
  'Austritt', 
  'Übergang in Rente'
) then massnahmenarttext
else null
end austritt,
case
when trunc(date_trunc('year', gueltigkeitsbeginn)) = trunc(date_trunc('year', importdate))
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
when trunc(date_trunc('year', gueltigkeitsbeginn)) >= trunc(date_trunc('year', importdate))
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
when trunc(date_trunc('month', gueltigkeitsbeginn)) = trunc(date_trunc('month', importdate))
and massnahmenarttext in (
'Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)',
'Wiedereinst./Rückkehr befr. EU' 
) then 1
else 0
end onboarded_this_month,
case
when trunc(date_trunc('year', gueltigkeitsbeginn)) = trunc(date_trunc('year', importdate))
and massnahmenarttext in (
  'Austritt (ADÜ)', 
  'Austritt', 
  'Übergang in Rente'
) then 1
else 0
end exits_ytd,
case
when gueltigkeitsbeginn > importdate
and massnahmenarttext in (
  'Austritt (ADÜ)', 
  'Austritt', 
  'Übergang in Rente'
) then 1
else 0
end future_exits,
case
when trunc(date_trunc('month', gueltigkeitsbeginn)) = trunc(date_trunc('month', importdate))
and massnahmenarttext in (
  'Austritt (ADÜ)', 
  'Austritt', 
  'Übergang in Rente'
) then 1
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
when massnahmenarttext in (
  'Austritt (ADÜ)', 
  'Austritt', 
  'Übergang in Rente'
)
and gueltigkeitsbeginn > importdate
and gueltigkeitsbeginn <= date_add('month', 6, importdate) then 1
else 0
end exit_next_six_month
  
from hr.core.massnahmen
  where 
  massnahmenarttext <> ''  and exits_this_month = 1 and importdate = ' 2024-01-31'
)
,pstxprn as (
Select
id_planstelle,
personalnumber_id,
endedatum,
beginndatum,
importdate
from
hr.core.planstellen_pst_prn

  )

Select 
id_planstelle,
entries.importdate,
beginndatum,
endedatum,    
massnahmenart_date_join,
gueltigkeitsbeginn,
gueltigkeitsende,
entries.personalnumber_id,
SUM(future_hires) future_hires,
SUM(hired_next_six_month) hired_next_six_month,
SUM(onboarded_this_month) onboarded_this_month,		
SUM(exits_this_month) exits_this_month 			                                                                     
from entries
left join pstxprn
on entries.importdate = pstxprn.importdate
and entries.personalnumber_Id = pstxprn.personalnumber_id
and massnahmenart_date_join >= beginndatum
and massnahmenart_date_join <= endedatum   
where ( future_hires <> 0 or hired_next_six_month <> 0 or onboarded_this_month <> 0 or exits_this_month <> 0 ) 
group by 1,2,3,4,5,6,7,8
