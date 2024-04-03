-- DROP VIEW reporting.dim_massnahmen;
CREATE VIEW reporting.dim_massnahmen as 
with massnahme as (
Select 
personalnumber_Id,
importdate,
massnahmenarttext,
case when massnahmenarttext in
('Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)',
'Wiedereinst./Rückkehr befr. EU' 
) then 'Eintritt' 
else 'Austritt' end massnahmenart_category,
gueltigkeitsbeginn massnahmenart_date,
case when massnahmenarttext in
('Ersteintr. Aktive (ADÜ)',
'Einstellung',
'Direktübernahme (Konzernleihe)',
'Wiedereinst./Rückkehr befr. EU' 
) then date(gueltigkeitsbeginn) else 
trunc(dateadd(day, -1, date(gueltigkeitsbeginn))) end massnahmenart_date_join

from hr.core.massnahmen
  where 
  massnahmenarttext <> '')
, pstxprn as (
Select
id_planstelle,
personalnumber_id,
endedatum,
beginndatum,
importdate
from
hr.core.planstellen_pst_prn
  )
,oexpst as (
Select
id_oe,
id_planstelle,
importdate,
endedatum_verknuepfung_zu_planstelle,
beginndatum_verknuepfung_zu_planstelle  
from
hr.core.oexpst
)
,pst as (Select  
id_planstelle,
beginndatum_attribute,
endedatum_attribute,
zaehlungsrelevante_planstelle planstellen_relevancy,
importdate
from hr.core.planstellen_attribute
              )
,pk as (
Select
id_planstelle,
kuerzel_planstelle,
name_der_planstelle,
endedatum_planstelle,
beginndatum_planstelle,
importdate
from
hr.core.planstelle_kuerzel

  )
,oe_attr as (
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
coalesce(oe8, oe7, oe6, oe5, oe4, oe3, 'K/A') oe,
  endedatum,
beginndatum
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
endedatum,
beginndatum,
importdate
from
hr.core.oe))
,mzd as (Select 
personalnumber_id, 
gueltigkeitsbeginn,
gueltigkeitsende,
mitarbeitergruppe,
mitarbeiterkreis,
importdate
from hr.core.mitarbeiterzusatzdatenohnewa
)
,rw as ( 
Select 
personalnumber_id, 
personalnumber_original, 
max(importdate) importdate,
max(last_name) last_name, 
max(first_name) first_name 
from core.raw_plaintextnames 
group by 1,2)
  
Select

massnahme.personalnumber_id,
massnahme.importdate,
massnahmenarttext,
massnahmenart_category,
massnahmenart_date_join massnahmenart_date,
pstxprn.id_planstelle,
vs,
oe_1,
oe_2,
oe_3,
mitarbeitergruppe,
mitarbeiterkreis,
oe_attr.beginndatum oe_startdate ,  
oe_attr.endedatum oe_enddate, 
beginndatum_verknuepfung_zu_planstelle planstelle_startdate,
endedatum_verknuepfung_zu_planstelle planstelle_enddate,
mzd.gueltigkeitsbeginn mzd_startdate,
mzd.gueltigkeitsende mzd_enddate,
rw.personalnumber_original,
rw.last_name,
rw.first_name,
plan.planstelle_original planstellen_id_original,
oe_level,
oe,
pk.kuerzel_planstelle planstellen_shorttext,
pst.planstellen_relevancy,
case   when planstellen_shorttext = 'MA spe'
  then 'Rausgefiltert'
    when Mitarbeiterkreis in ( '21', '2G', '27', '28', '29', '31') 
  then 'Nicht Rausgefiltert'  else 'Rausgefiltert' end zeus_filter,
case 
  when planstellen_shorttext = 'MA spe'
  then 'Rausgefiltert' 
    when lower(planstellen_relevancy) = 'x' 
  or mitarbeiterkreis in ('21', '2G', '27', '28', '29', '31') 
  then 'Nicht Rausgefiltert'
else 'Rausgefiltert' end as business_filter,
pk.beginndatum_planstelle beginndatum_planstellenkuerzel,
pk.endedatum_planstelle endedatum_planstellenkuerzel
from massnahme
    
left join pstxprn
on massnahme.importdate = pstxprn.importdate
and massnahme.personalnumber_Id = pstxprn.personalnumber_id
and endedatum >= massnahmenart_date_join
and beginndatum <= massnahmenart_date_join 
    
left join oexpst
on massnahme.importdate = oexpst.importdate
and pstxprn.id_planstelle = oexpst.id_planstelle
and endedatum_verknuepfung_zu_planstelle >= massnahmenart_date_join
and beginndatum_verknuepfung_zu_planstelle <= massnahmenart_date_join 
    
left join oe_attr
on massnahme.importdate = oe_attr.importdate
and oexpst.id_oe  = oe_attr.id_oe
and oe_attr.endedatum >= massnahmenart_date_join
and oe_attr.beginndatum <= massnahmenart_date_join 
    
left join mzd
on massnahme.importdate = mzd.importdate
and massnahme.personalnumber_Id = mzd.personalnumber_id   
and mzd.gueltigkeitsende >= massnahmenart_date_join
and mzd.gueltigkeitsbeginn <= massnahmenart_date_join        
   
left join rw on massnahme.personalnumber_id = rw.personalnumber_id 
left join core.raw_plaintext_planstellen plan on
pstxprn.id_planstelle = plan.planstelle_id
AND massnahme.importdate = plan.importdate 
    
left join pst
on massnahme.importdate = pst.importdate
and pstxprn.id_planstelle = pst.id_planstelle   
and pst.endedatum_attribute >= massnahmenart_date_join
and pst.beginndatum_attribute <= massnahmenart_date_join    
    
left join pk
on massnahme.importdate = pk.importdate
and pst.id_planstelle = pk.id_planstelle
and pk.endedatum_planstelle >= massnahmenart_date_join
and pk.beginndatum_planstelle <= massnahmenart_date_join   
    