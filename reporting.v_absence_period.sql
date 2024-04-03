--DROP VIEW reporting.v_absence_period;
CREATE VIEW reporting.v_absence_period as 

Select
personalnumber_id
,started_at
,ended_at
,absence_presence_type_de
,absence_presence_type_en
,is_absence
,status
,absence_presence_days
,'Aktuelle Planstellen' is_aktuelle_planstelle
,importdate
,absence_presence_type
from core.absence_presence_period app
left join core.absence_presence_translation apt
on absence_presence_type = apt.chronos_type