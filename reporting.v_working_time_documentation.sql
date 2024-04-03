--DROP VIEW reporting.v_working_time_documentation;
CREATE VIEW reporting.v_working_time_documentation as 
with work_time as (
SELECT
  wtd.personalnumber_id,
  wtd.absence_presence_at,
  case when wtd.capture_mode = 'true' then 'Arbeitszeiterfassung' else 'Arbeitszeitdokumentation' end capture_mode,
  UPPER(wtd.worked_time_less_or_equal_8) worked_time_less_or_equal_8,
  UPPER(wtd.worked_time_greater_8) worked_time_greater_8,
  UPPER(wtd.worked_time_greater_10) worked_time_greater_10,
  wtd.worked_time,
  wtd.overtime_delta,
  wtd.flextime_delta,
  wtd.flextime_account,
  apt.absence_presence_type_de,
  apt.absence_presence_type_en,
  UPPER(wtd.is_absence) is_absence,
  intra.absence_presence_type_de AS intraday_absence_presence_type_de,
  intra.absence_presence_type_en AS intraday_absence_presence_type_en,
  UPPER(wtd.is_business_day) is_business_day,
  wtd.last_editor,
  'Aktuelle Planstellen' AS is_aktuelle_planstelle,
  wtd.importdate,
  wtd.intraday_absence_presence_type AS intraday_absence_presence_type_id,
  wtd.absence_presence_type AS absence_presence_type_id,
  Rtrim(date_trunc('month', wtd.absence_presence_at), ' 00:00:00') absence_presence_month,
  wtd.updated_at,
  wtd.is_frozen_chronos,
  COALESCE(wtd.delivery_date, wtd.importdate) delivery_date_chronos
FROM
core.working_time_documentation wtd
      LEFT JOIN core.absence_presence_translation apt ON ((wtd.absence_presence_type = apt.chronos_type))    
      LEFT JOIN core.absence_presence_translation intra ON (
      (wtd.intraday_absence_presence_type):: text = ((intra.chronos_type):: character varying):: text)
)     
Select 
  work_time.personalnumber_id,
  absence_presence_at,
  capture_mode,
  worked_time_less_or_equal_8,
  worked_time_greater_8,
  worked_time_greater_10,
  worked_time,
  overtime_delta,
  flextime_delta,
  flextime_account,
  absence_presence_type_de,
  absence_presence_type_en,
  is_absence,
  intraday_absence_presence_type_de,
  intraday_absence_presence_type_en,
  is_business_day,
  last_editor,
  'Aktuelle Planstellen' AS is_aktuelle_planstelle,
  work_time.importdate,
  intraday_absence_presence_type_id,
  absence_presence_type_id,
  absence_presence_month,
  requested_overtime_amount, 
  approved_overtime_amount,
  null as worked_time_greater_12,
  updated_at,
  is_frozen_chronos,
  delivery_date_chronos
  from work_time
     LEFT JOIN reporting.v_overtime_process op on 
      work_time.importdate = op.importdate and
       work_time.personalnumber_id = op.personalnumber_id 
       and absence_presence_month = op.month
       