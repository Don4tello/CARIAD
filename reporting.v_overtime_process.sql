CREATE VIEW reporting.v_overtime_process as
WIth overtime as (Select 
personalnumber_id , 
requested_month, 
requested_year, 
CASE
WHEN (requested_month = 1) THEN '-01-01':: text
WHEN (requested_month = 2) THEN '-02-01':: text
WHEN (requested_month = 3) THEN '-03-01':: text
WHEN (requested_month = 4) THEN '-04-01':: text
WHEN (requested_month = 5) THEN '-05-01':: text
WHEN (requested_month = 6) THEN '-06-01':: text
WHEN (requested_month = 7) THEN '-07-01':: text
WHEN (requested_month = 8) THEN '-08-01':: text
WHEN (requested_month = 9) THEN '-09-01':: text
WHEN (requested_month = 10) THEN '-10-01':: text
WHEN (requested_month = 11) THEN '-11-01':: text
WHEN (requested_month = 12) THEN '-12-01':: text
ELSE NULL:: text end as 
month,
importdate,
Sum(requested_overtime_amount) requested_overtime_amount, 
Sum(approved_overtime_amount) approved_overtime_amount
from core.overtime_process 
group by 1,2,3,4,5)
                  
Select 
importdate,
personalnumber_id,
'Aktuelle Planstellen' as is_aktuelle_planstelle,
requested_year||month as month,
requested_overtime_amount, 
approved_overtime_amount
            
from overtime                  