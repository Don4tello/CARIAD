--DROP view reporting.report_current_headcount_positions_plus_historical; 
Create view reporting.report_current_headcount_positions_plus_historical as 
Select * from reporting.report_current_headcount_positions_plus_historical_old
where is_frozen_sap = true
UNION ALL 
Select * from reporting.report_current_headcount_positions_plus_historical_new
where is_frozen_sap = true
