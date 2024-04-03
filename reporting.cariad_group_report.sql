--DROP VIEW reporting.report_cariad_group_report;
--CREATE VIEW reporting.report_cariad_group_report as
with chp as (
Select
importdate,
vs, 
oe_1,  
SUM(approved_positions) approved_positions,
SUM(group_secondments) group_secondments,
SUM(inactive_contracts) inactive_contracts,
SUM(cariad_natives) cariad_natives,
SUM(occupied_positions) occupied_positions,
SUM(open_positions) open_positions,  
SUM(group_secondments) + SUM(inactive_contracts) + SUM(cariad_natives) cariad_se_actual

from reporting.report_current_headcount_positions_plus_historical
where business_filter = 'Nicht Rausgefiltert'
group by 1,2,3)
,rs as (
Select
importdate,
subsidiary_region cariad_group,
entity,
  CASE 
When aggr3 = 'Other group/brand commissions' THEN 'Engineering Rest'
When aggr3 = 'Third-party business' THEN 'Engineering Rest'
When aggr3 = 'Non-CARIAD business' THEN 'Engineering Rest'
When aggr3 = 'R&D activities' THEN 'Engineering Rest'
When aggr3 = 'Not yet assignable to an Domain/CSF' THEN 'CARIAD business (not yet assignable)'
WHEN aggr3 = 'G&A' THEN 'G&A'
WHEN aggr3 = 'Not yet assigned / assignable' Then 'Not yet assigned / assignable'
WHEN aggr3 = 'Other/non-allocable G&A HC' Then 'G&A'
ELSE null
END
vs,
aggr3,
  Sum(headcount) cariad_se_actual
from core.regionsandsubsidiaries
  
  where aggr0 = 'HC for CARIAD per Domain/CSF' and importdate = date
group by 1,2,3,4,5
  )
,final as (
Select
'Current Headcount Positions' source,
chp.importdate,
chp.vs, 
chp.oe_1,  
'CARIAD SE' company,
'CARIAD SE' entity,
cariad_se_actual,
approved_positions,
group_secondments,
inactive_contracts,
cariad_natives,
occupied_positions,
open_positions
  
from chp
UNION ALL
Select 
'Region & Subsidiary' source,
rs.importdate,
rs.vs, 
rs.aggr3 oe_1,  
rs.cariad_group company,
rs.entity,
cariad_se_actual,
null approved_positions,
null group_secondments,
null inactive_contracts,
null cariad_natives,
null occupied_positions,
null open_positions  
from rs
)

Select 
source,
final.importdate,
coalesce(final.vs, chp.vs) vs, 
final.oe_1,  
company,
entity,
final.cariad_se_actual,
final.approved_positions,
final.group_secondments,
final.inactive_contracts,
final.cariad_natives,
final.occupied_positions,
final.open_positions

from final 
left join chp on
chp.importdate = final.importdate and
chp.oe_1 = final.oe_1 
and source = 'Region & Subsidiary'