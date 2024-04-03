--drop view reporting.v_validation_report;
--CREATE VIEW reporting.v_validation_report as 
with
final as (
SELECT
planstellen_attribute.importdate importdatum,
'core.planstellen_attribute' AS source,
pg_catalog.row_number() OVER(
ORDER BY
planstellen_attribute.importdate DESC
) AS rank,
count(*) AS rowcount,
count(DISTINCT planstellen_attribute.id_planstelle) AS distinct_rowcount
FROM
core.planstellen_attribute
WHERE
planstellen_attribute.endedatum_attribute >= planstellen_attribute.importdate
AND planstellen_attribute.beginndatum_attribute <= planstellen_attribute.importdate
GROUP BY
planstellen_attribute.importdate
UNION ALL
SELECT
oe.importdate importdatum,
'core.oe' AS source,
pg_catalog.row_number() OVER(
ORDER BY
oe.importdate DESC
) AS rank,
count(*) AS rowcount,
count(DISTINCT oe.id_oe) AS distinct_rowcount
FROM
core.oe
WHERE
oe.endedatum >= oe.importdate
AND oe.beginndatum <= oe.importdate
GROUP BY
oe.importdate
UNION ALL
SELECT
oexpst.importdate importdatum,
'core.oexpst' AS source,
pg_catalog.row_number() OVER(
ORDER BY
oexpst.importdate DESC
) AS rank,
count(*) AS rowcount,
count(DISTINCT oexpst.id_planstelle) AS distinct_rowcount
FROM
core.oexpst
WHERE
oexpst.endedatum_verknuepfung_zu_planstelle >= oexpst.importdate
AND oexpst.beginndatum_verknuepfung_zu_planstelle <= oexpst.importdate
GROUP BY
oexpst.importdate
UNION ALL
SELECT
planstellen_pst_prn.importdate importdatum,
'core.planstellen_pst_prn' AS source,
pg_catalog.row_number() OVER(
ORDER BY
planstellen_pst_prn.importdate DESC
) AS rank,
count(*) AS rowcount,
count(DISTINCT planstellen_pst_prn.personalnumber_id) AS distinct_rowcount
FROM
core.planstellen_pst_prn
WHERE
planstellen_pst_prn.endedatum >= planstellen_pst_prn.importdate
AND planstellen_pst_prn.beginndatum <= planstellen_pst_prn.importdate
GROUP BY
planstellen_pst_prn.importdate
UNION ALL
SELECT
planstelle_kuerzel.importdate importdatum,
'core.planstelle_kuerzel' AS source,
pg_catalog.row_number() OVER(
ORDER BY
planstelle_kuerzel.importdate DESC
) AS rank,
count(*) AS rowcount,
count(DISTINCT planstelle_kuerzel.id_planstelle) AS distinct_rowcount
FROM
core.planstelle_kuerzel
WHERE
planstelle_kuerzel.endedatum_planstelle >= planstelle_kuerzel.importdate
AND planstelle_kuerzel.beginndatum_planstelle <= planstelle_kuerzel.importdate
GROUP BY
planstelle_kuerzel.importdate
UNION ALL
SELECT
massnahmen.importdate importdatum,
'core.massnahmen' AS source,
pg_catalog.row_number() OVER(
ORDER BY
massnahmen.importdate DESC
) AS rank,
count(*) AS rowcount,
count(DISTINCT massnahmen.personalnumber_id) AS distinct_rowcount
FROM
core.massnahmen
WHERE
massnahmen.gueltigkeitsende >= massnahmen.importdate
AND massnahmen.gueltigkeitsbeginn <= massnahmen.importdate
GROUP BY
massnahmen.importdate
UNION ALL
SELECT
mitarbeiterzusatzdatenohnewa.importdate importdatum,
'core.mitarbeiterzusatzdatenohnewa' AS source,
pg_catalog.row_number() OVER(
ORDER BY
mitarbeiterzusatzdatenohnewa.importdate DESC
) AS rank,
count(*) AS rowcount,
count(
DISTINCT mitarbeiterzusatzdatenohnewa.personalnumber_id
) AS distinct_rowcount
FROM
core.mitarbeiterzusatzdatenohnewa
WHERE
mitarbeiterzusatzdatenohnewa.gueltigkeitsende >= mitarbeiterzusatzdatenohnewa.importdate
AND mitarbeiterzusatzdatenohnewa.gueltigkeitsbeginn <= mitarbeiterzusatzdatenohnewa.importdate
GROUP BY
mitarbeiterzusatzdatenohnewa.importdate
UNION ALL
SELECT
moab_list.importdate AS importdatum,
'core.moab_list' AS source,
pg_catalog.row_number() OVER(
ORDER BY
moab_list.importdate DESC
) AS rank,
count(*) AS rowcount,
count(DISTINCT moab_list.planstellen_id) AS distinct_rowcount
FROM
core.moab_list
GROUP BY
moab_list.importdate
UNION ALL
SELECT
applicationoverview.importdate AS importdatum,
'core.applicationoverview' AS source,
pg_catalog.row_number() OVER(
ORDER BY
applicationoverview.importdate DESC
) AS rank,
count(*) AS rowcount,
count(DISTINCT applicationoverview.sequence_id) AS distinct_rowcount
FROM
core.applicationoverview
GROUP BY
applicationoverview.importdate
UNION ALL
SELECT
dimension_recruiting_job_overview_historical.importdate AS importdatum,
'reporting.dimension_recruiting_job_overview_historical' AS source,
pg_catalog.row_number() OVER(
ORDER BY
dimension_recruiting_job_overview_historical.importdate DESC
) AS rank,
count(*) AS rowcount,
count(
DISTINCT dimension_recruiting_job_overview_historical.job_id
) AS distinct_rowcount
FROM
reporting.dimension_recruiting_job_overview_historical
GROUP BY
dimension_recruiting_job_overview_historical.importdate
UNION ALL
SELECT
joboverview.importdate AS importdatum,
'core.joboverview' AS source,
pg_catalog.row_number() OVER(
ORDER BY
joboverview.importdate DESC
) AS rank,
count(*) AS rowcount,
count(DISTINCT joboverview.jobid) AS distinct_rowcount
FROM
core.joboverview
GROUP BY
joboverview.importdate
UNION ALL
SELECT
dimension_employee_job_profile_data_historical.importdate AS importdatum,
'reporting.dimension_employee_job_profile_data_historical':: text AS source,
pg_catalog.row_number() OVER(
ORDER BY
dimension_employee_job_profile_data_historical.importdate DESC
) AS rank,
count(*) AS rowcount,
count(
DISTINCT dimension_employee_job_profile_data_historical.personalnumber_id
) AS distinct_rowcount
FROM
reporting.dimension_employee_job_profile_data_historical
WHERE
dimension_employee_job_profile_data_historical.is_current = true
GROUP BY
dimension_employee_job_profile_data_historical.importdate,
2
UNION ALL
SELECT
report_employee_current_project_hours_ytd.importdate AS importdatum,
'reporting.report_employee_current_project_hours_ytd':: text AS source,
pg_catalog.row_number() OVER(
ORDER BY
report_employee_current_project_hours_ytd.importdate DESC
) AS rank,
count(*) AS rowcount,
count(
DISTINCT (
(
(
report_employee_current_project_hours_ytd.personalnumber_id
):: text || (
report_employee_current_project_hours_ytd.project
):: text
) || (
report_employee_current_project_hours_ytd.project_id
):: text
)
) AS distinct_rowcount
FROM
reporting.report_employee_current_project_hours_ytd
GROUP BY
report_employee_current_project_hours_ytd.importdate,
2
UNION ALL
SELECT
lt.importdate AS importdatum,
'core.locationtranslation':: text AS source,
pg_catalog.row_number() OVER(
ORDER BY
lt.importdate DESC
) AS rank,
count(*) AS rowcount,
count(
DISTINCT lt.personalteilbereich
) AS distinct_rowcount
FROM
core.locationtranslation lt

GROUP BY
lt.importdate,
2
UNION ALL
SELECT
fact_employee_entries_and_exits_historical.importdate AS importdatum,
'reporting.fact_employee_entries_and_exits_historical':: text AS source,
pg_catalog.row_number() OVER(
ORDER BY
fact_employee_entries_and_exits_historical.importdate DESC
) AS rank,
count(*) AS rowcount,
count(
DISTINCT fact_employee_entries_and_exits_historical.personalnumber_id
) AS distinct_rowcount
FROM
reporting.fact_employee_entries_and_exits_historical
GROUP BY
fact_employee_entries_and_exits_historical.importdate,
2
UNION ALL
SELECT
dimension_employee_personal_data_historical.importdate AS importdatum,
'reporting.dimension_employee_personal_data_historical':: text AS source,
pg_catalog.row_number() OVER(
ORDER BY
dimension_employee_personal_data_historical.importdate DESC
) AS rank,
count(*) AS rowcount,
count(
DISTINCT dimension_employee_personal_data_historical.personalnumber_id
) AS distinct_rowcount
FROM
reporting.dimension_employee_personal_data_historical
GROUP BY
dimension_employee_personal_data_historical.importdate,
2
UNION ALL
SELECT
reporting.report_current_headcount_positions.importdate AS importdatum,
'reporting.report_current_headcount_positions':: text AS source,
pg_catalog.row_number() OVER(
ORDER BY
reporting.report_current_headcount_positions.importdate DESC
) AS rank,
count(*) AS rowcount,
count(
DISTINCT reporting.report_current_headcount_positions.personalnumber_id
) AS distinct_rowcount
FROM
reporting.report_current_headcount_positions
where
is_aktuelle_planstelle = 'Aktuelle Planstellen'
GROUP BY
reporting.report_current_headcount_positions.importdate,
2
  union all
  Select importdate importdatum,'core.regionsandsubsidiaries' as source,pg_catalog.row_number() OVER( ORDER BY importdate DESC ) AS rank, count(*) as rowcount, count(*) as distinct_rowcount from core.regionsandsubsidiaries group by 1 union all
Select importdate importdatum,'core.userskillrating' as source,pg_catalog.row_number() OVER( ORDER BY importdate DESC ) AS rank, count(*) as rowcount, count(*) as distinct_rowcount from core.userskillrating group by 1 union all
Select importdate importdatum,'core.useractivity' as source,pg_catalog.row_number() OVER( ORDER BY importdate DESC ) AS rank, count(*) as rowcount, count(*) as distinct_rowcount from core.useractivity group by 1 union all
Select importdate importdatum,'core.mitarbeiterwa' as source,pg_catalog.row_number() OVER( ORDER BY importdate DESC ) AS rank, count(*) as rowcount, count(*) as distinct_rowcount from core.mitarbeiterwa group by 1 union all
Select importdate importdatum,'core.projecthours' as source,pg_catalog.row_number() OVER( ORDER BY importdate DESC ) AS rank, count(*) as rowcount, count(*) as distinct_rowcount from core.projecthours group by 1 union all
Select importdate importdatum,'core.joboverview' as source,pg_catalog.row_number() OVER( ORDER BY importdate DESC ) AS rank, count(*) as rowcount, count(*) as distinct_rowcount from core.joboverview group by 1

  
)
SELECT
final.importdatum,
final.source,
final.rank,
final.rowcount,
final.distinct_rowcount
from
final
where rank <=12
;