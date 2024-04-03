-- DROP view reporting.dim_employee_distances
Create view reporting.dim_employee_distances as
Select 
ed.personalnumber_id, 
ed.importdate,
max(case when ed.office_id = 'Office_001' then distance_in_km else null end) office_001_distance_in_km,
max(case when ed.office_id = 'Office_002' then distance_in_km else null end) office_002_distance_in_km,
max(case when ed.office_id = 'Office_003' then distance_in_km else null end) office_003_distance_in_km,
max(case when ed.office_id = 'Office_004' then distance_in_km else null end) office_004_distance_in_km,
max(case when ed.office_id = 'Office_005' then distance_in_km else null end) office_005_distance_in_km,
max(case when ed.office_id = 'Office_006' then distance_in_km else null end) office_006_distance_in_km,
max(case when ed.office_id = 'Office_007' then distance_in_km else null end) office_007_distance_in_km,
max(case when ed.office_id = 'Office_008' then distance_in_km else null end) office_008_distance_in_km,
max(case when ed.office_id = 'Office_009' then distance_in_km else null end) office_009_distance_in_km,
max(case when ed.office_id = 'Office_010' then distance_in_km else null end) office_010_distance_in_km,
max(case when ed.office_id = 'Office_011' then distance_in_km else null end) office_011_distance_in_km,
max(case when ed.office_id = 'Office_012' then distance_in_km else null end) office_012_distance_in_km,
max(case when ed.office_id = 'Office_013' then distance_in_km else null end) office_013_distance_in_km,
max(case when ed.office_id = 'Office_014' then distance_in_km else null end) office_014_distance_in_km,
max(case when ed.office_id = 'Office_015' then distance_in_km else null end) office_015_distance_in_km,
max(case when ed.office_id = 'Office_001' then distance_in_mins else null end) office_001_distance_in_min,
max(case when ed.office_id = 'Office_002' then distance_in_mins else null end) office_002_distance_in_min,
max(case when ed.office_id = 'Office_003' then distance_in_mins else null end) office_003_distance_in_min,
max(case when ed.office_id = 'Office_004' then distance_in_mins else null end) office_004_distance_in_min,
max(case when ed.office_id = 'Office_005' then distance_in_mins else null end) office_005_distance_in_min,
max(case when ed.office_id = 'Office_006' then distance_in_mins else null end) office_006_distance_in_min,
max(case when ed.office_id = 'Office_007' then distance_in_mins else null end) office_007_distance_in_min,
max(case when ed.office_id = 'Office_008' then distance_in_mins else null end) office_008_distance_in_min,
max(case when ed.office_id = 'Office_009' then distance_in_mins else null end) office_009_distance_in_min,
max(case when ed.office_id = 'Office_010' then distance_in_mins else null end) office_010_distance_in_min,
max(case when ed.office_id = 'Office_011' then distance_in_mins else null end) office_011_distance_in_min,
max(case when ed.office_id = 'Office_012' then distance_in_mins else null end) office_012_distance_in_min,
max(case when ed.office_id = 'Office_013' then distance_in_mins else null end) office_013_distance_in_min,
max(case when ed.office_id = 'Office_014' then distance_in_mins else null end) office_014_distance_in_min,
max(case when ed.office_id = 'Office_015' then distance_in_mins else null end) office_015_distance_in_min,
max(case when ed.office_id = 'Office_001' then office_name else null end) office_001_name,
max(case when ed.office_id = 'Office_002' then office_name else null end) office_002_name,
max(case when ed.office_id = 'Office_003' then office_name else null end) office_003_name,
max(case when ed.office_id = 'Office_004' then office_name else null end) office_004_name,
max(case when ed.office_id = 'Office_005' then office_name else null end) office_005_name,
max(case when ed.office_id = 'Office_006' then office_name else null end) office_006_name,
max(case when ed.office_id = 'Office_007' then office_name else null end) office_007_name,
max(case when ed.office_id = 'Office_008' then office_name else null end) office_008_name,
max(case when ed.office_id = 'Office_009' then office_name else null end) office_009_name,
max(case when ed.office_id = 'Office_010' then office_name else null end) office_010_name,
max(case when ed.office_id = 'Office_011' then office_name else null end) office_011_name,
max(case when ed.office_id = 'Office_012' then office_name else null end) office_012_name,
max(case when ed.office_id = 'Office_013' then office_name else null end) office_013_name,
max(case when ed.office_id = 'Office_014' then office_name else null end) office_014_name,
max(case when ed.office_id = 'Office_015' then office_name else null end) office_015_name
from core.employee_distances ed 
left join core.employee_distances_mapping edm 
on ed.office_id = edm.office_id
and ed.importdate = edm.importdate
group by 1,2