-- DROP VIEW reporting.report_training_forecast
CREATE VIEW reporting.report_training_forecast as 
With td as (
Select max(training_title) training_title, importdate,training_type_id 
from core.group_trainingsdata 
where training_type_id is not null and training_title is not null
group by 2,3
)
Select 
monthly_effective_date,
case when yhat_lower<0 then 0 else yhat_lower end yhat_lower,
case when yhat<0 then 0 else yhat end yhat,
case when yhat_upper<0 then 0 else yhat_upper end yhat_upper,
cap,
tf.training_type_id,
coalesce(cluster_id, tf.training_type_id) cluster_id,
coalesce(training_title, tf.training_type_id, cluster_id) training_title,
tf.importdate,
is_forecast,
quality_issue


from core.trainings_forecast tf
left join td
on tf.importdate = td.importdate and
tf.training_type_id = td.training_type_id 
where (quality_issue <> 'non-computable' or quality_issue is null)