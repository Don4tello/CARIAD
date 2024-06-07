--DROP view reporting.oe_lead cascade;

Create view reporting.oe_lead as

with

oe_attr as (

  Select

  importdate,

  id_oe oe_id,

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

coalesce(oe8, oe7, oe6, oe5, oe4, oe3, 'K/A') oe

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

importdate

from

core.oe

where

endedatum >= importdate

and beginndatum <= importdate

and importdate = (

  select max(importdate) importdate from core.oe where is_frozen_sap = true

)

)

)

, mgr_grp_stg_1 as (

  Select oe, cast(userid as text) userid, row_number() over(partition by oe) rank

  from core.managergroup ),

  mgr_grp_stg_2 as

  (Select

  oe,

  cast( max(case when rank =1 then userid else ' ' end) as text) userid_1,

  cast( max(case when rank =2 then userid else ' ' end) as text) userid_2,

  cast( max(case when rank =3 then userid else ' ' end) as text) userid_3

  from mgr_grp_stg_1

  group by 1), mgr_grp as (

Select oe, 

    case when userid_2 = ' ' and userid_3 = ' ' then userid_1

         when userid_3 = ' ' and userid_2 <> ' ' then CONCAT(userid_1, CONCAT(cast(', ' as text), userid_2))

         when userid_3 <> ' ' and userid_2 <> ' ' then CONCAT(userid_1, CONCAT(CONCAT(cast(', ' as text), userid_2), CONCAT(cast(', ' as text), userid_3) ))

         else 'K/A' end userid

    from mgr_grp_stg_2)

Select

oe_id,

oe_level,

vs,

coalesce( vs1.userid, 'K/A') userid_vs,

oe_1,

coalesce(oe1.userid, vs1.userid, 'K/A') userid_oe1,

oe_2,

coalesce(oe2.userid,oe1.userid, vs1.userid, 'K/A') userid_oe2,

oe_3,

coalesce(oe3.userid,oe2.userid,oe1.userid, vs1.userid, 'K/A') userid_oe3,

oe_4,

coalesce(oe4.userid,oe3.userid,oe2.userid,oe1.userid, vs1.userid, 'K/A') userid_oe4,

oe_5,

coalesce(oe5.userid,oe4.userid,oe3.userid,oe2.userid,oe1.userid, vs1.userid, 'K/A') userid_oe5,

oe_attr.oe

from oe_attr

left join mgr_grp vs1 on oe_attr.vs = vs1.oe

left join mgr_grp oe1 on oe_attr.oe_1 = oe1.oe

left join mgr_grp oe2 on oe_attr.oe_2 = oe2.oe

left join mgr_grp oe3 on oe_attr.oe_3 = oe3.oe

left join mgr_grp oe4 on oe_attr.oe_4 = oe4.oe

left join mgr_grp oe5 on oe_attr.oe_5 = oe5.oe
