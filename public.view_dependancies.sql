--DROP VIEW public.view_dependancies
CREATE VIEW public.view_dependancies as 
SELECT DISTINCT 
 srcobj.oid AS src_oid
 ,srcnsp.nspname AS src_schemaname
 ,srcobj.relname AS src_objectname
 ,tgtobj.oid AS dependent_viewoid
 ,tgtnsp.nspname AS dependent_schemaname
 ,tgtobj.relname AS dependent_objectname
FROM
 pg_catalog.pg_class AS srcobj
INNER JOIN
 pg_catalog.pg_depend AS srcdep
 ON srcobj.oid = srcdep.refobjid
INNER JOIN
 pg_catalog.pg_depend AS tgtdep
 ON srcdep.objid = tgtdep.objid
JOIN
 pg_catalog.pg_class AS tgtobj
 ON tgtdep.refobjid = tgtobj.oid
 AND srcobj.oid <> tgtobj.oid
LEFT OUTER JOIN
 pg_catalog.pg_namespace AS srcnsp
 ON srcobj.relnamespace = srcnsp.oid
LEFT OUTER JOIN
 pg_catalog.pg_namespace tgtnsp
 ON tgtobj.relnamespace = tgtnsp.oid
WHERE tgtdep.deptype = 'i'
AND tgtobj.relkind = 'v'
