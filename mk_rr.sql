
-- queries to collect rr_ids, and their related data: rr_qry, session..
-- need this to fill RR (ybcx_rrqs_mst) and RR-QL (ybx_qurr_lnk) 
-- before running display-functions in p_get.sh <pid>

/*

Questions:
  - do we really need rr_id ? why not stick with rr_uuid ?? 
    -> keep the ID, it is easier to search + read
    => later, use the first 8 chars of rr_uuid? 
  - how to determine RR is really over ? 
  - ghost-sm: come from detecting sess from ash, need to fix doubles..
  - need indicator if rr still running ? e.g. found new ash on last poll ? 
  - need toplevel indicator on qury_rr_lnk
  - shouldnt we includ sess_id right away ? 
  - dur(ation) : sec? ms ? float ? 
  - do we need log_host and log_dt for completeness?  => not yet, later.
  - do we need a snapshot (snap_id) ?
    to facilitate linking rr to sess and tsrv/host (as sessions come+go)
    => not at the moment. 
      all linking problems seem +/- solved, or un-solvable due to sampling

*/

\timing on

-- drop view ybx_rrqs_mvw ; 

delete from ybx_qurr_lnk ; 
delete from ybx_rrqs_mst ; 

-- smaller version
-- if using this: needs a view to join relevant sess and duration-ms..
--   drop table ybx_rrqs_mst ;
 create table ybx_rrqs_mst (
  id          bigint  generated always as identity primary key
, sess_id     bigint      -- sess_id, bcse tsrv+pid not unique over time
, rr_uuid     uuid 
, rr_min_dt   timestamptz
, rr_max_dt   timestamptz
, constraint ybx_rrqs_mst_uk unique ( rr_uuid ) ; 
-- client-info, app, : use view
-- fk to tsrv_uuid, 
-- , constraint FK to session
-- fk to sess_id, (implies fk to tsrv, as session is linked to tsrv?)
-- fk to qury_mst: via ybx_qurr_lnk
-- 
);

alter table ybx_rrqs_mst add constraint ybx_rrqs_mst_uk unique ( rr_uuid ) ; 

insert /* rr_01 */ into ybx_rrqs_mst (
  sess_id
, rr_uuid 
, rr_min_dt 
, rr_max_dt
)
select /* rr_01 */ 
  sm.id
, al.root_request_id 
, min ( al.sample_time ) 
, max ( al.sample_time ) 
-- , count ( distinct top_level_node_id ) cnt_top_node
-- , count (distinct pid ) cnt_pid
-- , count ( distinct query_id ) qry
-- , count ( distinct sample_time ) cnt_smpl
 from ybx_ashy_log al 
    , ybx_sess_mst sm
 where  1=1
   and al.top_level_node_id       = sm.tsrv_uuid
   and al.pid                     = sm.pid  -- need time-frame criterium as well...!!
   and al.root_request_id::text   not like '0000%'
   and  al.sample_time            > now() - interval '1 hour'
   and sm.usesysid is not null     -- no ghosts, resolve this by detecting PID from ASH
   and not exists ( select 'x' 
            from ybx_rrqs_mst rm
            where rm.sess_id = sm.id
              and rm.rr_uuid = al.root_request_id
            -- need to build MERGE to UPDATE rrs with higher max_sample_time
           ) 
group by 1, 2
;

/* 
-- need view for joining: database, username
create or replace view ybx_rrqs_mvw as 
select 
  sm.id           as sess_id
, sm.tsrv_uuid   
, sm.pid        
, sm.backend_start 
, sm.host         as host  -- should really be a join
, rm.id           
, rm.rr_uuid
, rm.rr_min_dt
, rm.rr_max_dt
, extract ( epoch from ( rm.rr_max_dt - rm.rr_min_dt ) ) * 1000 as dur_ms
, sm.app_name
from ybx_rrqs_mst rm
   , ybx_sess_mst sm
where rm.sess_id = sm.id
;
*/  

-- now use the rr table to pick up queries per rr..
-- later: pick min/max sample-times per node..
-- for now: just link rr to query
-- detailed reporting per rr can use ashy_log, order by sample-time

-- drop table ybx_qurr_lnk ; 
create table ybx_qurr_lnk (
  queryid   bigint
, rr_id     bigint
, qurr_start_dt timestamp with time zone 
, constraint ybx_qurr_lnk_pk primary key ( queryid, rr_id ) 
-- fk to rr,
-- fk to qry
) ;

-- needs trick to find min-dt from ashy_log
insert into ybx_qurr_lnk ( rr_id, queryid, qurr_start_dt, dur_ms ) 
select /* distinct  */
  rm.id 
, al.query_id  
, min ( al.sample_time ) 
, extract ( epoch from ( max ( al.sample_time ) - min ( al.sample_time ) ) ) * 1000 as dur_ms
from ybx_rrqs_mst rm
   , ybx_ashy_log al
where 1=1
  and rm.rr_uuid = al.root_request_id       -- assume rr is unique
  and not exists ( select 'x'
          from ybx_qurr_lnk ql
          where ql.rr_id   = rm.id
            and ql.queryid = al.query_id
            -- need to add MERGE to UPDATE queries with higher max-sample
          )
group by 1, 2
;


-- now we can run a time-ordered list of activities per RR:

-- select session + RR data..
-- from sess: sess, host, app, backend start, 
--    - from rr: root_req
--       - from ashy: hhmmss.ms, node(10), evenbt (25), 
--          - qry (subtr 60?)
--          - table or tablet if appropriate..
--            for indent, use select 'string' || chr(10) || '  more ' ;
/** 
select 
from ybx_rrqs_mst rm
where rm.root_request_id = '1'::uuid
**/

-- show queries for the rr..
-- (here, some chronological orde would benefit...)

-- select the ASH-log data per rr and per query
-- time hh:mm:ss.ms (order by)
-- host - node
-- query (first part of sql)
-- event
-- tablet [+table] if appropriate

