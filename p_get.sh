#!//bin/bash

# get data from a pid, using pid as $1
#
# requires: complete ash-log (after finisghing) and mk_rr.sql
#
# todo: 
#   - check for ash-records witout query ? 
# 
# 

OUTFILE=sess${1}.out
OUTFILE2=sess_tree${1}.out

psql -h localhost -p 5433 -U yugabyte -X <<EOF | tee $OUTFILE


drop table ybx_rore_mst ; 
create temporary table ybx_rore_mst (
  rr_uuid   uuid primary key 
, sess_id   bigint
, rr_min_dt timestamptz
, rr_max_dt timestamptz
-- , constraint ybx_rore_mst_fk_sess foreign key (sess_id ) references ybx_sess_mst ( id ) 
); 

drop table ybx_rrqy_lnk ; 
create temporary table ybx_rrqy_lnk (
  queryid   bigint
, rr_uuid   uuid
, qurr_start_dt timestamp with time zone
, dur_ms    bigint
, constraint ybx_rrqy_lnk_pk primary key ( queryid, rr_uuid ) 
-- fk to rr,
-- fk to qry
) ;


insert /* rore_01 */ into ybx_rore_mst (
  sess_id
, rr_uuid
, rr_min_dt
, rr_max_dt
)
select /* rore_01 */
  sm.id
, al.root_request_id
, min ( al.sample_time )
, max ( al.sample_time )
 from ybx_ashy_log al
    , ybx_sess_mst sm
 where  1=1
   and al.top_level_node_id       = sm.tsrv_uuid
   and al.pid                     = sm.pid  -- need time-frame criterium as well...!!
   and sm.pid                = $1
   and al.root_request_id::text   not like '0000%'
   and  al.sample_time            > now() - interval '1 hour'
   and sm.usesysid is not null     -- no ghosts, resolve this by detecting PID from ASH
   and not exists ( select 'x'
            from ybx_rore_mst rm
            where rm.sess_id = sm.id
              and rm.rr_uuid = al.root_request_id
            -- need to build MERGE to UPDATE rrs with higher max_sample_time
           )
group by 1, 2
;

-- insert the relevant RRs, just the pid
insert /* rrqy 01 */ into ybx_rrqy_lnk ( rr_uuid, queryid, qurr_start_dt, dur_ms )
select /* distinct  */
  rm.rr_uuid 
, al.query_id
, min ( al.sample_time )
, extract ( epoch from ( max ( al.sample_time ) - min ( al.sample_time ) ) ) * 1000 as dur_ms
from ybx_rore_mst rm -- no need to select pid, rore only contains relevant records
   , ybx_ashy_log al
where 1=1
  and rm.rr_uuid = al.root_request_id       -- assume rr is unique
  and not exists ( select 'x'
          from ybx_rrqy_lnk ql
          where ql.rr_uuid   = rm.rr_uuid
            and ql.queryid = al.query_id
            -- need to add MERGE to UPDATE queries with higher max-sample
          )
group by 1, 2
;



\set ECHO none
\timing off

\! echo .
\! echo -- -- -- The session for the RR -- -- -- 
\! echo .


select sm.id as sess_id, sm.host, sm.pid, sm.app_name
, substr ( sm.client_addr || ':' || sm.client_port , 1, 25 ) as client_info
, to_char ( sm.backend_start, 'HH24:MI:SS' ) sess_start
-- , to_char ( sm.gone_dt      , 'HH24:MI:SS' ) backend_end
, trunc ( extract ( epoch from ( sm.gone_dt - sm.backend_start ) ), 3)  as dur_secs
from ybx_sess_mst sm
where 1=1
  and sm.pid = $1 
;

\! read -t5 -p "The session info" abc 
\! read -p "The session info" abc 

\! echo .
\! echo -- -- -- time + duration of the RR into the session. -- -- -- 
\! echo .

-- possibly us \get to pick up sess_id, rr_id, rr_uuid, and sess_start
-- then also report how long into the session this rr started 

-- time + duration of the  rrs:
select 
  -- rm.id as rr_id
  substr ( rm.rr_uuid::text, 1, 8 ) rr_uuid
, rm.rr_uuid
, to_char ( rm.rr_min_dt , 'HH24:MI:SS' ) rr_start
, trunc ( extract ( epoch from ( rm.rr_min_dt - sm.backend_start  ) ), 3)  as secs_in
, trunc ( extract ( epoch from ( rm.rr_max_dt - rm.rr_min_dt      ) ), 3)  as rr_duration_s
from ybx_rore_mst rm
   , ybx_sess_mst sm
where rm.sess_id = sm.id
  and sm.pid = $1 
order by rm.rr_min_dt ; 
;

\! read -p "The the root-req from the session " abc 

\! echo .
\! echo -- -- -- most occuring queries, in order of first_occurence  -- -- -- 
\! echo .

select 
        qm.queryid
      , substr ( replace ( qm.query, chr(10), ' '), 1, 60)      as Query
      , count (*)                                               as nr_of_rr
      , to_char ( Min ( ql.qurr_start_dt ), 'HH24:MI:SS.MS' )   as first_occ
      , sum ( ql.dur_ms )                                       as total_ms
from ybx_rore_mst rm
   , ybx_rrqy_lnk ql 
   , ybx_qury_mst qm
   , ybx_sess_mst sm
where rm.rr_uuid  = ql.rr_uuid
  and qm.queryid  = ql.queryid
  and sm.id       = rm.sess_id
  and sm.pid = $1 
group by 1, 2
order by 4
;

\! echo .
\! echo -- -- -- Queries per  RR -- -- -- 
\! echo .

select  case when ( rm.rr_uuid = lag(rm.rr_uuid) over ( order by ql.qurr_start_dt ) ) 
              then null else substr ( rm.rr_uuid::text, 1, 4 ) end    as rr_uuid
      , to_char ( ql.qurr_start_dt , 'HH24:MI:SS.MS' )                as qury_started
      , qm.queryid
      , trunc ( extract(epoch from 
                (ql.qurr_start_dt - lead(ql.qurr_start_dt) over (order by ql.qurr_start_dt))) * -1000
              )                                             as lag_ms
      , ql.dur_ms as  dur_ms
      , substr ( replace ( qm.query, chr(10), ' '), 1, 60)  as Query
from ybx_rore_mst rm
   , ybx_rrqy_lnk ql 
   , ybx_qury_mst qm
   , ybx_sess_mst sm
where 1=1
  and rm.id       = ql.rr_id
  and qm.queryid  = ql.queryid
  and sm.id       = rm.sess_id
  and sm.pid = $1 
order by ql.qurr_start_dt
;


\! read -p "The the root-req from the session " abc 

\! echo .
\! echo -- -- -- Events per RR  -- -- -- 
\! echo .

select
  substr ( rm.rr_uuid::text, 1, 8 ) as rr_uuid
, to_char ( al.sample_time, ' - HH24:MI:SS.MS' ) as sample_tim
, al.host
, al.wait_event
, al.query_id
from ybx_rore_mst rm
   , ybx_ashy_log al 
   , ybx_sess_mst sm
where 1=1
  and rm.rr_uuid  = al.root_request_id
  and rm.sess_id  = sm.id
  and sm.pid = $1
order by al.sample_time
;


\! echo .
\! echo -- -- -- Events in the pid-session  -- -- -- 
\! echo .


select 
  case when ( rm.rr_uuid = lag(rm.rr_uuid) over ( order by al.sample_time ) ) 
       then null else substr ( rm.rr_uuid::text, 1, 8 ) end  as rr_uuid
-- rm.rr_uuid 
, to_char ( al.sample_time, 'HH24:MI:SS.MS' )      as time_ms
, case al.rpc_request_id when 0 then al.host else ' L '|| al.host end 
|| '     ' ||        substr ( wait_event, 1, 15 )  as event
-- ,        substr ( wait_event_component, 1, 4)      as cmpn
-- , rpad ( substr ( wait_event_type,      1, 4 ), 4) as e_tp
-- ,        substr ( wait_event_class,     1, 4 )     as e_cl
,    tbm.table_name
,    substr ( al.wait_event_aux , 1, 12 )             as ev_aux
,    al.rpc_request_id                                as rpc_r_id
-- , substr ( replace ( qm.query, chr(10), ' ' ), 1, 50 ) as query
-- , al.*
from      ybx_ashy_log al
left join ybx_tblt_mst tbm on substr ( replace ( tbm.tblt_uuid::text, '-', '' ) , 1, 15 ) = al.wait_event_aux
     join ybx_rrqs_mst rm  on rm.rr_uuid = al.root_request_id 
     join ybx_qury_mst qm  on qm.queryid = al.query_id 
where 1=0 -- switch off to save time
and 1=1
and rm.rr_uuid in ( 
  select rr.rr_uuid 
    from ybx_rore_mst rr
       , ybx_sess_mst sm
   where rr.sess_id = sm.id 
     and sm.pid = $1
)
order by al.sample_time ;

select ' p_get.sh: end of first part, wait for 2nd... ' msg ; 

-- generate tree-shaped output, using temp tables and just the records from 1 PID

truncate table ybx_tmp_out; -- tmp-table, blunt approach..

select ybx_evnt_ppid2 ( $1 ) ;

-- show, and spool to file
select output from ybx_tmp_out where ltrim ( output)  != ''  order by id ;

select ybx_get_host()         as your_host
, pg_backend_pid ()           as your_pid
, substr ( version(), 1, 30 ) as your_version ;

\q

EOF 

psql 

echo .
echo Results in $OUTFILE 
echo .


