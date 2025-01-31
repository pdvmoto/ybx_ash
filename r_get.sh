#!//bin/bash

# get data from a rr, using like on first part 

psql -h localhost -p 5433 -U yugabyte -X <<EOF

\set ECHO none
\timing off

\! echo .
\! echo -- -- -- The session for the RR -- -- -- 
\! echo .


select sm.id as sess_id, sm.host, sm.app_name
, substr ( sm.client_addr || ':' || sm.client_port , 1, 25 ) as client_info
, to_char ( sm.backend_start, 'DD HH24:MI:SS' ) sess_start
, to_char ( sm.gone_dt      , 'DD HH24:MI:SS' ) backend_end
, trunc ( extract ( epoch from ( sm.gone_dt - sm.backend_start ) ), 3)  as dur_secs
from ybx_rrqs_mvw rm
   , ybx_sess_mst sm
where rm.sess_id = sm.id
  and rm.rr_uuid::text like '$1'|| '%' 
;

\! echo .
\! echo -- -- -- time + duration of the RR into the session. -- -- -- 
\! echo .

-- possibly us \get to pick up sess_id, rr_id, rr_uuid, and sess_start
-- then also report how long into the session this rr started 

-- time + duration of this rr:
select 
  rm.id as rr_id
, rm.host
, rm.rr_uuid
, to_char ( rm.rr_min_dt , 'HH24:MI:SS' ) rr_start
, trunc ( extract ( epoch from ( rm.rr_min_dt - sm.backend_start  ) ), 3)  as secs_into_sess
, trunc ( dur_ms, 3 ) as rr_duration_ms
from ybx_rrqs_mvw rm
   , ybx_sess_mst sm
where rm.sess_id = sm.id
  and rm.rr_uuid::text like '$1'|| '%' 
;

\! echo .
\! echo -- -- -- Queries in the RR -- -- -- 
\! echo .

select qm.queryid
      , substr ( replace ( qm.query, chr(10), ' '), 1, 60)  as Query
from ybx_rrqs_mvw rm
   , ybx_qurr_lnk ql 
   , ybx_qury_mst qm
where rm.id  = ql.rr_id
  and qm.queryid = ql.queryid
  and rm.rr_uuid::text like '$1'|| '%' 
;

\! echo .
\! echo -- -- -- Events in the RR  -- -- -- 
\! echo .

select
  to_char ( al.sample_time, ' - HH24:MI:SS.MS' ) as sample_tim
, al.host
, al.wait_event
, al.query_id
from ybx_rrqs_mvw rm
   , ybx_ashy_log al 
where rm.rr_uuid  = al.root_request_id
  and rm.rr_uuid::text like '$1'|| '%' 
order by al.sample_time
;


EOF

