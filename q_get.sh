

ysqlsh -h localhost -p 5433 -U yugabyte -X <<EOF

select query from ybx_qury_mst where queryid = $1 ;

select qm.queryid
, substr ( rm.rr_uuid::text, 1, 8 ) as root_req
, rm.id rr_id
, to_char ( rm.rr_min_dt , 'HH24:MI:SS.MS' ) st_time
, trunc ( extract ( epoch from ( rm.rr_max_dt - rm.rr_min_dt ) ), 3 )   nr_seconds
, count ( *) cnt_ash
from ybx_qury_mst   qm
   , ybx_qurr_lnk   ql
   , ybx_rrqs_mst   rm
   , ybx_ashy_log   al
where qm.queryid  = $1 
  and qm.queryid  = ql.queryid
  and rm.id       = ql.rr_id
  and rm.rr_uuid  = al.root_request_id
  and rm.rr_min_dt > now() - interval '1 hour' 
group by 1, 2, 3, 4, 5
order by  rm.rr_min_dt ; 


EOF

