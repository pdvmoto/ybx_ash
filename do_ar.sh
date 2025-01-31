#!/bin/sh
#
# do_ashrep.sh: fill table with 1st and last dates for repoting.
#
# arg1 : start report arg1 seconds ago, dflt 900 sec, or 15min.
# arg2 : duration of interval in seconds, dflt 900 sec..
#
# example: do_ashrep.sh 1800 600 :
#          generates report starting 30min back and covering 10min
#
# fill two timestamps with first and last timestamp to report between
#
#
# todo: find longest running sql from timing of root_req ...
# select a.root_request_id, a.query_id
# , min ( sample_time), max (sample_time )	
# ,  ( max ( sample_time) -  min (sample_time )) as itervr
# ,  extract ( epoch from  max ( sample_time) -  min (sample_time ) ) as sec 
# from ybx_ashy_log a
# where a.root_request_id::text not like '0000%'
# and query_id not between  -100 and 100 
# group by a.root_request_id , a.query_id
# order by sec desc ;
# 
#
#
# set -v -x 

# arg1 defaults to 900 sec or 15min  $1
n_sec_start=900
n_sec_start="${1:-$n_sec_start}"
n_sec_intvl=900
n_sec_intvl="${2:-$n_sec_intvl}"

echo do_ashrep.sh: start $n_sec_start sec ago and take $n_sec_intvl sec interval

# pick up the host name...
export hostnm=`hostname`

ysqlsh -X postgresql://yugabyte@localhost:5433,localhost:5433,localhost:5434?connect_timeout=2 <<EOF


  -- pick dates using parameters, keep dates as epoch-numbers, seconds.. 
  SELECT extract(epoch FROM now())::bigint - $n_sec_start                 AS from_ep \gset
  SELECT extract(epoch FROM now())::bigint - $n_sec_start + $n_sec_intvl  AS to_ep \gset

  -- verify parametetrs
  select  :from_ep as from_ep
        , :to_ep as to_ep 
        , to_timestamp ( :from_ep ) as start_dt
        , to_timestamp ( :to_ep   ) as end_dt ; 

  select to_timestamp ( :from_ep ) as start_dt \gset
  select to_timestamp ( :to_ep   ) as end_dt \gset

  select 'dbg: dates set' ; 

  \timing on

  with intv as  /* q01 show samples from and to */
  ( select  to_timestamp ( :from_ep ) as first_dt
         ,  to_timestamp ( :to_ep   ) as last_dt
  )
  select 'report on interval :' as titl
       , to_char ( i.first_dt, 'YYYY-DD-MM HH24:MI:SS' )  as from_dt 
       , to_char ( i.last_dt , 'YYYY-DD-MM HH24:MI:SS' )  as to_dt
       , count (*)  cnt_samples
  from ybx_ashy_log a, intv i 
  where sample_time between i.first_dt and i.last_dt 
  group by 1, 2, 3;
        

  with intv as  /* q02 crosstab nrs*/
  ( select  to_timestamp ( :from_ep ) as first_dt
         ,  to_timestamp ( :to_ep   ) as last_dt
  )
  select
    to_char ( a.sample_time, 'DDD DY HH24:MI:00') as     dt_hr
  ,            a.host
  , count (*)  smpls_per
  from ybx_ashy_log a, intv i 
  where 1=1
  --and wait_event_component not in ('YCQL')
  and a.sample_time between i.first_dt and i.last_dt
  group by 2, 1     /* host , to_char ( a.sample_time, 'DDD DY HH24:MI:00') */
  order by 1, 2 \crosstabview
  ;

  -- busiest nodes in sample
  with intv as  /* q03 busiest nodes*/
  ( select  to_timestamp ( :from_ep ) as first_dt
         ,  to_timestamp ( :to_ep   ) as last_dt
  )
  select
     count (*)                                             recs_in_intrv
  , to_char ( min (sample_time), 'YYYY-MM-DD HH24:MI:SS' ) oldest_stored
  , to_char ( max (sample_time), 'YYYY-MM-DD HH24:MI:SS' ) latest_stored
  , to_char (  age ( now (), max(sample_time) ), 'ssss' )  secs_ago
  , a.host
  from ybx_ashy_log a, intv i
  where 1=1 
  and a.sample_time between i.first_dt and i.last_dt
  group by a.host
  order by a.host ;

  with intv as  /* q04 busiest components*/
  ( select  to_timestamp ( :from_ep ) as first_dt
         ,  to_timestamp ( :to_ep   ) as last_dt
  )
  select 
          count (*)             cnt
        , a.host
        , wait_event_component  busiest_comp
        , wait_event_class
  --, host
  from ybx_ashy_log a
     , intv i
  where 1=1
  and a.sample_time between i.first_dt and i.last_dt
  group by a.host, a.wait_event_component , a.wait_event_class -- , c.host
  order by 1 desc, 2
  ;

  -- busiest events
  with intv as  /* q05 busiest class, type, event p host */
  ( select  to_timestamp ( :from_ep ) as first_dt
         ,  to_timestamp ( :to_ep   ) as last_dt
  )
  select count (*) cnt
      , wait_event_class
      , wait_event_type
      , wait_event   as   busiest_event
      , ya.host      as   per_host
  from ybx_ashy_log ya
     , intv i
  where 1=1
  and ya.sample_time between i.first_dt and i.last_dt
  group by wait_event_class, wait_event_type, wait_event, host
  order by 1 desc 
  limit 40;

  \! echo .
  \! echo now the busiest tablets per host.
  \! echo .
  with intv as  /* q06 busiest tablets and tables */
  ( select  to_timestamp ( :from_ep ) as first_dt
         ,  to_timestamp ( :to_ep   ) as last_dt
  )
  select count (*)  cnt
      ,             tm.host
      ,             a.wait_event_aux
      ,             tb.ysql_schema_name
      ,             tb.table_name
  from ybx_ashy_log   a
     , ybx_tblt_rep  tr
     , ybx_tblt_mst  tb
     , ybx_tsrv_mst  tm
     , intv      i
  where 1=1
  and   a.sample_time         between i.first_dt and i.last_dt
  and   substr ( replace ( tb.tblt_uuid::text, '-', '' ) , 1, 15 ) 
                              = a.wait_event_aux
  and   tr.tsrv_uuid          = a.tsrv_uuid     -- tr on same tsrv as a-record
  and   tr.tsrv_uuid          = tm.tsrv_uuid     -- find tsrv for host
  and   tr.gone_dt            is null -- only active tablets
  and   a.wait_event_aux      is not null
  -- and wait_event_component not in ('YCQL')
  group by tm.host, a.wait_event_aux, tb.ysql_schema_name, tb.table_name
  order by 1 desc, 2
  limit 20 ;

  \! echo .
  \! echo now the busiest query in the interval, e.g. most logged events.
  \! echo .
  -- find queries, and later: top-root-req, to see if many rreq
  with intv as  /* q10 top qry and nr_rt_req */
  ( select  to_timestamp ( :from_ep ) as first_dt
         ,  to_timestamp ( :to_ep   ) as last_dt
  )
  select count (*) cnt_ashev                           --, min (sample_time) , max(sample_time)
      , count ( distinct ya.root_request_id  )    nr_rreq
      , ya.query_id                               top_qry
      , substr ( replace ( qm.query, chr(10), ' '), 1, 60)  as Query
  from ybx_ashy_log     ya
     , ybx_qury_mst     qm
     , intv             i
  where 1=1
  and   ya.sample_time between i.first_dt and i.last_dt
  and   ya.query_id = qm.queryid
  and   ya.root_request_id::text not like '000%'
  --and   ya.root_request_id::text like 'd1dc9%'
  group by ya.query_id, qm.query
  order by 1 desc
  limit 20;

  -- try looking for qry via id, using saved pgs_stmnt
  with intv as  /* q11 qry per root-req and per qry  */
  ( select  to_timestamp ( :from_ep ) as first_dt
         ,  to_timestamp ( :to_ep   ) as last_dt
  )
  select count (*)   as cnt --, min (sample_time) , max(sample_time)
      , substr ( ya.root_request_id::text, 1, 9)                      as top_root_req
      , qm.queryid                                                    as top_qry
      , max ( substr ( replace ( qm.query, chr(10), ' ' ), 1, 60)  )  as Query
  from ybx_ashy_log     ya
     , ybx_qury_mst     qm
     , intv             i
  where 1=1
  and   ya.sample_time between i.first_dt and i.last_dt
  and   qm.queryid                 =     ya.query_id
  and   ya.root_request_id::text  not   like '000%'
  group by ya.root_request_id , qm.queryid
  order by 1 desc
  limit 20;

  -- try looking for qry and PID
  with intv as  /* q11 qry per root-req and per qry  */
  ( select  to_timestamp ( :from_ep ) as first_dt
         ,  to_timestamp ( :to_ep   ) as last_dt
  )
  select count (*) cnt
      , ya.pid                                                        as    top_pid
      , qm.queryid                                                   as    top_qry
      , max ( substr ( replace ( qm.query, chr(10), ' ' ), 1, 60)  )    as    Query
  from ybx_ashy_log      ya
     , ybx_qury_mst      qm
     , intv              i
  where 1=1
  and   ya.sample_time between i.first_dt and i.last_dt
  and   qm.queryid                 =     ya.query_id
  and   ya.root_request_id::text  not   like '000%'
  group by ya.pid, qm.queryid
  order by 1 desc
  limit 20;

  -- slowest run  + count of each query
  with intv as  /* q02 final xtab nrs*/
  ( select  to_timestamp ( :from_ep ) as first_dt
         ,  to_timestamp ( :to_ep   ) as last_dt
  )
  select /* a.root_request_id, */       qm.queryid
  --, min ( sample_time), max (sample_time )	
  --,  ( max ( sample_time) -  min (sample_time )) as itervr
  ,  trunc ( extract ( epoch from  max ( sample_time) -  min (sample_time ) ) ) as max_sec 
  , count (*) nr_occ
  -- , qm.query
  , substr ( replace ( qm.query, chr(10), ' ' ), 1, 60)  as    Query
  from ybx_ashy_log   al
     , ybx_qury_mst   qm 
     , intv           i
  where 1=1
  and al.sample_time      between i.first_dt and i.last_dt
  and qm.queryid          = al.query_id
  and al.root_request_id::text not like '0000%'
  and qm.queryid          not between  -100 and 100 
  group by /* al.root_request_id , */ qm.queryid, qm.query
  having extract ( epoch from  max ( sample_time) -  min (sample_time ) )  > 1
  order by max_sec desc ;

  \! echo .
  \! echo once more crosstab per host 
  \! echo .
  with intv as  /* q02 final xtab nrs*/
  ( select  to_timestamp ( :from_ep ) as first_dt
         ,  to_timestamp ( :to_ep   ) as last_dt
  )
  select
    to_char ( a.sample_time, 'DDD DY HH24:MI:00') as     dt_hr
  ,            a.host
  , count (*)  smpls_per
  from ybx_ashy_log  a, intv i 
  where 1=1
  --and wait_event_component not in ('YCQL')
  and a.sample_time between i.first_dt and i.last_dt
  group by 2, 1     /* host , to_char ( a.sample_time, 'DDD DY HH24:MI:00') */
  order by 1, 2 \crosstabview
  ;

  -- cleanup
  -- delete from ybx_ash_rep ; 

EOF



