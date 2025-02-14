

-- now the functions, start with some simple ones,
-- then do the ASH and qury as the most complicated oneso
-- to start with ASH, we need to disable constrints, 
-- as parent-records may no be there


-- logging of universe: do_snap.sh/sql..
-- logging of host_mst, mast_m/l + tsrv_m/l: do_snap
-- logging of hosts: unames...

-- logging of databases: get_datb
-- logging session: get_sess() : both mst + log
-- logging queries: get_qury() : fow now just mst, 
--    qury_log will be similar to pg_stmt, and activity
-- logging of tablets : 
-- logging ash: get_ashy() : mainly ash, but stil does activity

/* ****

ybx_get_datb(): function to insert databases and logging

logs both datb_mst, if new db, and datb_log tables.

*/

CREATE OR REPLACE FUNCTION ybx_get_datb()
  RETURNS bigint    
  LANGUAGE plpgsql
AS $$
DECLARE
  nr_rec_processed BIGINT         := 0 ;
  n_datb_new    bigint            := 0 ; -- newly found from pg_stat_db
  n_datb_log    bigint            := 0 ; -- log- the numbes from pg_stat_db
  n_datb_upd    bigint            := 0 ; -- had to get data from pg_other view
  this_host     text              := ybx_get_host() ;  -- only get this once..
  retval        bigint            := 0 ;
  start_dt      timestamp         := clock_timestamp(); 
  end_dt        timestamp         := now() ;
  duration_ms   double precision  := 0.0 ;
  cmmnt_txt     text              := 'comment ' ;
BEGIN

  -- RAISE NOTICE 'ybx_get_datb() : starting..' ;

  insert /* get_datb_1_mst */ into ybx_datb_mst (  datid,   datname ) 
  select                    d.datid, d.datname 
  from pg_catalog.pg_stat_database d
  where not exists  ( select 'x' 
                      from ybx_datb_mst m 
                      where d.datid = m.datid ) ; 

  GET DIAGNOSTICS n_datb_new := ROW_COUNT;
  retval := retval + n_datb_new ;

  -- RAISE NOTICE 'ybx_get_datb() nr new : % ', n_datb_new ;

  -- the log-data.. 
  insert /* get_datb_2_log */ into ybx_datb_log ( 
      datid         
    , numbackends  
    , xact_commit 
    , xact_rollback
    , blks_read   
    , blks_hit   
    , tup_returned 
    , tup_fetched 
    , tup_inserted 
    , tup_updated 
    , tup_deleted 
    , conflicts  
    , temp_files 
    , temp_bytes 
    , deadlocks 
    , checksum_failures 
    , checksum_last_failure 
    , blk_read_time        
    , blk_write_time      
    , session_time       
    , active_time       
    , idle_in_transaction_time 
    , sessions              
    , sessions_abandoned   
    , sessions_fatal      
    , sessions_killed    
    , stats_reset )  
  select 
      datid         
    , numbackends  
    , xact_commit 
    , xact_rollback
    , blks_read   
    , blks_hit   
    , tup_returned 
    , tup_fetched 
    , tup_inserted 
    , tup_updated 
    , tup_deleted 
    , conflicts  
    , temp_files 
    , temp_bytes 
    , deadlocks 
    , checksum_failures 
    , checksum_last_failure 
    , blk_read_time        
    , blk_write_time      
    , session_time       
    , active_time       
    , idle_in_transaction_time 
    , sessions              
    , sessions_abandoned   
    , sessions_fatal      
    , sessions_killed    
    , stats_reset 
  from pg_stat_database d ; 
  -- no where clause needed, just log
      
  GET DIAGNOSTICS n_datb_log := ROW_COUNT;
  retval := retval + n_datb_log ;

  -- RAISE NOTICE 'ybx_get_datb() nr logged : % ', n_datb_log ;


  duration_ms := EXTRACT ( MILLISECONDS from ( clock_timestamp() - start_dt ) ) ;

  -- RAISE NOTICE 'ybx_get_datb() elapsed : % ms'     , duration_ms ;

  cmmnt_txt :=  'get_datb: from new: ' || n_datb_new
                    || ', from_log: '  || n_datb_log || '.';
                 -- || ', from upd: '  || n_datb_upd || '.';
  
  insert into ybx_log ( logged_dt, host,       component,     ela_ms,      info_txt )
         select clock_timestamp(), ybx_get_host(), 'ybx_get_datb', duration_ms, cmmnt_txt ;
  
  -- end of fucntion..            
  return retval ;   
  
END; -- ybx_get_datb, to incrementally populate table
$$
; 



-- -- -- -- GET QUERIES -- -- -- --  

/* *****************************************************************

function : ybx_get_qury();

collect SQL from ash + pg_stat_stmnts + pg_stat_activity for current node
and update any empty qry text if possible, with pg_stat as source

todo: qury_log not done yet...!  ybx_pgs_stmt currently loggin stmnts

returns total nr of records

*/

CREATE OR REPLACE FUNCTION ybx_get_qury()
  RETURNS bigint
  LANGUAGE plpgsql
AS $$
DECLARE
  nr_rec_processed BIGINT         := 0 ;
  n_qrys_ash    bigint            := 0 ; -- from ash
  n_qrys_act    bigint            := 0 ; -- from pg_stat_act
  n_qrys_stmt   bigint            := 0 ; -- from pg_stat
  n_qrys_upd    bigint            := 0 ; -- had to get query-text from pg_stat
  n_qrys_log    bigint            := 0 ; -- if stmnt-stats get logged..
  this_host     text              := ybx_get_host() ;  -- only get this once..
  this_tsrv     uuid              := ybx_get_tsrv( this_host ) ;  -- only get this once..
  retval        bigint            := 0 ;
  start_dt      timestamp         := clock_timestamp();
  end_dt        timestamp         := now() ;
  duration_ms   double precision  := 0.0 ;
  cmmnt_txt      text              := 'comment ' ;
BEGIN

  -- RAISE NOTICE 'ybx_get_qury() : starting..' ;

  -- init didnt work...
  this_tsrv := ybx_get_tsrv( this_host ) ; 

  -- note tsrv + host can use dflts, but defaults (function calls) seem slower
  insert /* qury_1 from ash */ 
  into ybx_qury_mst ( queryid, log_tsrv, log_host, log_dt )
    select a.query_id, this_tsrv, this_host, min ( a.sample_time ) 
    from yb_active_session_history a            -- consider select from table after gathering data ?
    where a.sample_time > ( start_dt - make_interval ( secs=>900 ) )
      and not exists ( select 'x' from ybx_qury_mst m where m.queryid = a.query_id ) 
      and a.query_id is not null
    group by a.query_id, this_tsrv, this_host ; 

  GET DIAGNOSTICS n_qrys_ash := ROW_COUNT;
  retval := retval + n_qrys_ash ;

  -- RAISE NOTICE 'ybx_get_qury() from ash : % '     , n_qrys_ash ;

  insert /* qury_2 from act */ into ybx_qury_mst ( queryid, log_tsrv, log_host, log_dt, query )
    select a.query_id, this_tsrv, this_host, min ( coalesce ( a.query_start, clock_timestamp() ) ), min ( a.query)
      from pg_stat_activity a
     where not exists ( select 'x' from ybx_qury_mst m where m.queryid = a.query_id ) 
       and a.query_id is not null
    group by a.query_id, this_tsrv, this_host ;  -- note the min-query : bcse multiple texts can exist?

  GET DIAGNOSTICS n_qrys_act := ROW_COUNT;
  retval := retval + n_qrys_act ;

  -- RAISE NOTICE 'ybx_get_qury() from act : % '     , n_qrys_act ;

  -- consider a merge with 4.. 
  -- use dflt for log_dt
  insert /*qury_3 from stmt */ into ybx_qury_mst ( queryid, query )
    select s.queryid,  min ( s.query ) -- explain appears with same queryid 
      from pg_stat_statements s
     where not exists ( select 'x' from ybx_qury_mst m where m.queryid = s.queryid )  
    group by s.queryid ;  -- note the min-query : bcse multiple texts can exist?

  GET DIAGNOSTICS n_qrys_stmt := ROW_COUNT;
  retval := retval + n_qrys_stmt ;

  -- RAISE NOTICE 'ybx_get_qury() from stmt : % '     , n_qrys_stmt ;

  -- consider a merge.. 
  update /*qury_4_upd */ ybx_qury_mst m
    set query = ( select min ( query ) 
                  from pg_stat_statements s 
                  where m.queryid = s.queryid 
                    and length ( s.query) is not null )
  where coalesce (  ( trim ( m.query)), '' ) = ''  -- pg funny way to detect empty or null..  
    and m.log_dt > now() - interval '1 hour'
  ; 

  GET DIAGNOSTICS n_qrys_upd := ROW_COUNT;
  retval := retval + n_qrys_upd ;

  -- RAISE NOTICE 'ybx_get_qury() from upd  : % '     , n_qrys_upd ;


  -- -- -- now do the QURY_LOG data... 

  -- just copy the contents of pg_stat_statement
  -- we ignore activity we could spot from yb_active_sess_hist for the moment
  -- note: explain causes duplicte queryids... 
  -- use dflts for tsrv_uuid, host  and log_dt
  -- todo: using dflts fog get_host and get_tsrv is not efficient !
  insert /* get_qury_5_logs */ into ybx_qury_log ( 
    queryid,
    tsrv_uuid, 
    userid , 
    dbid , 
    toplevel ,
    plans ,
      total_plan_time ,
        min_plan_time ,
        max_plan_time, 
       mean_plan_time ,
     stddev_plan_time ,
    calls , 
    total_exec_time ,
        min_exec_time  ,
      max_exec_time  ,
     mean_exec_time ,
     stddev_exec_time  ,
    "rows"  ,
    shared_blks_hit  ,
    shared_blks_read  ,
    shared_blks_dirtied  ,
    shared_blks_written  ,
    local_blks_hit  ,
    local_blks_read  ,
    local_blks_dirtied  ,
    local_blks_written ,
    temp_blks_read ,
    temp_blks_written ,
    blk_read_time ,
    blk_write_time ,
      wal_records ,
      wal_fpi ,
      wal_bytes ,
      jit_functions ,
      jit_generation_time ,
      jit_inlining_count ,
      jit_inlining_time ,
      jit_optimization_count ,
      jit_optimization_time ,
      jit_emission_count ,
      jit_emission_time ,
    yb_latency_histogram 
  )
  select 
    queryid,
    this_tsrv,
    userid , 
    dbid , 
    toplevel ,
    plans ,
    total_plan_time ,
    min_plan_time ,
    max_plan_time, 
    mean_plan_time ,
    stddev_plan_time ,
    calls , 
    total_exec_time ,
        min_exec_time  ,
      max_exec_time  ,
     mean_exec_time ,
     stddev_exec_time  ,
    "rows"  ,
    shared_blks_hit  ,
    shared_blks_read  ,
    shared_blks_dirtied  ,
    shared_blks_written  ,
    local_blks_hit  ,
    local_blks_read  ,
    local_blks_dirtied  ,
    local_blks_written ,
    temp_blks_read ,
    temp_blks_written ,
    blk_read_time ,
    blk_write_time ,
      wal_records ,
    wal_fpi ,
    wal_bytes ,
    jit_functions ,
    jit_generation_time ,
    jit_inlining_count ,
    jit_inlining_time ,
    jit_optimization_count ,
    jit_optimization_time ,
    jit_emission_count ,
    jit_emission_time ,
    yb_latency_histogram 
  from pg_stat_statements s
  where 1=1
  and upper ( left ( query, 20 ) ) not like '%EXPLAIN%'    
  and not exists ( select 'x' from ybx_qury_log ol
       where ol.queryid     =   s.queryid
         and ol.tsrv_uuid   =   this_tsrv
         and ol.dbid        = s.dbid
        and ol.userid       = s.userid
        and ol.toplevel     = s.toplevel
        and ol.rows         = s.rows
        and ol.calls        = s.calls
) ; 

  -- and not exists.. prev record:
  --  same sql, same datid, user, same tsrv, same nr_rows, same nr calls.. 
  --  ... date is irrelevant.. as long as not re-strted ? 
  --  less-rows: will get new insert.. 

  -- note: any risk of doubles: avoid explain...

  GET DIAGNOSTICS n_qrys_log := ROW_COUNT;
  retval := retval + n_qrys_log ;
  -- RAISE NOTICE 'ybx_get_qury() logged from pg_stat_stmts : % ' , n_qrys_log ; 

  duration_ms := EXTRACT ( MILLISECONDS from ( clock_timestamp() - start_dt ) ) ;

  -- RAISE NOTICE 'ybx_get_qury() elapsed : % ms'     , duration_ms ;

  cmmnt_txt := 'get_qury, from : ash: '  || n_qrys_ash 
                              || ', act: '  || n_qrys_act 
                              || ', stmt: ' || n_qrys_stmt 
                              || ', upd: '  || n_qrys_upd 
                              || ', log: '  || n_qrys_log || '.';

  insert into ybx_log ( logged_dt, host,       component,     ela_ms,      info_txt )
         select clock_timestamp(), this_host, 'ybx_get_qury', duration_ms, cmmnt_txt ;

  -- end of fucntion..
  return retval ;

END; -- ybx_get_qury, to incrementally populate table
$$
;


-- -- -- -- GET SESS -- -- -- --

/* *****************************************************************

function : ybx_get_sess();

collect ash + pg_stat_stmnts + pg_stat_activity for current node
returns total nr of records

*/

CREATE OR REPLACE FUNCTION ybx_get_sess()
  RETURNS bigint
  LANGUAGE plpgsql
AS $$
DECLARE
  nr_rec_processed BIGINT         := 0 ;
  n_sess_act    bigint            := 0 ; -- from pg_stat_act
  n_sess_ash    bigint            := 0 ; -- from ash
  n_sess_upd    bigint            := 0 ; -- had to get data from pg_other view
  n_sess_log    bigint            := 0 ; -- nr of lines logged+updated.
  this_host     text              := ybx_get_host() ;  -- only get this once..
  this_tsrv     uuid              := ybx_get_tsrv( ybx_get_host() ) ;  -- only get this once..
  retval        bigint            := 0 ;
  start_dt      timestamp         := clock_timestamp();
  end_dt        timestamp         := now() ;
  duration_ms   double precision  := 0.0 ;
  cmmnt_txt      text              := 'comment ' ;
BEGIN

  -- RAISE NOTICE 'ybx_get_sess() : starting..' ;

  -- get from pg_stat_activity., the easiest one bcse usually not too many lines (less than ash)
  -- save log-data for a log-table with time-dependent data
  insert /* get_sess_1 */ into ybx_sess_mst 
        ( tsrv_uuid, host,     pid,         backend_start
       ,  client_addr,         client_port, client_hostname 
       , datid,               usesysid,     leader_pid, app_name, backend_type ) 
  select this_tsrv, this_host, pid, backend_start
       , host ( client_addr) ::text, client_port, client_hostname
       , datid,               usesysid,     leader_pid, application_name, backend_type
    from pg_stat_activity a 
    where not exists ( select 'X' from ybx_sess_mst m 
                        where this_host             = m.host   -- prefer uuid here...
                          and a.pid                 = m.pid
                          and a.backend_start       = m.backend_start 
                          and m.gone_dt         is null -- still open, e.g. session was not terminated 
                          -- sessions detected from ASH may already be gone..
                      ) ;

  GET DIAGNOSTICS n_sess_act := ROW_COUNT;
  retval := retval + n_sess_act ;

  -- RAISE NOTICE 'ybx_get_sess() from act : % '     , n_sess_act ;

  -- get from ash, many more lines...?
  -- should we  only catch those who are "local" 
  -- e.g. top_level_node=000 or top_level_node = local tsrv
  -- => no, a node can see a sess_mst before the originating tsrv found it.. ?
  -- ash can give us : tsrv_uuid (top_node_id), pid (on originating node, 
  -- and min-dt(as tentative backend start), also: datid and client-info
  -- missing is usesysid and real backend_start... 
  -- But... if we fill in min ( sample_time ) for backend-st, 
  -- we need a marker.. we pre-empt the correct data. so that correction IF possible
  -- marker: usesysid unknown => needs correction from pg_stat_activit, IF Found...
  -- also an option: inspect recent sess_mst, and verify 
        -- if exist earlier backend-start, merge/update to earliest backend start, 
        -- remove later sessions with same pid+tsrv
  -- where not exists in mst-table yet..
  -- option: when detecting a new combi if cl_add+port: 
  -- put tsrv+cl_add+port somewhere for later addition?
  -- but investigate via collected data in ash + activity first: 
  -- do any sess get discoverd from ash-only ?
  insert /* get_sess_2 */ into ybx_sess_mst 
        ( tsrv_uuid         , host,      pid,   backend_start
         , client_addr
         , client_port  
         , datid )
  select a.top_level_node_id,  this_host, a.pid , min(a.sample_time)
         , split_part ( client_node_ip, ':', 1 ) as client_addr
         , split_part ( client_node_ip, ':', 2 )::int as client_port
         , a.ysql_dbid
    from yb_active_session_history a 
    where 1=1                                     -- disable if needed, for the moment
      and not exists ( select 'X' from ybx_sess_mst m 
                        where a.top_level_node_id   = m.tsrv_uuid
                          and a.pid                 = m.pid
                          -- no check on backend_start ? should be "recent", but how ?
                          -- and m.gone_dt is null -- e.g. session was not terminated 
                          -- sessions detected from ASH may already be gone..
                      )
     group by a.top_level_node_id, this_host, a.pid, 5, 6, 7 ;

     -- can not limit records by time, as this would lead to double-counts after 900sec 
     --  and a.sample_time >  ( now - make_interval ( secs=> 900  ) )  ;  -- limit nr records..

  GET DIAGNOSTICS n_sess_ash := ROW_COUNT;
  retval := retval + n_sess_ash ;

  -- RAISE NOTICE 'ybx_get_sess() from ash : % ', n_sess_ash ;

  -- now find the closed sessions..
  update /* get_sess_3_upd */ ybx_sess_mst m
  set gone_dt = now()
  where host=ybx_get_host ()
   and gone_dt is null 
  and not exists ( select 's' from pg_stat_activity  a 
                    where ybx_get_host() = m.host
                      and a.pid = m.pid
                      and a.backend_start = m.backend_start ) ; 

  GET DIAGNOSTICS n_sess_upd := ROW_COUNT;
  retval := retval + n_sess_upd ;

  -- RAISE NOTICE 'ybx_get_sess() nr gone : % ', n_sess_upd ;

  duration_ms := EXTRACT ( MILLISECONDS from ( clock_timestamp() - start_dt ) ) ;

  -- for sess_log: just copy whatever is in pg_stat_activity, joint with sess_mst

  -- constants for tsrv+host, dont overuse functions to get tsrv + host: too slow
  -- with /* get_sess_3_act */ 
  --   h as ( select ybx_get_host () as host, now() as smpltm )
  insert /* get_sess_4_log_from_act */ into ybx_sess_log (
    sess_id,
    tsrv_uuid,
    pid,
    host ,
    datid ,
    datname ,
    leader_pid , 
    usesysid ,
    usename ,
    application_name ,
    client_addr ,
    client_hostname ,
    client_port ,
    backend_start ,
    xact_start ,
    query_start ,
    state_change ,
    wait_event_type ,
    wait_event ,
    state ,
    backend_xid ,
    backend_xmin ,
    query_id ,
    query ,
    backend_type ,
    catalog_version ,
    allocated_mem_bytes ,
    rss_mem_bytes ,
    yb_backend_xid 
    )
  select 
    sm.id, 
    this_tsrv, 
    a.pid, 
    this_host, 
    a.datid ,
    a.datname ,
    a.leader_pid , 
    a.usesysid ,
    a.usename ,
    a.application_name ,
    a.client_addr ,
    a.client_hostname ,
    a.client_port ,
    a.backend_start ,
    a.xact_start ,
    a.query_start ,
    a.state_change ,
    a.wait_event_type ,
    a.wait_event ,
    a.state ,
    a.backend_xid ,
    a.backend_xmin ,
    a.query_id ,
    a.query ,
    a.backend_type ,
    a.catalog_version ,
    a.allocated_mem_bytes ,
    a.rss_mem_bytes ,
    a.yb_backend_xid
  from pg_stat_activity a
     , ybx_sess_mst     sm 
  where sm.tsrv_uuid      = this_tsrv
    and sm.pid            = a.pid
    and sm.backend_start  = a.backend_start;
  -- join pg_stat_act with mst to fetch sess_id, 
  -- assume datid etc are all functions of PID 

  GET DIAGNOSTICS n_sess_log := ROW_COUNT;
  retval := retval + n_sess_log ;
  -- RAISE NOTICE 'ybx_get_sess() sess_log : % ' , n_sess_log ; 
    
  duration_ms := EXTRACT ( MILLISECONDS from ( clock_timestamp() - start_dt ) ) ;

  -- RAISE NOTICE 'ybx_get_sess() elapsed : % ms'     , duration_ms ;

  cmmnt_txt := 'get_sess: ash: '     || n_sess_ash
                    || ', act: '     || n_sess_act 
                    || ', closed: '  || n_sess_upd 
                    || ', logged: '  || n_sess_log || '.';

  insert into ybx_log ( logged_dt, host,       component,     ela_ms,      info_txt )
         select clock_timestamp(), ybx_get_host(), 'ybx_get_sess', duration_ms, cmmnt_txt ;
  -- end of fucntion..
  return retval ;   

END; -- ybx_get_sess, to incrementally populate table
$$
; 

/* **************** GET TABLETS ******************

function : ybx_get_tblts();

collect ybx_tblt with local tablets, local to current node
handles both mst (parent needed) and replica (local)

returns total nr of records inserted and updated

todo: how to spot tablet-replicas from nodes that have dissapeared... ? 

*/

CREATE OR REPLACE FUNCTION ybx_get_tblt()
  RETURNS bigint
  LANGUAGE plpgsql
AS $$
DECLARE
  this_host         text ;
  this_tsrv         uuid ;
  nr_rec_processed  bigint            := 0 ;
  n_mst_created     bigint            := 0 ;
  n_rep_created     bigint            := 0 ;
  n_mst_gone        bigint            := 0 ;
  n_rep_gone        bigint            := 0 ;
  n_gone            bigint            := 0 ;
  start_dt          timestamp         := clock_timestamp();
  end_dt            timestamp         := now() ;
  duration_ms       double precision  := 0.0 ;
  retval            bigint            := 0 ;
  cmmnt_txt         text              := ' ' ;
BEGIN

this_host = ybx_get_host ();
this_tsrv = ybx_get_tsrv ( this_host ) ;

-- insert any new-found tablets
-- with /* get_tblt_1 */ 
--   h as ( select ybx_get_host () as host )

insert /* get_tblt_1 */ into ybx_tblt_mst (
  tblt_uuid,
  tabl_uuid ,
  table_type ,
  namespace_name ,
  ysql_schema_name ,
  table_name ,
  partition_key_start ,
  partition_key_end
)
select
  tablet_id::uuid,
  table_id::uuid tabl_uuid ,
  table_type ,
  namespace_name ,
  ysql_schema_name ,
  table_name ,
  partition_key_start ,
  partition_key_end
from yb_local_tablets t
where not exists (
  select 'x' from ybx_tblt_mst m
  where 1=1 
  and   t.tablet_id::uuid   =  m.tblt_uuid
  and   m.gone_dt           is null  --  catch moving + returning tablets 
  ) ;

GET DIAGNOSTICS n_mst_created := ROW_COUNT;
retval := retval + n_mst_created ;

-- RAISE NOTICE 'ybx_get_tblt() mst_created : % tblts' , n_mst_created ; 

-- insert Replicas..this node only
insert /* get_tblt_2 */ into ybx_tblt_rep (
  tblt_uuid    -- tsrv, host, log_dt, role, state, .. all defaut to correct values, check
)
select
  l.tablet_id::uuid
from yb_local_tablets l
where not exists (
  select 'x' from ybx_tblt_rep r
  where r.tsrv_uuid       = this_tsrv
  and   r.tblt_uuid       = l.tablet_id::uuid 
  and   r.gone_dt         is null  --  catch moving + returning tablets 
  ) ;

GET DIAGNOSTICS    n_rep_created := ROW_COUNT;
retval := retval + n_rep_created ;
-- RAISE NOTICE 'ybx_get_tblt() rep_created : % tblts'  , n_rep_created ; 

-- detect gone-replicas
update /* get_tblt_3 */ ybx_tblt_rep r 
  set gone_dt = start_dt 
where 1=1 
and   r.gone_dt    is null                   -- has no end time yet
and   r.tsrv_uuid  = this_tsrv               -- same, local tsrv_uuid 
and not exists (                             -- no more local tblt
  select 'x' from yb_local_tablets l
  where   r.tblt_uuid  =  l.tablet_id::uuid
  )
;

GET DIAGNOSTICS    n_rep_gone := ROW_COUNT;
retval := retval + n_rep_gone ;
-- RAISE NOTICE 'ybx_get_tblt() rep_gone : % tblts'  , n_rep_gone ; 

-- update the gone_date on mst if tablet no longer present in replicas..
-- signal gone_date if ... gone
update /* get_tblt_4 */ ybx_tblt_mst m 
  set gone_dt = start_dt 
where 1=1 
and   m.gone_dt    is null            -- no end time yet
and not exists (                      -- no more open replicas
  select 'x' from ybx_tblt_rep r
  where   m.tblt_uuid  =  r.tblt_uuid
  and     m.gone_dt    is null        -- reps no longer existing...  
  )
;

GET DIAGNOSTICS    n_mst_gone := ROW_COUNT;
retval := retval + n_mst_gone ;
-- RAISE NOTICE 'ybx_get_tblt() mst_gone : % tblts'  , n_mst_gone ; 

duration_ms := EXTRACT ( MILLISECONDS from ( clock_timestamp() - start_dt ) ) ; 

-- RAISE NOTICE 'ybx_get_tblt() elapsed  : % ms'     , duration_ms ; 

cmmnt_txt := 'm_created: ' || n_mst_created 
        || ', r_created: ' || n_rep_created 
        || ', r_gone: '    || n_rep_gone 
        || ', m_gone: '    || n_mst_gone || '.' ;

insert into ybx_log ( logged_dt, host,       component,     ela_ms,      info_txt )
       select clock_timestamp(), ybx_get_host(), 'ybx_get_tblt', duration_ms, cmmnt_txt ; 

  -- end of fucntion..
  return retval ;

END; -- function ybx_get_tblt: to get_tablets
$$
;

/* ******************* ASH ****************************************

function : ybx_get_ashy();

new version, for table ybx_ashy..

collect ash + pg_stat_stmnts + pg_stat_activity for current node
returns total nr of records

*/ 

CREATE OR REPLACE FUNCTION ybx_get_ashy()
  RETURNS bigint
  LANGUAGE plpgsql 
AS $$
DECLARE
  nr_rec_processed BIGINT         := 0 ;
  n_ashrecs     bigint            := 0 ; 
  n_stmnts      bigint            := 0 ; 
  n_actvty      bigint            := 0 ; 
  retval        bigint            := 0 ;
  start_dt      timestamp         := clock_timestamp();
  end_dt        timestamp         := now() ;
  duration_ms   double precision  := 0.0 ;
  this_host      text             := ybx_get_host () ; 
  this_tsrv      uuid                               ;
  cmmnt_txt      text             := 'comment ' ;
BEGIN

this_tsrv := ybx_get_tsrv( this_host ) ; 

-- ash-records, much faster using with clause ?
-- todo: avoid the dflts for host + tsrv_uuid: function calls not efficient
with /* get_ash_1 */ 
  h as ( select this_host as host )
, t as ( select this_tsrv as tsrv_uuid )
-- , l as ( select al.* from ybx_ash al 
--              where al.host = this_host
--                and al.sample_time > (now() - interval '900 sec' ) )
insert into ybx_ashy_log  (
  tsrv_uuid
, host 
, sample_time 
, root_request_id 
, rpc_request_id
, wait_event_component 
, wait_event_class 
, wait_event 
, top_level_node_id 
, query_id 
, ysql_session_id  -- find related info
, pid
, client_node_ip 
, wait_event_aux
, sample_weight 
, wait_event_type 
, ysql_dbid
)
select 
  t.tsrv_uuid
, h.host as host
, a.sample_time  
, a.root_request_id  
, coalesce ( a.rpc_request_id, 0 )  as rpc_id
, a.wait_event_component 
, a.wait_event_class 
, a.wait_event 
, a.top_level_node_id 
, a.query_id 
, 0 -- a.ysql_session_id  -- find related info
, a.pid
, a.client_node_ip 
, a.wait_event_aux
, a.sample_weight 
, a.wait_event_type 
, a.ysql_dbid
from yb_active_session_history a , h h, t t
where not exists ( select 'x' from ybx_ashy_log b 
                   where b.host            = h.host  -- prefer ts-uuid
                   and   b.sample_time     = a.sample_time
                   and   b.root_request_id = a.root_request_id
                   and   b.rpc_request_id  = coalesce ( a.rpc_request_id, 0 )
                   and   b.wait_event      = a.wait_event
                   -- and   b.sample_time > ( start_dt - make_interval ( secs=>900 ) )
                 )
  and not ( a.wait_event = 'Extension' and query_id in ( 5, 7) ) ;

GET DIAGNOSTICS n_ashrecs := ROW_COUNT;
retval := retval + n_ashrecs ;

-- RAISE NOTICE 'ybx_get_ashy() yb_act_sess_hist : % ' , n_ashrecs ; 

duration_ms := EXTRACT ( MILLISECONDS from ( clock_timestamp() - start_dt ) ) ; 

-- RAISE NOTICE 'ybx_get_ashy() elapsed : % ms'     , duration_ms ; 

cmmnt_txt := 'ashy: ' || n_ashrecs || '.'; 

insert into ybx_log ( logged_dt, host,       component,     ela_ms,      info_txt )
       select clock_timestamp(), ybx_get_host(), 'ybx_get_ashy', duration_ms, cmmnt_txt ; 

-- end of fucntion..
return retval ;

END; -- ybx_get_ashy, to incrementally populate table
$$
;


/* *****************************************************************

function : ybx_get_evnt();

collect all possible wait_event names (name + component)
returns total nr of records added

by running this function regularly, we hope to spot all events

*/

CREATE OR REPLACE FUNCTION ybx_get_evnt()
  RETURNS bigint
  LANGUAGE plpgsql
AS $$
DECLARE
  start_dt      timestamp         := clock_timestamp();
  end_dt        timestamp         := now() ;
  hostnm        text              := ybx_get_host() ;
  duration_ms   double precision  := 0.0 ;
  n_evnt_ins    bigint            := 0 ;
  retval        bigint            := 0 ;
  ev_cmmnt_txt  text              := 'Event found ' ;
BEGIN

  ev_cmmnt_txt := 'first found on: ' || hostnm
                         || ', at: ' || start_dt::text ;

  insert /* get_evnt_1_ins */ into ybx_evnt_mst ( 
    wait_event_component,     wait_event_type
  ,     wait_event_class,      wait_event,    wait_event_notes )
  select distinct
    wait_event_component,     wait_event_type
  , wait_event_class ,         wait_event,    ev_cmmnt_txt
  from yb_active_session_history h
  where not exists ( select 'xzy' as xyz from ybx_evnt_mst m
                      where h.wait_event_component = m.wait_event_component
                      and   h.wait_event           = m.wait_event
  );
   
  GET DIAGNOSTICS n_evnt_ins := ROW_COUNT;
  retval := retval + n_evnt_ins ;
  -- RAISE NOTICE 'ybx_get_evnt() inserted : % ' , n_evnt_ins ; 

  duration_ms := EXTRACT ( MILLISECONDS from ( clock_timestamp() - start_dt ) ) ;
  -- RAISE NOTICE 'ybx_get_evnst() elapsed : % ms'     , duration_ms ;

  ev_cmmnt_txt := 'get_evnt: new events found : ' || n_evnt_ins || '.' ;

  insert into ybx_log ( logged_dt, host,       component,            ela_ms,      info_txt )
         select clock_timestamp(), ybx_get_host(), 'ybx_get_evnt', duration_ms,   ev_cmmnt_txt ;

  -- end of fucntion..
  return retval ;

END; -- get_evnt, to incrementally populate table
$$
;


  \! echo .
  \! echo .
  \! read -t 10 -p "10sec to check before testing, or hit enter  " abc
  \! echo .
\! echo .

\set ECHO all 

select ybx_get_datb ();

select * from ybx_datb_mst ; 
select log_dt, datid, numbackends from ybx_datb_log order by log_dt desc limit 3; 

select ybx_get_qury ();
select log_dt, queryid
,  substr ( replace ( query, chr(10), ' '), 1, 50)  as Query
from ybx_qury_mst order by log_dt desc limit 1; 

select ybx_get_sess ();
select app_name, host, pid, backend_start from ybx_sess_mst order by backend_start desc limit 3; 
select sess_id,  log_dt, wait_event       from ybx_sess_log order by backend_start desc limit 3; 

select ybx_get_ashy ();
select host, sample_time, wait_event from ybx_ashy_log order by sample_time desc limit 3; 

select ybx_get_tblt() ;
select tabl_uuid, tblt_uuid, log_dt from ybx_tblt_mst order by log_dt desc limit 4; 
select tsrv_uuid, tblt_uuid, log_dt from ybx_tblt_rep order by log_dt desc limit 4; 

select ybx_get_evnt() ;
select wait_event, log_dt from ybx_evnt_mst order by log_dt desc limit 4; 

\set ECHO none 


