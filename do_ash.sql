/*


beware: needs unames.sh

*/

\timing on


-- set start 
SELECT extract(epoch FROM now())::numeric AS start_ash_dt \gset

select ybx_get_datb () ;

-- select ybx_get_tablog () ;

select ybx_get_tblt () ;

select ybx_get_sess () ;

select ybx_get_qury () ;  

select ybx_get_ashy () ;

select ybx_get_evnt () ; 

-- old but keep

-- select ybx_get_tblts () ;

-- select ybx_get_ash () ;

-- select ybx_get_evlst() ; 


-- separate script for host-log, scraping..

-- set start 
SELECT extract(epoch FROM now())::numeric AS start_hostlog_dt \gset

-- make sure empty
delete from ybx_kvlog where host = ybx_get_host () ; 

-- need to go via file, bcse curl does not seem to work well if "from program"
\! /usr/local/bin/unames.sh > /tmp/abc.out 

COPY ybx_kvlog(key, value)
FROM '/tmp/abc.out'
WITH (FORMAT text, DELIMITER '=', HEADER false, NULL '');

/*** 
-- verify raw data
select * from ybx_kvlog where host = ybx_get_host();

-- verify transfer data
with 
  gh as ( select ybx_get_host() as host)
, pr as ( select value from ybx_kvlog kv, gh where kv.host = gh.host and key = 'nr_processes' )
, mm as ( select value from ybx_kvlog kv, gh where kv.host = gh.host and key = 'master_mem' )
, tm as ( select value from ybx_kvlog kv, gh where kv.host = gh.host and key = 'tserver_mem' )
, du as ( select value from ybx_kvlog kv, gh where kv.host = gh.host and key = 'disk_usage_mb' )
, ti as ( select value from ybx_kvlog kv, gh where kv.host = gh.host and key = 'top_info' )
, lt as ( select count (*) nr_local_tablets from yb_local_tablets )
select gh.host
, pr.value::int		nr_processes
, mm.value::bigint 	master_mem
, tm.value::bigint 	tserver_mem
, du.value::bigint 	disk_usage_mb
, lt.nr_local_tablets
, ti.value 		top_info
from gh gh
, pr pr 
, mm mm
, tm tm
, du du
, lt lt
, ti ti
; 

***/

-- do transfer to log-table 
-- note the intrinsic convesion to some numbers...
with 
  gh as ( select ybx_get_host() as host)
, pr as ( select value from ybx_kvlog kv, gh where kv.host = gh.host and key = 'nr_processes' )
, mm as ( select value from ybx_kvlog kv, gh where kv.host = gh.host and key = 'master_mem' )
, tm as ( select value from ybx_kvlog kv, gh where kv.host = gh.host and key = 'tserver_mem' )
, du as ( select value from ybx_kvlog kv, gh where kv.host = gh.host and key = 'disk_usage_mb' )
, ti as ( select value from ybx_kvlog kv, gh where kv.host = gh.host and key = 'top_info' )
, lt as ( select count (*) nr_local_tablets from yb_local_tablets )
insert into ybx_host_log ( host, nr_processes, master_mem, tserver_mem, disk_usage_mb, nr_local_tablets, top_info )
select gh.host
, pr.value::int 	nr_processes
, mm.value::bigint 	master_mem
, tm.value::bigint 	tserver_mem
, du.value::bigint 	disk_usage_mb
, lt.nr_local_tablets
, ti.value 		      top_info
from gh gh
, pr pr 
, mm mm
, tm tm
, du du
, lt lt
, ti ti
; 

-- select * from ybx_host_log ; 

-- cleanup, it is temporary data after all
delete from ybx_kvlog where host = ybx_get_host() ; 

-- measure time for scraping, if poss..

with d as ( SELECT (extract(epoch FROM now())::numeric - :start_hostlog_dt ) * 1000 AS ela_ms )
insert into ybx_log ( logged_dt, host,            component,     ela_ms,      info_txt )
        select clock_timestamp(), ybx_get_host(), 'get_hostlog', round ( d.ela_ms::numeric, 3)
              , 'do_ash, uname.sh scraped data into ybx_host_log' 
        from d d ;

-- measure total time for do_ash, if poss..

with d as ( SELECT (extract(epoch FROM now())::numeric - :start_ash_dt     ) * 1000  AS ela_ms )
insert into ybx_log ( logged_dt, host,            component,     ela_ms,      info_txt )
        select clock_timestamp(), ybx_get_host(), 'do_ash.sql', round ( d.ela_ms::numeric, 3 ) 
              , 'do_ash, total time...'  
        from d d ;

