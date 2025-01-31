
-- mk_yblog_d.sql: only the drop...

\! echo .
\! read -t 10 -p "About to drop ASH-tables, use ^C to prevent... " abc
\! echo .


-- dont drop these yet, used by others
-- drop table ybx_log ; 

-- drop table ybx_evnt_mst ;

drop view ybg_tsrv_rwr ; 
drop view ybg_tsrv_cpu ; 
drop view ybx_logg_inf ;
drop view ybx_wait_typ ;

drop view ybx_rrqs_mvw ; 

-- the tables

drop table ybx_ashy_log ;

drop table ybx_qurr_lnk ; 
drop table ybx_rrqs_mst ; 

drop table ybx_tblt_rep ;
drop table ybx_tabl_log ;

drop table ybx_tata_lnk ;

drop table ybx_tblt_mst ;
drop table ybx_tabl_mst ;

drop table ybx_qury_log ;
drop table ybx_sess_log ;

drop table ybx_mast_log ;
drop table ybx_tsrv_log ;
drop table ybx_univ_log ;

-- drop table ybx_datb_log ;
-- drop table ybx_datb_mst ;

drop table ybx_qury_pln ;
drop table ybx_qury_mst ;

drop table ybx_sess_mst ;

drop table ybx_datb_log ;
drop table ybx_datb_mst ;

drop table ybx_tsrv_mst ;
drop table ybx_mast_mst ;

drop table ybx_host_log ;
drop table ybx_host_mst ;

drop table ybx_univ_mst ;

drop table ybx_snap_log ;
-- there is no snap_mst, yet

drop function ybx_get_univ() ;
drop function ybx_get_datb() ;
drop function ybx_get_sess() ;
drop function ybx_get_tblt() ;
drop function ybx_get_ashy() ;

-- drop function ybx_get_tsrv() ;
-- drop function ybx_get_host() ;

\! echo .
\! echo .. dropped, but reminder to check old code...
\! echo .
\! echo mk_yblog_d.sql: dropping... done.
\! echo .


