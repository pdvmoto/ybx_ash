
drop table ybx_tmp_out ;

create table ybx_tmp_out ( 
  id bigint generated always as identity 
, output text 
);

-- first test a function with table output

-- drop function tto ( the_pid bigint ); 
drop function ybx_evnt_ppid ( the_pid bigint ); 
drop function ybx_evnt_ppid2 ( the_pid bigint ); 
drop function ybx_get_aux_tbl ( aux_txt text ) ; 

create or replace function ybx_get_aux_tbl ( aux_txt text ) 
  returns text
  language plpgsql
AS $$
declare
    tbl_name    text ; 
    output_text text ; 
begin

  select table_name 
    into tbl_name
  from ybx_tblt_mst tm
  where substr ( replace ( tm.tblt_uuid::text, '-', '' ), 1, 15 ) = aux_txt
  limit 1 ; 

  return tbl_name ; 

end $$ ; -- get_aux_tbl


-- -- -- -- --


create or replace function ybx_evnt_ppid2 ( the_pid bigint ) 
  -- returns table ( output_text text ) 
  returns bigint
  language plpgsql
AS $$
declare
    textline    text ; 
    output_text text ; 
    aux_txt     text ; 
    tbl_name    text ; 
    rr_rec      record ; 
    ql_rec      record ; 
    al_rec      record ; 
    rr_cnt      bigint := 0 ;
    ql_cnt      bigint := 0 ;
    al_cnt      bigint := 0 ; 
begin

  for rr_rec in  
      select rm.rr_uuid
           , rm.rr_min_dt, rr_max_dt 
           , to_char ( rm.rr_min_dt, 'HH24:MI:SS.MS  ' ) 
           || rm.rr_uuid::text 
           || ' from pid: ' || the_pid 
           || ' clnt: ' || sm.client_addr || ':' || client_port
           || ' app: ' || sm.app_name
           || ' ' || ' '                   as rr_txt 
      from ybx_rore_mst rm
         , ybx_sess_mst sm 
      where rm.sess_id = sm.id
        and sm.pid = the_pid 
      order by rm.rr_min_dt
  loop

    rr_cnt := rr_cnt + 1 ; 
    output_text := rr_rec.rr_txt ; -- || ' (rr:' || rr_cnt || ')' ; 
    insert into ybx_tmp_out ( output ) values ( output_text ) ; 
    --RETURN NEXT ; 

    --output_text := '.' ; 
    --RETURN NEXT ; 

    ql_cnt := 0 ; 

    -- program loop over queries and ash here..
    for ql_rec in 
      select
              qm.queryid                                              as this_queryid
            , substr ( replace ( qm.query, chr(10), ' '), 1, 60)      as Query
            , to_char ( ql.qurr_start_dt, 'HH24:MI:SS.MS  ' )         as qry_start
            , ql.dur_ms                                               as dur_ms
            , to_char ( ql.qurr_start_dt, 'HH24:MI:SS.MS  ' ) 
            || substr ( rr_rec.rr_uuid::text, 1, 2 ) 
            || '  qry_id: ' || lpad ( ql.queryid::text, 20  ) 
            -- || ' (' || to_char ( ql.dur_ms, '99999' ) || 'ms) '
            || '  qry: ' || substr ( replace ( qm.query , chr(10), ' ' ), 1, 60 )  as ql_out
      from 
           ybx_rrqy_lnk ql
         , ybx_qury_mst qm
      where rr_rec.rr_uuid   = ql.rr_uuid
        and qm.queryid  = ql.queryid
      order by ql.qurr_start_dt
    loop
 
      ql_cnt := ql_cnt + 1 ; 

      output_text := ql_rec.qry_start || substr ( rr_rec.rr_uuid::text, 1, 2 ) ;  -- empty line before next query...
      insert into ybx_tmp_out ( output ) values ( output_text ) ; 

      output_text :=  ql_rec.ql_out ; -- || '          ( rr:'|| rr_cnt || ' ql:'|| ql_cnt ||  ')' ; 
      insert into ybx_tmp_out ( output ) values ( output_text ) ; 
      -- RETURN NEXT ; 
  
      --output_text := ' .' ; 
      output_text := ql_rec.qry_start || substr ( rr_rec.rr_uuid::text, 1, 2 ) ;  -- empty line before ashy details ..
      insert into ybx_tmp_out ( output ) values ( output_text ) ; 
      -- RETURN NEXT ; 

      al_cnt := 0 ; 

      for al_rec in
            select al.sample_time
                 , al.root_request_id
                 , al.host
                 , al.query_id, al.rpc_request_id
                 , al.wait_event
                 , tb.table_name
                 , coalesce ( al.wait_event_aux, ' - ' ) as wait_event_aux 
            from       ybx_ashy_log  al
            left join  ybx_tblt_mst  tb  on substr ( replace ( tb.tblt_uuid::text, '-', '' ), 1, 15 ) = al.wait_event_aux 
            where al.root_request_id = rr_rec.rr_uuid
              and al.query_id        = ql_rec.this_queryid
            order by al.sample_time
      loop

        al_cnt := al_cnt + 1 ; 

        -- pick up table-name if aux maps to table...
        -- note: join would be more efficient.. 
        -- aux_txt := al_rec.wait_event_aux ; 
        -- tbl_name := ybx_get_aux_tbl ( aux_txt ) ; 

        output_text := to_char ( al_rec.sample_time, 'HH24:MI:SS:MS  ' )  
                       || substr ( al_rec.root_request_id::text, 1, 2 ) 
                       || rpad ( case al_rec.rpc_request_id 
                                  when 0 then '  '  || al_rec.host || '      ' || al_rec.wait_event 
                                  else        '    \ ' || al_rec.host || '    \ '|| al_rec.wait_event  
                                  end, 35 ) 
                       -- || '   ' || coalesce ( tbl_name , '             ' ) 
                       || rpad ( case coalesce ( al_rec.table_name, '-' ) when '-' 
                                  then '    aux: ' || al_rec.wait_event_aux
                                  else '    tbl: ' || al_rec.table_name
                                 end, 30 )
                       -- || '  rpc:' || al_rec.rpc_request_id 
                       -- || '  ( rr:'|| rr_cnt || ' ql:' || ql_cnt || ' al:' || al_cnt || ')' 
                        ;

        -- cleanup
        output_text := replace ( output_text, ' aux:  -', ' ' );

        insert into ybx_tmp_out ( output ) values ( output_text ) ; 

      end loop ; -- al list inside rr

      -- output_text := ql_rec.qry_start || substr ( rr_rec.rr_uuid::text, 1, 2 ) 
      --               ||  '    -- -- -- end ash -- -- -- / '  
      --               || '        ( rr:'|| rr_cnt || ' ql:' || ql_cnt || ' al:' || al_cnt || ')'; 
      --  insert into ybx_tmp_out ( output ) values ( output_text )  ;

      /*****
      -- old code with formatting inside SQL, possibley more effective
      -- loop over ashy-records for this rr and this qry
      for al_rec in  
        select  
             to_char ( al.sample_time, 'HH24:MI:SS.MS' )           as qry_start
           , to_char ( al.sample_time, 'HH24:MI:SS.MS  ' )
          || substr ( al.root_request_id::text, 1, 2 ) 
          || rpad ( case when ( al.rpc_request_id == 0 ) then '  ' else '   \ ' end
                  || al.host || ' - ' || al.wait_event, 30 )  
          || '  aux: ' || coalesce ( tbm.table_name, '      ' || al.wait_event_aux)  as ashy_txt
        from      ybx_ashy_log al              -- add tablets later
        left join ybx_tblt_mst tbm on substr ( replace ( tbm.tblt_uuid::text, '-', '' ) , 1, 15 ) = al.wait_event_aux
        where 1=1                 
          and al.root_request_id  = rr_rec.rr_uuid
          -- and al.pid = the_pid    -- the parent-process doent have this rr.. ?? 
          -- and al.query_id         = ql_rec.this_queryid
          and al.sample_time between rr_rec.rr_min_dt and rr_rec.rr_max_dt -- built in safety to prevent double pids
        order by al.sample_time 
      loop

        al_cnt := al_cnt + 1 ; 
        output_text :=  al_rec.ashy_txt || '( rr:'|| rr_cnt || ' ql:' || ql_cnt || ' al:' || al_cnt || ')' ; 
        insert into ybx_tmp_out ( output ) values ( output_text ) ; 
        -- RETURN NEXT ; 

      end loop ; -- over ASH records in qry, in rr
      ***/ 

      output_text := ql_rec.qry_start || substr ( rr_rec.rr_uuid::text, 1, 2 ) 
                    || '   -- -- -- end qry -- -- -- / '  
                    || '        ( rr:'|| rr_cnt || ' ql:' || ql_cnt || ' al:' || al_cnt || ')'; 
      insert into ybx_tmp_out ( output ) values ( output_text ) ; 
      -- RETURN NEXT ; 

    end loop ; -- over Queris in RR, query_link

    output_text := ql_rec.qry_start || substr ( rr_rec.rr_uuid::text, 1, 2 ) 
                    || ' -- -- -- end RR -- -- -- / ' ; 
    insert into ybx_tmp_out ( output ) values ( output_text ) ; 
    -- RETURN NEXT ; 
    output_text := ' .' ; 
    insert into ybx_tmp_out ( output ) values ( output_text ) ; 

  end loop ;  -- over RR

  textline  := ' -- the end -- ' ; 
  output_text := ' -- the end -- ' ; 
  insert into ybx_tmp_out ( output ) values ( output_text ) ; 
  -- RETURN next ; 

  return 0 ; 

end $$ ; 

-- -- -- -- --


create or replace function ybx_evnt_ppid ( the_pid bigint ) 
  -- returns table ( output_text text ) 
  returns bigint
  language plpgsql
AS $$
declare
    textline    text ; 
    output_text text ; 
    aux_txt     text ; 
    tbl_name    text ; 
    rr_rec      record ; 
    ql_rec      record ; 
    al_rec      record ; 
    rr_cnt      bigint := 0 ;
    ql_cnt      bigint := 0 ;
    al_cnt      bigint := 0 ; 
begin

  for rr_rec in  
      select rm.rr_uuid, rm.id
           , rm.rr_min_dt, rr_max_dt 
           , to_char ( rm.rr_min_dt, 'HH24:MI:SS.MS  ' ) 
           || rm.rr_uuid::text 
           || ' from pid: ' || the_pid 
           || ' clnt: ' || sm.client_addr || ':' || client_port
           || ' app: ' || sm.app_name
           || ' ' || ' '                   as rr_txt 
      from ybx_rrqs_mst rm
         , ybx_sess_mst sm 
      where rm.sess_id = sm.id
        and sm.pid = the_pid 
      order by rm.rr_min_dt
  loop

    rr_cnt := rr_cnt + 1 ; 
    output_text := rr_rec.rr_txt ; -- || ' (rr:' || rr_cnt || ')' ; 
    insert into ybx_tmp_out ( output ) values ( output_text ) ; 
    --RETURN NEXT ; 

    --output_text := '.' ; 
    --RETURN NEXT ; 

    ql_cnt := 0 ; 

    -- program loop over queries and ash here..
    for ql_rec in 
      select
              qm.queryid                                              as this_queryid
            , substr ( replace ( qm.query, chr(10), ' '), 1, 60)      as Query
            , to_char ( ql.qurr_start_dt, 'HH24:MI:SS.MS  ' )         as qry_start
            , ql.dur_ms                                               as dur_ms
            , to_char ( ql.qurr_start_dt, 'HH24:MI:SS.MS  ' ) 
            || substr ( rr_rec.rr_uuid::text, 1, 2 ) 
            || '  qry_id: ' || lpad ( ql.queryid::text, 20  ) 
            -- || ' (' || to_char ( ql.dur_ms, '99999' ) || 'ms) '
            || '  qry: ' || substr ( replace ( qm.query , chr(10), ' ' ), 1, 60 )  as ql_out
      from 
           ybx_qurr_lnk ql
        -- , ybx_rrqs_mvw rm
         , ybx_qury_mst qm
      where rr_rec.id   = ql.rr_id
        and qm.queryid  = ql.queryid
      order by ql.qurr_start_dt
    loop
 
      ql_cnt := ql_cnt + 1 ; 

      output_text := ql_rec.qry_start || substr ( rr_rec.rr_uuid::text, 1, 2 ) ;  -- empty line before next query...
      insert into ybx_tmp_out ( output ) values ( output_text ) ; 

      output_text :=  ql_rec.ql_out ; -- || '          ( rr:'|| rr_cnt || ' ql:'|| ql_cnt ||  ')' ; 
      insert into ybx_tmp_out ( output ) values ( output_text ) ; 
      -- RETURN NEXT ; 
  
      --output_text := ' .' ; 
      output_text := ql_rec.qry_start || substr ( rr_rec.rr_uuid::text, 1, 2 ) ;  -- empty line before ashy details ..
      insert into ybx_tmp_out ( output ) values ( output_text ) ; 
      -- RETURN NEXT ; 

      al_cnt := 0 ; 

      for al_rec in
            select al.sample_time
                 , al.root_request_id
                 , al.host
                 , al.query_id, al.rpc_request_id
                 , al.wait_event
                 , tb.table_name
                 , coalesce ( al.wait_event_aux, ' - ' ) as wait_event_aux 
            from       ybx_ashy_log  al
            left join  ybx_tblt_mst  tb  on substr ( replace ( tb.tblt_uuid::text, '-', '' ), 1, 15 ) = al.wait_event_aux 
            where al.root_request_id = rr_rec.rr_uuid
              and al.query_id        = ql_rec.this_queryid
            order by al.sample_time
      loop

        al_cnt := al_cnt + 1 ; 

        -- pick up table-name if aux maps to table...
        -- note: join would be more efficient.. 
        -- aux_txt := al_rec.wait_event_aux ; 
        -- tbl_name := ybx_get_aux_tbl ( aux_txt ) ; 

        output_text := to_char ( al_rec.sample_time, 'HH24:MI:SS:MS  ' )  
                       || substr ( al_rec.root_request_id::text, 1, 2 ) 
                       || rpad ( case al_rec.rpc_request_id 
                                  when 0 then '  '  || al_rec.host || '      ' || al_rec.wait_event 
                                  else        '    \ ' || al_rec.host || '    \ '|| al_rec.wait_event  
                                  end, 35 ) 
                       -- || '   ' || coalesce ( tbl_name , '             ' ) 
                       || rpad ( case coalesce ( al_rec.table_name, '-' ) when '-' 
                                  then '    aux: ' || al_rec.wait_event_aux
                                  else '    tbl: ' || al_rec.table_name
                                 end, 30 )
                       -- || '  rpc:' || al_rec.rpc_request_id 
                       -- || '  ( rr:'|| rr_cnt || ' ql:' || ql_cnt || ' al:' || al_cnt || ')' 
                        ;

        -- cleanup
        output_text := replace ( output_text, ' aux:  -', ' ' );

        insert into ybx_tmp_out ( output ) values ( output_text ) ; 

      end loop ; -- al list inside rr

      -- output_text := ql_rec.qry_start || substr ( rr_rec.rr_uuid::text, 1, 2 ) 
      --               ||  '    -- -- -- end ash -- -- -- / '  
      --               || '        ( rr:'|| rr_cnt || ' ql:' || ql_cnt || ' al:' || al_cnt || ')'; 
      --  insert into ybx_tmp_out ( output ) values ( output_text )  ;

      /*****
      -- old code with formatting inside SQL, possibley more effective
      -- loop over ashy-records for this rr and this qry
      for al_rec in  
        select  
             to_char ( al.sample_time, 'HH24:MI:SS.MS' )           as qry_start
           , to_char ( al.sample_time, 'HH24:MI:SS.MS  ' )
          || substr ( al.root_request_id::text, 1, 2 ) 
          || rpad ( case when ( al.rpc_request_id == 0 ) then '  ' else '   \ ' end
                  || al.host || ' - ' || al.wait_event, 30 )  
          || '  aux: ' || coalesce ( tbm.table_name, '      ' || al.wait_event_aux)  as ashy_txt
        from      ybx_ashy_log al              -- add tablets later
        left join ybx_tblt_mst tbm on substr ( replace ( tbm.tblt_uuid::text, '-', '' ) , 1, 15 ) = al.wait_event_aux
        where 1=1                 
          and al.root_request_id  = rr_rec.rr_uuid
          -- and al.pid = the_pid    -- the parent-process doent have this rr.. ?? 
          -- and al.query_id         = ql_rec.this_queryid
          and al.sample_time between rr_rec.rr_min_dt and rr_rec.rr_max_dt -- built in safety to prevent double pids
        order by al.sample_time 
      loop

        al_cnt := al_cnt + 1 ; 
        output_text :=  al_rec.ashy_txt || '( rr:'|| rr_cnt || ' ql:' || ql_cnt || ' al:' || al_cnt || ')' ; 
        insert into ybx_tmp_out ( output ) values ( output_text ) ; 
        -- RETURN NEXT ; 

      end loop ; -- over ASH records in qry, in rr
      ***/ 

      output_text := ql_rec.qry_start || substr ( rr_rec.rr_uuid::text, 1, 2 ) 
                    || '   -- -- -- end qry -- -- -- / '  
                    || '        ( rr:'|| rr_cnt || ' ql:' || ql_cnt || ' al:' || al_cnt || ')'; 
      insert into ybx_tmp_out ( output ) values ( output_text ) ; 
      -- RETURN NEXT ; 

    end loop ; -- over Queris in RR, query_link

    output_text := ql_rec.qry_start || substr ( rr_rec.rr_uuid::text, 1, 2 ) 
                    || ' -- -- -- end RR -- -- -- / ' ; 
    insert into ybx_tmp_out ( output ) values ( output_text ) ; 
    -- RETURN NEXT ; 
    output_text := ' .' ; 
    insert into ybx_tmp_out ( output ) values ( output_text ) ; 

  end loop ;  -- over RR

  textline  := ' -- the end -- ' ; 
  output_text := ' -- the end -- ' ; 
  insert into ybx_tmp_out ( output ) values ( output_text ) ; 
  -- RETURN next ; 

  return 0 ; 

end $$ ; 


CREATE OR REPLACE FUNCTION list_events_per_pid ( the_pid bigint ) 
RETURNS TABLE (
    output_text TEXT
) 
LANGUAGE plpgsql
AS $$
DECLARE
    rr_rec RECORD;
    qurr_rec RECORD;
    ashy_rec RECORD;
    time_diff INTERVAL;
    outstr    text ;
BEGIN
    -- Loop over records in ybx_rrqs_mst ordered by rr_min_dt
    FOR rr_rec IN
        SELECT rm.sess_id, rm.rr_uuid, rm.rr_min_dt, rm.rr_max_dt
        FROM ybx_rrqs_mst rm
           , ybx_sess_mst sm
        where sm.pid      = the_pid
          and rm.sess_id  = sm.id
        ORDER BY rr_min_dt
    LOOP
        -- Calculate the time difference between rr_min_dt and rr_max_dt
        time_diff := rr_rec.rr_max_dt - rr_rec.rr_min_dt;

        -- Return rrqs_mst record details
        output_text := 'sess: ' || rr_rec.sess_id || ' ' || rr_rec.rr_uuid::text || ' ' || to_char ( rr_rec.rr_max_dt, 'HH24:MI.SS' )   ; 
        -- format(
        --    'Session ID: %, RR UUID: %, RR Min Date: %, RR Max Date: %, Difference (seconds): %',
        --    rr_rec.sess_id, rr_rec.rr_uuid, rr_rec.rr_min_dt, rr_rec.rr_max_dt, EXTRACT(EPOCH FROM time_diff)
        -- );

        RETURN NEXT ; 

        -- Loop over records in ybx_qurr_lnk ordered by qurr_start_dt for the current rr_id
        FOR qurr_rec IN
            SELECT queryid, qurr_start_dt, dur_ms
            FROM ybx_qurr_lnk
            WHERE rr_id = (SELECT id FROM ybx_rrqs_mst WHERE rr_uuid = rr_rec.rr_uuid)
            ORDER BY qurr_start_dt
        LOOP
            -- Return qurr_lnk record details
            output_text :=  format(
                '  Query ID: %, Qurr Start Date: %, Duration (ms): %',
                qurr_rec.queryid, qurr_rec.qurr_start_dt, qurr_rec.dur_ms
            );

            RETURN NEXT ; 

            -- Loop over records in ybx_ashy_log ordered by sample_time for the current queryid
            FOR ashy_rec IN
                SELECT sample_time, host, wait_event, wait_event_aux, rpc_request_id
                FROM ybx_ashy_log
                WHERE query_id = qurr_rec.queryid
                ORDER BY sample_time
            LOOP
                -- Return ashy_log record details
                output_text := format(
                    '    Sample Time: %, Host: %, Wait Event: %, Wait Event Aux: %, RPC Request ID: %',
                    ashy_rec.sample_time, ashy_rec.host, ashy_rec.wait_event, ashy_rec.wait_event_aux, ashy_rec.rpc_request_id
                );
                RETURN NEXT ; 

            END LOOP;
        END LOOP;

        -- Return a blank line when a new ybx_rrqs_mst record is processed
        output_text := '\n';
        RETURN NEXT ;

    END LOOP;
END $$;

