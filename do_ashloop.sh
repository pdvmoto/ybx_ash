#!/bin/bash

# do_ashloop.sh: collect ash and tablet data in loop of 5min (300sec)
#
# usage; 
#   copy to each node and start with nohup.
#   start using st_ashloop.sh or do_stuff.sh 
# 
# verify running with : 
#    tail on logfile
#    select host, min(sample_time) earliest_sample, max(sample_time) latest_sample from ybx_ash group by host order by 3 desc ;
#
# todo:
#   - configur nr seconds as parameter, 120sec seems ok for now, measure durations..
#   - SQL in separate file(s), easier to adjust, loop.sh just does the looping..
#   - use semaphore to stop running: done.
#   - test sleep-pg vs sleep-linux, consume a pg-process, detect sleep-wait ? 
#   - configure for credentials ? 
#

# a bit quick, during benchmarkng, but set to 5 or 10min later
N_SECS=180
F_SEM=/tmp/ybx_ash_off.sem

while true 
do

  if [ -f ${F_SEM} ]; then

    date "+%Y-%m-%dT%H:%M:%S do_ashloop.sh on ${HOSTNAME} : ash Not running, ${F_SEM} found "

  else 

    date "+%Y-%m-%dT%H:%M:%S do_ashloop.sh on ${HOSTNAME} : running ..."
  
    # snapshot should preceed ash, to catch new tsevers
    # but is only needed on 1 node
    /usr/local/bin/do_snap.sh

    ysqlsh -h $HOSTNAME -X <<EOF

      \i /usr/local/bin/do_ash.sql

EOF

  fi
 
  echo .
  date "+%Y-%m-%dT%H:%M:%S do_ashloop.sh on ${HOSTNAME} : sleeping ..."
  echo .

  if [ -f /tmp/ash_sleep.sh ]; then
      sh /tmp/ash_sleep.sh
  else
    echo .
    echo $0 `date` going to sleep for dflt $N_SECS
    sleep $N_SECS
  fi

done

echo do_ashloop.sh: should never get here...

