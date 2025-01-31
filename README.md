# ybx_ash

Contain the files needed to deploy ash-logging..

notably..


to create the required objects:

    \i mk_yblog.sql 

this will include _d (drop), the _f (functions) and _out (output-generating functions)


To log data on each node, copy to each node, /usr/bin/local:

  st_ashloop.sh           : to start collecting initiate do_ahsloop.sh
  do_ashloop.sh
  do_ash.sql              : main sql to collect ash-data
  do_snap.sh              : shell script to collect server-side data (host, yb-admin, universes...)
  /tmp/ash_sleep.sh       : to set optional sleeptime (dflt in do_ashloop.sh was 300 sec)
  /tmp/ybx_ash_off.sem    : to disable ashlooop collection

other files
  ash_on.sh
  ash_off.sh              : loop over nodes to switch on/off
  collect_ash.sh          : loop over nodes to do adhoc-collection (before generating output)


to run: 
./do_ashloop.sh >> outfile


suggestions :
 - yb_servers(): to include blacklisted servers ? 
 - yb_universe() to collect universe-data 
 - yb_masters() to collect master-data

