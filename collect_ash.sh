#!/bin/ksh

# collect_ash.sh: loop over all nodes to colle t ash And generate rrs
#
# typical usage: after a benchmark, to secure ash-data 
#

#  verify first, show command

ASHFILE=do_ash_client.sql
RRFILE=do_ash_client.sql

echo .
echo `date` $0 : \[  $* \] ... 
echo .

nodenrs="2 3 4 5 6 " 

# create nodes, platform, install tools, but no db yet...
for nodenr in $nodenrs
do

  # define all relevant pieces (no spaces!)
  hname=node${nodenr}
  pgport=543${nodenr}
  yb7port=700${nodenr}
  yb9port=900${nodenr}
  yb12p000=1200${nodenr}
  yb13p000=1300${nodenr}
  yb13port=1343${nodenr}
  yb15port=1543${nodenr}

  echo .
  echo `date '+%Y-%m-%d %H:%M:%S'` $0 : ---- $hname  -------
  echo .

  psql -h localhost -p ${pgport} -U yugabyte -X -f $ASHFILE

  # any other command for the node: here..

done


psql -h localhost -p 5433 -X -U yugabyte -f /Users/pdvbv/data/gittest/pg_scripts/mk_rr.sql

echo .
echo `date '+%Y-%m-%d %H:%M:%S'` $0 : \[ $1 \] ... Done -- -- -- -- 
echo . 
