#!/bin/ksh

# cp_all.sh: loop over all nodes with a cp , $1 is sourcefile, $2 is dest-dir (or file)...
#
# todo: HARDcoded nodenames 
#
# typical usage: distribute ybflags..
#

#  verify first, show command

set -v -x

echo .
echo `date` $0 : \[  $* \] ... 
echo .

# do it once, quick...
for node in node2 node3 node4 node5 node6 node7 node8 node9
# for node in node2 node3 node4 node5 node6 
do

  echo doing node $node  
  docker cp $1 $node:/$2

done

echo .
echo copies done.
echo . 

