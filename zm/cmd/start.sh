#!/bin/sh

#source build.sh

CURDIR=$(cd `dirname $0`; pwd)
echo $CURDIR
nohup $CURDIR"/zonemaster" -c $CURDIR"/zonemaster.toml" > $CURDIR/nohup.out 2>&1 &

echo "baudengine zonemaster started. pid:[$!]"

echo $! > $CURDIR/zonemaster.pid
