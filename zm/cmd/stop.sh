#!/bin/sh

CURDIR=$(cd `dirname $0`; pwd)

if [ ! -e $CURDIR/zonemaster.pid ]; then
    echo "ERROR: zonemaster.pid file not exists!!!"
    exit 1
fi

kill -9 `cat $CURDIR/zonemaster.pid`
rm $CURDIR/zonemaster.pid
rm $CURDIR/nohup.out

echo "baudengine zonemaster had stopped."
