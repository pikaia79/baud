#!/bin/sh

CURDIR=$(cd `dirname $0`; pwd)

if [ ! -e $CURDIR/router.pid ]; then
    echo "ERROR: router.pid file not exists!!!"
    exit 1
fi

kill -9 `cat $CURDIR/router.pid`
rm $CURDIR/router.pid
rm $CURDIR/nohup.out

echo "baudengine router had stopped."
