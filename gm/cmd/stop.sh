#!/bin/sh

if [ ! -e gm.pid ]; then
    echo "ERROR: gm.pid file not exists!!!"
    exit 1
fi

kill -2 `cat gm.pid`
rm gm.pid
rm gm
rm nohup.out

echo "baudengine gm had stopped."
