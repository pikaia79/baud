#!/bin/sh

if [ ! -e master.pid ]; then
    echo "ERROR: master.pid file not exists!!!"
    exit 1
fi

kill -9 `cat master.pid`

echo "baud master had stopped."
