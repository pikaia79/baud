#!/bin/sh

CURDIR=`pwd`
echo $CURDIR

source $CURDIR"/build.sh"

nohup $CURDIR"/master" -c $CURDIR"/master.toml" > nohup.out 2>&1 &

echo "baudengine master started. pid:[$!]"

echo $! > master.pid
