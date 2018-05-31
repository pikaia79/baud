#!/bin/sh

CURDIR=`pwd`
echo $CURDIR

source $CURDIR"/build.sh"

nohup $CURDIR"/gm" -c $CURDIR"/gm.toml" > nohup.out 2>&1 &

echo "baudengine gm started. pid:[$!]"

echo $! > gm.pid
