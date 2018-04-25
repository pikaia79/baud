#!/bin/sh

source build.sh

CURDIR=`pwd`
echo $CURDIR
nohup $CURDIR"/master" -c $CURDIR"/master.toml" > nohup.out 2>&1 &

echo "baud master started. pid:[$!]" 

echo $! > master.pid
