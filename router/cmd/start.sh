#!/bin/sh

#source build.sh

CURDIR=$(cd `dirname $0`; pwd)
echo $CURDIR
nohup $CURDIR"/router" -c $CURDIR"/router.toml" > $CURDIR/nohup.out 2>&1 &

echo "baudengine router started. pid:[$!]"

echo $! > $CURDIR/router.pid
