#!/bin/sh

source build.sh

nohup ./master -c ./master.toml > nohup.out 2>&1 &

echo "baud master started. pid:[$!]" 

echo $! > master.pid
