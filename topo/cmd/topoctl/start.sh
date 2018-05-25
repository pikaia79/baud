#!/bin/sh

CURDIR=`pwd`
echo $CURDIR

#source $CURDIR"/build.sh"

#nohup $CURDIR"/topoctl" -c $CURDIR"/master.toml" > nohup.out 2>&1 &
#
#echo "baudengine master started. pid:[$!]"
#
#echo $! > master.pid
./topoctl -topo_implementation=etcd3 -topo_global_server_addrs=127.0.0.1:9301,127.0.0.1:9302,127.0.0.1:9303 -topo_global_root_dir=/globalroot
