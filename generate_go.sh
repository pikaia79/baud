#!/usr/bin/env bash
. ./common.sh

#check_protoc_version

PROGRAM=$(basename "$0")

installed_commands=(protoc-gen-gofast goimports)
for cmd in ${installed_commands[@]}
do
    command -v $cmd >/dev/null 2>&1 || { echo >&2 "I require "$cmd" but it's not installed.  Aborting."; exit 1; }
done

gopath_array=($(go env GOPATH|tr ":" "\n"))

first_gopath=${gopath_array[0]}

if [ -z "$first_gopath" ]; then
    echo "Error : GOPATH does not set!"
    exit 1
fi

O_OUT_M=
GO_INSTALL='go get'

echo "check and install gogoproto code/generator ..."

# link gogo to GOPATH
echo "first GOPATH: " $first_gopath

# install gogo
gogo_protobuf_url=github.com/gogo/protobuf
#GOGO_ROOT=$first_gopath/src/vendor/${gogo_protobuf_url}
GOGO_ROOT=$first_gopath/src/${gogo_protobuf_url}
if [ ! -d $GOGO_ROOT ]; then
    echo "install gogoprotobuf ..."
    ${GO_INSTALL} ${gogo_protobuf_url}/proto
    ${GO_INSTALL} ${gogo_protobuf_url}/protoc-gen-gogo
    ${GO_INSTALL} ${gogo_protobuf_url}/gogoproto
    ${GO_INSTALL} ${gogo_protobuf_url}/protoc-gen-gofast
fi

goimports_url="golang.org/x/tools/cmd/goimports"
if [ ! -d $first_gopath/src/${goimports_url} ]; then
    echo "install goimports ..."
    ${GO_INSTALL} ${goimports_url}
fi

# add the bin path of gogoproto generator into PATH if it's missing
if ! cmd_exists protoc-gen-gofast; then
    gogo_proto_bin="${first_gopath}/bin/protoc-gen-gofast"
    if [ -e "${gogo_proto_bin}" ]; then
        export PATH=$(dirname "${gogo_proto_bin}"):$PATH
        break
    fi
fi

echo "generate go code..."
cd proto
ret=0
for file in `ls *.proto`
do
    base_name=$(basename $file ".proto")"pb"
    mkdir -p $base_name
    protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf --gofast_out=plugins=grpc,$GO_OUT_M:$base_name $file || ret=$?
    cd $base_name
    sed -i.bak -E 's/import _ \"gogoproto\"//g' *.pb.go
    sed -i.bak -E 's/import fmt \"fmt\"//g' *.pb.go
    sed -i.bak -E 's/import io \"io\"//g' *.pb.go
    sed -i.bak -E 's/import math \"math\"//g' *.pb.go
    rm -f *.bak
    goimports -w *.pb.go
    cd ..
done
exit $ret
