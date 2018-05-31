#!/bin/sh

source ./common.sh

curl -v $GLOBAL_MASTER_ADDR"/manage/db/list"
