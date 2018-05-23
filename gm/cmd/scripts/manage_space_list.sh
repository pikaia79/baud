#!/bin/sh

source common.sh

curl -v $GLOBAL_MASTER_ADDR"/manage/space/list?db_name=mydb1"
