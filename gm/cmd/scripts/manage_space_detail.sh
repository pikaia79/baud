#!/bin/sh

source common.sh

curl -v $GLOBAL_MASTER_ADDR"/manage/space/detail?db_name=mydb1&space_name=myspace1"
