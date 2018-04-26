#!/bin/sh

source common.sh

curl -v -d "db_name=mydb1&space_name=myspace1&partition_key=abc&partition_func=myfunc&partition_num=3" $LEADER_ADDR"/manage/space/create"
