#!/bin/sh

source common.sh

curl -v $LEADER_ADDR"/manage/space/detail?db_name=mydb1&space_name=myspace1"
