#!/bin/sh

source common.sh

curl -v $LEADER_ADDR"/manage/space/list?db_name=mydb1"
