#!/bin/sh

source common.sh

curl -v $LEADER_ADDR"/manage/db/detail?db_name=mydb1"
