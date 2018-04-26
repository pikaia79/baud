#!/bin/sh

source common.sh

curl -v -d "db_name=mydb1" $LEADER_ADDR"/manage/db/create"
