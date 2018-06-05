#!/bin/sh

source ./common.sh

curl -v -d "db_name=mydb2" $LEADER_ADDR"/manage/db/create"
