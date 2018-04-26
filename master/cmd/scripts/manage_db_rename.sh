#!/bin/sh

source common.sh

curl -v  $LEADER_ADDR"/manage/db/rename?src_db_name=mydb1&dest_db_name=mydb2"
