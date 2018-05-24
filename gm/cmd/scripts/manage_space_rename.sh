#!/bin/sh

source common.sh

curl -v  $GLOBAL_MASTER_ADDR"/manage/space/rename?db_name=mydb1&src_space_name=myspace1&dest_space_name=myspace2"
