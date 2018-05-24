#!/bin/sh

source ./common.sh

curl -v -d "db_name=mydb2" $GLOBAL_MASTER_ADDR"/manage/db/create"
