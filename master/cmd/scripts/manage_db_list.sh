#!/bin/sh

source ./common.sh

curl -v $LEADER_ADDR"/manage/db/list"
