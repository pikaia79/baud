#!/bin/sh

expect imagecopy.exp 172.22.212.57 imagecopy.sh 
expect imagecopy.exp 172.22.212.57 imagecopy.exp
expect imagecopy.exp 172.22.212.57 master

expect imagecopy.exp 172.22.212.58 imagecopy.sh 
expect imagecopy.exp 172.22.212.58 imagecopy.exp
expect imagecopy.exp 172.22.212.58 master

