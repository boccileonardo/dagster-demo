#!/bin/bash

#delete contents of dagster home
rm -rf .dagster_home/*/

# delete directories and contents: /data/landing, /data/bronze, /data/silver, /data/gold
rm -rf data/landing/*
rm -rf data/bronze/*
rm -rf data/silver/*
rm -rf data/gold/*

# create directories: /data/landing/1001, /data/landing/1002
mkdir -p data/landing/1001
mkdir -p data/landing/1002

# copy data from /faker/data/daily_files to /data/landing/1001
cp faker/data/daily_files/* data/landing/1001/

# copy data from /faker/data/single_file_many_dates to /data/landing/1002
cp faker/data/single_file_many_dates/* data/landing/1002/
