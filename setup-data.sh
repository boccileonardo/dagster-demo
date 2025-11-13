#!/bin/bash

# delete contents of dagster home (including hidden directories)
rm -rf .dagster_home/.*/ .dagster_home/*/

# delete directories and contents (or create if they don't exist)
mkdir -p data/landing data/bronze data/silver data/gold
find data/landing data/bronze data/silver data/gold -mindepth 1 -delete

# create directories
mkdir -p data/landing/1001
mkdir -p data/landing/1002
mkdir -p data/landing/1003

# data copy from faker generated files to retailer landing zones
cp faker/data/daily_files/* data/landing/1001/
cp faker/data/single_file_many_dates/* data/landing/1002/
cp faker/data/files_per_store/* data/landing/1003/
