#!/bin/bash

#delete contents of dagster home
rm -rf .dagster_home/*/

# delete directories and contents
rm -rf data/landing/*
rm -rf data/bronze/*
rm -rf data/silver/*
rm -rf data/gold/*

# create directories
mkdir -p data/landing/1001
mkdir -p data/landing/1002
mkdir -p data/landing/1003

# data copy from faker generated files to retailer landing zones
cp faker/data/daily_files/* data/landing/1001/
cp faker/data/single_file_many_dates/* data/landing/1002/
cp faker/data/files_per_store/* data/landing/1003/
