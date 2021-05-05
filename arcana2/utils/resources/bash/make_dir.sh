#!/usr/bin/env bash
# Zips a directory relative to its parent (i.e. missing out intermediate directories)

base_dir=$1
new_dir=$2

mkdir $1/$2
