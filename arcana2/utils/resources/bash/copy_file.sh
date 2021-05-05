#!/usr/bin/env bash
# Zips a directory relative to its parent (i.e. missing out intermediate directories)

src=$1
base_dir=$2
name_file=$3

cp $1 $2/$3

