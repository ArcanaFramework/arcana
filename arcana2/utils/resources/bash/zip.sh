#!/usr/bin/env bash
# Zips a directory relative to its parent (i.e. missing out intermediate directories)

# Check for relative paths
if [[ $1 = /* ]]; then
  output=$1
else
  output=`pwd`/$1
fi
pushd `dirname $2`
zip -rq $output `basename $2`
popd
