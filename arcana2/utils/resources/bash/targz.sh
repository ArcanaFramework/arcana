#!/usr/bin/env bash
# TAR_GZ a directory relative to its parent (i.e. missing out intermediate directories)

cwd=`pwd`
dname=`basename $1`
pushd `dirname $2`
tar -zcvf $cwd/$1 `basename $2`
popd

