#!/usr/bin/env bash

image="$1"
echo $image

contains() {
     string=$1
     substring=$2
     if test "${string#*$substring}" != "$string"
     then
         mv "$image" "gre_field_map_phase"    # $substring is in $string
     else
         mv "$image" "gre_field_map_mag"    # $substring is not in $string
     fi
 }

echo `mrinfo "$1"`
echo $hd

contains $hd "-4096"
