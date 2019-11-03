#!/bin/sh

for file in `find target/debug/deps/ \
  ! -name 'test*' \
  ! -name 'near' \
  ! -name 'near-*' \
  ! -name '*.so' \
  ! -name 'loadtester-*' \
  `
do
  if [ -f $file ] && [ -x $file ]; then
    rm "$file"
  fi
done