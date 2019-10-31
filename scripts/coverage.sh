#!/bin/sh

rm -rf target/cov

for file in `find target/debug/deps/ \
  ! -name 'test*' \
  ! -name 'near' \
  ! -name 'near-*' \
  ! -name '*.so' \
  ! -name 'loadtester-*' \
  `
do
  if [ -f $file ] && [ -x $file ]; then
    mkdir -p "target/cov/$(basename $file)"
    kcov --include-pattern=nearcore --verify "target/cov/$(basename $file)" "$file"
    break
  fi
done

pwd
ls -l
curl -s https://codecov.io/bash | bash
echo "Uploaded code coverage"
