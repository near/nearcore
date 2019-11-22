#!/bin/sh

if [[ -z "${GITLAB_CI}" ]]; then
  echo "This script only works in CI"
  exit 1
fi

for file in `find target/debug -mindepth 1 -maxdepth 1 -type f \
  ! -name 'near' \
  ! -name 'near-*' \
  ! -name '*.so' \
  ! -name 'loadtester-*' \
  `
do
  if [ -x $file ]; then
    # codecov script cannot follow symlinks, so place here and mv it to target
    mkdir -p "target2/cov/$(basename $file)"
    kcov --include-pattern=nearcore --verify "target2/cov/$(basename $file)" "$file"
  fi
done

rm target
mv target2 target
curl -s https://codecov.io/bash | bash
echo "Uploaded code coverage"
