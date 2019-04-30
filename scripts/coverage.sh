#!/bin/sh

wget https://github.com/SimonKagstrom/kcov/archive/master.tar.gz
tar xzf master.tar.gz
cd kcov-master
mkdir build
cd build
cmake ..
make
make install DESTDIR=../../kcov-build
cd ../..
rm -rf kcov-master

# Remove binaries
rm -rf target/debug/deps/test*
rm -rf target/debug/deps/nearcore*
rm -rf target/debug/deps/alphanet*
rm -rf target/debug/deps/nearmint*

for file in target/debug/deps/*
do
  if [ -f $file ] && [ -x $file ]; then
    mkdir -p "target/cov/$(basename $file)"
    ./kcov-build/usr/local/bin/kcov --exclude-pattern=/.cargo,/usr/lib --verify "target/cov/$(basename $file)" "$file"
  fi
done

curl -s https://codecov.io/bash | bash
echo "Uploaded code coverage"
