#!/bin/bash
set -e

git tag -d $1 || true
git push --delete origin $1 || true
git tag -a $1 -m "$2"
git push origin $1

