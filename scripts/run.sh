#!/bin/bash

set -e

rm -rf kdTree
git clone https://github.com/sclosaf/kdTree
cd kdTree

make
make run
