#!/bin/sh

set -e

# Initialize homa module
git submodule init homa
git submodule update --recursive homa

# Switch to branch homa-dev
cd homa
git checkout -b homa-dev
git pull origin homa-dev

# Build homa library
mkdir build
cmake ..
make clean; make -j
