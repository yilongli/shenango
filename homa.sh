#!/bin/sh

set -e

# Initialize homa module
git submodule init homa
git submodule update --recursive homa

# Switch to branch homa-dev
cd homa
git fetch --all
git reset --hard origin/shenango

# Build homa library
mkdir build && cd build && cmake .. && make clean; make -j
