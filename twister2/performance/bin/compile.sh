#!/usr/bin/env bash

TWISTER2_HOME=/home/supun/dev/projects/twister2/twister2/bazel-bin/scripts/package/twister2-dist

mvn install

cp bin/run.sh ${TWISTER2_HOME}/bin/