#!/usr/bin/env bash

APP_DIR=/home/supun/dev/projects/twister2/twister2applications

./bin/twister2 submit nodesmpi jar $APP_DIR/twister2/performance/target/twister2-performance-1.0-SNAPSHOT-jar-with-dependencies.jar edu.iu.dsc.tws.apps.Program 4 10000 100000 1 true "4,4,1" 2>&1 | tee output.txto

