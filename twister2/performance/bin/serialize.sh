#!/usr/bin/env bash

java -cp ../target/twister2-performance-1.0-SNAPSHOT-jar-with-dependencies.jar edu.iu.dsc.tws.apps.SerialTest 100000 1000 2>&1 | tee output.txto
java -cp ../target/twister2-performance-1.0-SNAPSHOT-jar-with-dependencies.jar edu.iu.dsc.tws.apps.SerialTest 100000 2000 2>&1 | tee output.txto
java -cp ../target/twister2-performance-1.0-SNAPSHOT-jar-with-dependencies.jar edu.iu.dsc.tws.apps.SerialTest 100000 4000 2>&1 | tee output.txto
java -cp ../target/twister2-performance-1.0-SNAPSHOT-jar-with-dependencies.jar edu.iu.dsc.tws.apps.SerialTest 100000 8000 2>&1 | tee output.txto
java -cp ../target/twister2-performance-1.0-SNAPSHOT-jar-with-dependencies.jar edu.iu.dsc.tws.apps.SerialTest 100000 16000 2>&1 | tee output.txto
java -cp ../target/twister2-performance-1.0-SNAPSHOT-jar-with-dependencies.jar edu.iu.dsc.tws.apps.SerialTest 100000 32000 2>&1 | tee output.txto
java -cp ../target/twister2-performance-1.0-SNAPSHOT-jar-with-dependencies.jar edu.iu.dsc.tws.apps.SerialTest 100000 64000 2>&1 | tee output.txto
