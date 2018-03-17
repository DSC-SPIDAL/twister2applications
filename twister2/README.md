Performance Command
-------------------

./bin/twister2 submit nodesmpi jar /home/supun/dev/projects/twister2/twister2applications/twister2/performance/target/twister2-performance-1.0-SNAPSHOT-jar-with-dependencies.jar edu.iu.dsc.tws.apps.Program -con 2 -size 640 -itr 1000 -col 9 -stream -stages "32,32,32" -gap 0 -fname /home/supun/dev/projects/twister2/twister2/bazel-bin/scripts/package/twister2-dist/perf -outstanding 100 -type byte 2>&1 | tee output.txt


K-Means command
---------------



