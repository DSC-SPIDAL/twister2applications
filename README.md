# Twister2Applications
Applications to be developed in the Twister2 framework, Also to manage the same application developed in other frameworks such as MPI

# Compile

Make sure you have OpenMPI 3.1.2 installed and compiled with enabling Java support. 
Twister2 Docs will help you to do this. Next important thing is to extract the twister2-0.1.0.tar.gaz
inside the following directory within Twister2 code base. 

```bash
bazel-bin/scripts/package/

```

Then you need to refer to the ./bin/twister2 file to run your code when you do a submit. 
You can either use an environmental variable or refer to the path when running the examples. 



```bash
    mvn clean install
```

#Examples

## KMeans Communication Based Example

For example

```bash
~/./github/fork/twister2/twister2-0.1.0/bin/twister2 submit standalone jar twister2/kmeans/target/twister2-kmeans-1.0-SNAPSHOT-jar-with-dependencies.jar edu.iu.dsc.tws.apps.kmeans.Program -workers 4 -iter 2 -dim 2 -clusters 4 -pointsFile /home/vibhatha/sandbox/kmeans/input.txt -centersFile /home/vibhatha/sandbox/kmeans/centroids.txt  -points 100 -fname /home/vibhatha/sandbox/kmeans/output.txt -stages 4,4 -col 1 -size 4 -gap 1

```

## Twister2 Communication Benchmark Suite

The module comms-benchmark is designed to benchmark the Twister2 communication package along with the new applications developed on top of it. The main focus is to analyze the system performance after Twister2 0.1.0 release. 

### Developer Guide

1. The RunMain contains the running abstraction for all applications. 
2. In order to specify the running application the following architecture should be followed. 

Param Definitions:

op : operation type in running examples. For example -op "reduce", this will run the reduce example. 
app : denotes that you want run an application developed in Twister2
appName : denotes the application name you want to run , For example : -app -appName terasort. This will run the terasort application. 
keyed : denotes you want to run keyed based examples
comms : denotes you are running communication examples , -keyed -comms -op reduce, this will run the keyedreduce example in comms.
taske : denotes you want to run task execution based examples. -taske -op reduce, this will run the reduce example
stream: denotes that you want to run the stream mode of the application, by default the batch mode applications will be executed.

### Example 

```bash
~/./github/fork/twister2/twister2-0.1.0/bin/twister2 submit standalone jar twister2/comms-benchmark/target/comms-benchmark-1.0.0-SNAPSHOT-jar-with-dependencies.jar edu.iu.dsc.RunMain -itr 200000 -parallelism 4 -size 300 -comms -op "partition" -stages 4,4 -verify 2>&1 | tee out.txt

```
