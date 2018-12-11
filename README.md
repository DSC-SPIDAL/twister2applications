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