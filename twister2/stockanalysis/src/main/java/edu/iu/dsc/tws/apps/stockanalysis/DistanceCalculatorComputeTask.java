package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
import edu.iu.dsc.tws.api.data.FSDataOutputStream;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.apps.stockanalysis.utils.VectorPoint;
import edu.iu.dsc.tws.apps.stockanalysis.utils.WriterWrapper;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class DistanceCalculatorComputeTask extends BaseCompute {

    private static final long serialVersionUID = -5190777711234234L;

    private static final Logger LOG = Logger.getLogger(DistanceCalculatorComputeTask.class.getName());

    private String vectorFolder;
    private String distFolder;
    private String edgeName;

    private static int INC = 8000;
    private int distanceType;
    private int index = 0;

    private Map<Integer, VectorPoint> currentPoints;

    public DistanceCalculatorComputeTask(String vectorfolder, String distfolder, int distancetype, String edgename) {
        this.vectorFolder = vectorfolder;
        this.distFolder = distfolder;
        this.distanceType = distancetype;
        this.edgeName = edgename;
    }

    @Override
    public boolean execute(IMessage content) {
        LOG.info("Message values:" + content);
        if (content.getContent() != null) {
            currentPoints = new HashMap<>();
            currentPoints = (Map<Integer, VectorPoint>) content.getContent();
            ++index;
        }
        LOG.info("Vector points size in distance calculator:" + currentPoints.size() + "index value:" + index);
        if (currentPoints.size() > 0) {
            processVectors(currentPoints, index);
            //process();
        }
        return true;
    }

    private void processVectors(Map<Integer, VectorPoint> currentPoints, int index) {
        int INC = currentPoints.size();
        WriterWrapper writer = new WriterWrapper("/tmp/distancematrix" + index + ".bin", false);

        // initialize the double arrays for this block
        double values[][] = new double[INC][];
        double cachedValues[][] = new double[INC][];

        LOG.info("Context Task Index:" + context.taskIndex() + "\t" + values.length + "\t" + cachedValues.length);
        for (int i = 0; i < values.length; i++) {
            values[i] = new double[currentPoints.size()];
            cachedValues[i] = new double[currentPoints.size()];
        }

        for (int i = 0; i < cachedValues.length; i++) {
            for (int j = 0; j < cachedValues[i].length; j++) {
                cachedValues[i][j] = -1;
            }
        }

        int[] histogram = new int[100];
        double[] changeHisto = new double[100];

        double dmax = Double.MIN_VALUE;
        double dmin = Double.MAX_VALUE;

        int startIndex;
        int endIndex = -1;

        List<VectorPoint> vectors;

        startIndex = endIndex + 1;
        endIndex = startIndex + INC - 1;

        int readStartIndex = 0;
        int readEndIndex = INC - 1;

        vectors = readVectors(currentPoints, startIndex, endIndex);
        LOG.info("Vectors size:" + vectors.size());

        // now start from the beginning and go through the whole file
        List<VectorPoint> secondVectors = vectors;
        for (int i = 0; i < secondVectors.size(); i++) {
            VectorPoint sv = secondVectors.get(i);
            double v = VectorPoint.vectorLength(1, sv);
            for (int z = 0; z < 100; z++) {
                if (v < (z + 1) * .1) {
                    changeHisto[z]++;
                    break;
                }
            }
            for (int j = 0; j < vectors.size(); j++) {
                VectorPoint fv = vectors.get(j);
                double cor = 0;
                // assume i,j is equal to j,i
                if (cachedValues[readStartIndex + i][j] == -1) {
                    cor = sv.correlation(fv, distanceType);
                } else {
                    cor = cachedValues[readStartIndex + i][j];
                }

                if (cor > dmax) {
                    dmax = cor;
                }

                if (cor < dmin) {
                    dmin = cor;
                }
                values[j][readStartIndex + i] = cor;
                cachedValues[j][readStartIndex + i] = cor;
            }
        }
        readStartIndex = readEndIndex + 1;
        readEndIndex = readStartIndex + INC - 1;
        LOG.info("MAX distance is: " + dmax + " MIN Distance is: " + dmin);

        // write the vectors to file
        List<Short> distanceMatrix = new LinkedList<>();
        for (int i = 0; i < vectors.size(); i++) {
            for (int j = 0; j < values[i].length; j++) {
                double doubleValue = values[i][j] / dmax;
                for (int k = 0; k < 100; k++) {
                    if (doubleValue < (k + 1.0) / 100) {
                        histogram[k]++;
                        break;
                    }
                }
                if (doubleValue < 0) {
                    System.out.println("*********************************ERROR, invalid distance*************************************");
                    throw new RuntimeException("Invalid distance");
                } else if (doubleValue > 1) {
                    System.out.println("*********************************ERROR, invalid distance*************************************");
                    throw new RuntimeException("Invalid distance");
                }
                short shortValue = (short) (doubleValue * Short.MAX_VALUE);
                distanceMatrix.add(shortValue);
                writer.writeShort(shortValue);
            }
            writer.line();
        }
        if (writer != null) {
            writer.close();
        }
        LOG.info("Distance Matrix Size:" + distanceMatrix.size());
        LOG.info("MAX: " + VectorPoint.maxChange + " MIN: " + VectorPoint.minChange);
        context.write(edgeName, distanceMatrix);

        LOG.info("Distance history");
        for (int i = 0; i < histogram.length; i++) {
            System.out.print(histogram[i] + ", ");
        }
        System.out.println();

        LOG.info("Ratio history");
        for (int i = 0; i < changeHisto.length; i++) {
            System.out.print(changeHisto[i] + ", ");
        }
        System.out.println();
        System.out.println(dmax);
    }

    public static List<VectorPoint> readVectors(Map<Integer, VectorPoint> vectorPointMap, int startIndex, int endIndex) {
        List<VectorPoint> vecs = new ArrayList<>();
        String line;
        int count = 0;
        int readCount = 0;
        int globalVectorLength = -1;

        LOG.info("vectorpoint map size:" + vectorPointMap.size() + "\t" + startIndex + "\t" + endIndex);
        for (Iterator<Map.Entry<Integer, VectorPoint>> it = vectorPointMap.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Integer, VectorPoint> entry = it.next();
            VectorPoint v = entry.getValue();
            line = v.serialize();
            if (count >= startIndex) {
                readCount++;
                // process the line.
                String parts[] = line.trim().split(" ");
                if (parts.length > 0 && !(parts.length == 1 && parts[0].equals(""))) {
                    int key = Integer.parseInt(parts[0]);
                    double cap = Double.parseDouble(parts[1]);

                    int vectorLength = parts.length - 2;
                    double[] numbers = new double[vectorLength];
                    for (int i = 2; i < parts.length; i++) {
                        numbers[i - 2] = Double.parseDouble(parts[i]);
                    }
                    VectorPoint p = new VectorPoint(key, numbers);
                    if (key < 10) {
                        p = new VectorPoint(key, globalVectorLength, true);
                        p.setConstantVector(true);
                    } else if (globalVectorLength < 0) {
                        globalVectorLength = vectorLength;
                    }
                    p.addCap(cap);
                    vecs.add(p);
                }
            }
            count++;
            // we stop
            if (readCount > endIndex - startIndex) {
                break;
            }
        }
        LOG.info("Total read count value:" + count);
        return vecs;
    }

    private void process() {
        BlockingQueue<Map<Integer, VectorPoint>> vectorsMap = new LinkedBlockingQueue<>();
        try {
            vectorsMap.put(currentPoints);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        List<Thread> threads = new ArrayList<>();
        // start 4 threads
        for (int i = 0; i < 4; i++) {
            Thread t = new Thread(new Worker(vectorsMap));
            t.start();
            threads.add(t);
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Distance calculator finished...");
    }

    private class Worker implements Runnable {
        private BlockingQueue<Map<Integer, VectorPoint>> queue;

        private Worker(BlockingQueue<Map<Integer, VectorPoint>> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            while (!queue.isEmpty()) {
                try {
                    Map<Integer, VectorPoint> f = queue.take();
                    processVectors(f, 0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    //For testing write into file
    private void removeUnwantedVectorPoints() {
        String directory = "/tmp/vectorfile";
        FSDataOutputStream outputStream;
        try {
            FileSystem fs = FileSystemUtils.get(new Path(directory), config);
            outputStream = fs.create(new Path(directory, generateRandom(10) + ".csv"));
            for (Iterator<Map.Entry<Integer, VectorPoint>> it = currentPoints.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<Integer, VectorPoint> entry = it.next();
                VectorPoint v = entry.getValue();
                String sv = v.serialize();
                PrintWriter pw = new PrintWriter(outputStream);
                pw.print(sv);
                outputStream.sync();
                pw.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String generateRandom(int length) {
        boolean useLetters = true;
        boolean useNumbers = false;
        return RandomStringUtils.random(length, useLetters, useNumbers);
    }
}
