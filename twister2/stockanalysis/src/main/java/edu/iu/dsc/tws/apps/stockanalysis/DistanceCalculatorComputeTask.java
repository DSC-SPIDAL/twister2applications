package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.nodes.BaseCompute;
import edu.iu.dsc.tws.apps.stockanalysis.utils.Utils;
import edu.iu.dsc.tws.apps.stockanalysis.utils.VectorPoint;
import edu.iu.dsc.tws.apps.stockanalysis.utils.WriterWrapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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

    private static int INC = 7000;
    private int distanceType;

    private List<String> vectorsPoint;
    private Map<Integer, VectorPoint> currentPoints = new HashMap();
    private Map<Integer, String> vectorsMap = new LinkedHashMap<>();

    public DistanceCalculatorComputeTask(String vectorfolder, String distfolder, int distancetype, String edgename) {
        this.vectorFolder = vectorfolder;
        this.distFolder = distfolder;
        this.distanceType = distancetype;
        this.edgeName = edgename;
    }

    @Override
    public boolean execute(IMessage content) {
        if (content.getContent() != null) {
            vectorsMap  = (Map<Integer, String>) content.getContent();
        }
        LOG.info("Vector points size in distance calculator:" + vectorsMap.size());
        for (Iterator<Map.Entry<Integer, String>> it = vectorsMap.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Integer, String> entry = it.next();
            LOG.info("Index:" + entry.getKey() + "\t" + entry.getValue());
        }

        //process();
        context.write(edgeName, "hello");
        return true;
    }

    private class Worker implements Runnable {
        private BlockingQueue<File> queue;

        private Worker(BlockingQueue<File> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            while (!queue.isEmpty()) {
                try {
                    File f = queue.take();
                    processFile(f);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    public void process() {
        LOG.info("Starting Distance calculator..." + vectorFolder);
        File inFolder = new File(vectorFolder);
        if (!inFolder.isDirectory()) {
            LOG.info("In should be a folder: " + vectorFolder);
            return;
        }

        // create the out directory
        Utils.createDirectory(distFolder);
        try {
            BlockingQueue<File> files = new LinkedBlockingQueue<File>();
            List<File> list = new ArrayList<File>();
            Collections.addAll(list, inFolder.listFiles());
            Collections.sort(list);
            files.addAll(list);

            /*BlockingQueue<File> queue = files;
            while (!queue.isEmpty()) {
                try {
                    File f = queue.take();
                    processFile(f);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }*/

            List<Thread> threads = new ArrayList<Thread>();
            // start 4 threads
            for (int i = 0; i < 1; i++) {
                Thread t = new Thread(new Worker(files));
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
            LOG.info("Distance calculator finished...");
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private void processFile(File fileEntry) {
        WriterWrapper writer;
        if (fileEntry.isDirectory()) {
            return;
        }

        String outFileName = distFolder + "/" + fileEntry.getName();
        String smallValDir = distFolder + "/small";
        String smallOutFileName = smallValDir + "/" + fileEntry.getName();

        LOG.info("Calculator vector file: " + fileEntry.getAbsolutePath() + " Output: " + outFileName);
        writer = new WriterWrapper(outFileName, false);
        int lineCount = countLines(fileEntry);

        // initialize the double arrays for this block
        double values[][] = new double[INC][];
        double cachedValues[][] = new double[INC][];
        for (int i = 0; i < values.length; i++) {
            values[i] = new double[lineCount];
            cachedValues[i] = new double[lineCount];
        }

        for (int i = 0; i < cachedValues.length; i++) {
            for (int j = 0; j < cachedValues[i].length; j++) {
                cachedValues[i][j] = -1;
            }
        }
        int[] histogram = new int[100];
        double[] changeHistogram = new double[100];

        double dmax = Double.MIN_VALUE;
        double dmin = Double.MAX_VALUE;

        int startIndex = 0;
        int endIndex = -1;

        List<VectorPoint> vectors;

        startIndex = endIndex + 1;
        endIndex = startIndex + INC - 1;

        int readStartIndex = 0;
        int readEndIndex = INC - 1;

        vectors = Utils.readVectors(fileEntry, startIndex, endIndex);
        LOG.info("Reading Vector Size:" + vectors.size());

        // now start from the beginning and go through the whole file
        List<VectorPoint> secondVectors = vectors;
        LOG.info("Reading second block: " + readStartIndex + " : " + readEndIndex + " read size: " + secondVectors.size());
        for (int i = 0; i < secondVectors.size(); i++) {
            VectorPoint sv = secondVectors.get(i);
            double v = VectorPoint.vectorLength(1, sv);
            for (int z = 0; z < 100; z++) {
                if (v < (z + 1) * .1) {
                    changeHistogram[z]++;
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
                writer.writeShort(shortValue);
            }
            writer.line();
        }

        if (writer != null) {
            writer.close();
        }
        LOG.info("MAX: " + VectorPoint.maxChange + " MIN: " + VectorPoint.minChange);
        LOG.info("Distance history");
        for (int i = 0; i < 100; i++) {
            System.out.print(histogram[i] + ", ");
        }
        System.out.println();

        LOG.info("Ratio history");
        for (int i = 0; i < 100; i++) {
            System.out.print(changeHistogram[i] + ", ");
        }
        System.out.println();
        System.out.println(dmax);
    }

    private int countLines(File file) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file));
            String line;
            int count = 0;
            while ((line = br.readLine()) != null) {
                count++;
            }
            return count;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read file");
        }
    }


    public void process(boolean flag) {
        File inFolder = new File(vectorFolder);
        try {
            BlockingQueue<File> files = new LinkedBlockingQueue<File>();
            List<File> list = new ArrayList<File>();
            Collections.addAll(list, inFolder.listFiles());
            Collections.sort(list);
            files.addAll(list);

            BlockingQueue<File> queue = files;
            while (!queue.isEmpty()) {
                try {
                    File f = queue.take();
                    processFile(f);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            LOG.info("Distance calculator finished...");
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

}
