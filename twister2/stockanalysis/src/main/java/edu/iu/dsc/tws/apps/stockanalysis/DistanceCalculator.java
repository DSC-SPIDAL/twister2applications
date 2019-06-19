package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.apps.stockanalysis.utils.Utils;
import edu.iu.dsc.tws.apps.stockanalysis.utils.VectorPoint;
import edu.iu.dsc.tws.apps.stockanalysis.utils.WriterWrapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class DistanceCalculator {

    private static final Logger LOG = Logger.getLogger(DistanceCalculator.class.getName());

    private String vectorFolder;
    private String distFolder;
    private int distanceType;

    public DistanceCalculator(String vectorfolder, String distfolder, int distancetype) {
        this.vectorFolder = vectorfolder;
        this.distFolder = distfolder;
        this.distanceType = distancetype;
    }

    private static int INC = 7000;

    public void process() {
        LOG.info("Starting Distance calculator...");
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

            //TODO: MODIFY THIS PART AND USE THE EXECUTOR
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

    private void processFile(File fileEntry) {
        WriterWrapper writer = null;
        if (fileEntry.isDirectory()) {
            return;
        }

        String outFileName = distFolder + "/" + fileEntry.getName();
        String smallValDir = distFolder + "/small";
        String smallOutFileName = smallValDir + "/" + fileEntry.getName();

        LOG.info("Calculator vector file: " + fileEntry.getAbsolutePath() + " Output: " + outFileName);
        //File smallDirFile = new File(smallValDir);
        //smallDirFile.mkdirs();
        writer = new WriterWrapper(outFileName, false);
        //WriterWrapper smallWriter = new WriterWrapper(smallOutFileName, true);
        // +1 to accomodate constant sctock
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
        double[] chanegHisto = new double[100];

        double dmax = Double.MIN_VALUE;
        double dmin = Double.MAX_VALUE;

        int startIndex = 0;
        int endIndex = -1;

        List<VectorPoint> vectors;

//        do {
        startIndex = endIndex + 1;
        endIndex = startIndex + INC - 1;

        int readStartIndex = 0;
        int readEndIndex = INC - 1;

        vectors = Utils.readVectors(fileEntry, startIndex, endIndex);
//            if (vectors.size() == 0) {
//                break;
//            }

        // System.out.println("Processing block: " + startIndex + " : " + endIndex);
        // now start from the begining and go through the whole file
        List<VectorPoint> secondVectors = vectors;
        LOG.info("Reading second block: " + readStartIndex + " : " + readEndIndex + " read size: " + secondVectors.size());
        for (int i = 0; i < secondVectors.size(); i++) {
            VectorPoint sv = secondVectors.get(i);
            double v = VectorPoint.vectorLength(1, sv);
            for (int z = 0; z < 100; z++) {
                if (v < (z + 1) * .1) {
                    chanegHisto[z]++;
                    break;
                }
            }
            for (int j = 0; j < vectors.size(); j++) {
                VectorPoint fv = vectors.get(j);
                double cor = 0;
                // assume i,j is eqaul to j,i
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
//        } while (true);
        if (writer != null) {
            writer.close();
        }
        LOG.info("MAX: " + VectorPoint.maxChange + " MIN: " + VectorPoint.minChange);
        LOG.info("Distance histo");
        for (int i = 0; i < 100; i++) {
            System.out.print(histogram[i] + ", ");
        }
        System.out.println();

        LOG.info("Ratio histo");
        for (int i = 0; i < 100; i++) {
            System.out.print(chanegHisto[i] + ", ");
        }
        System.out.println();

//        if (smallWriter != null) {
//            smallWriter.close();
//        }
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
}
