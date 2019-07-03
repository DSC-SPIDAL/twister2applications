package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.modifiers.Collector;
import edu.iu.dsc.tws.api.task.nodes.BaseSink;
import edu.iu.dsc.tws.apps.stockanalysis.utils.VectorPoint;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class DataPreprocessingSinkTask extends BaseSink implements Collector {

    private static final Logger LOG = Logger.getLogger(DataPreprocessingSinkTask.class.getName());

    private String vectorDirectory;
    private String distanceDirectory;
    private int distanceType;

    private int lineCount;
    private Map<Integer, VectorPoint> currentPoints;
    private List<VectorPoint> vectors;

    public DataPreprocessingSinkTask(String vectordirectory, String distancedirectory, int distancetype) {
        this.vectorDirectory = vectordirectory;
        this.distanceDirectory = distancedirectory;
        this.distanceType = distancetype;
    }

    @Override
    public boolean execute(IMessage content) {
        currentPoints = (Map<Integer, VectorPoint>) content.getContent();
        lineCount = currentPoints.size();
        LOG.info("Received size and message:" + lineCount + "\t" + currentPoints);
        for (Map.Entry<Integer, VectorPoint> entry : currentPoints.entrySet()) {
            LOG.info("%%%%%%%%%%Entry Values:%%%%%%" + entry);
        }

        DistanceCalculator distanceCalculator = new DistanceCalculator(vectorDirectory, distanceDirectory,
                distanceType);
        distanceCalculator.process();
        /*DistanceCalculator distanceCalculator = new DistanceCalculator(currentPoints, distanceDirectory, distanceType);
        distanceCalculator.process();*/
        return true;
    }

    @Override
    public DataPartition<?> get() {
        return null;
    }

    @Override
    public DataPartition<?> get(String name) {
        return null;
    }

    public void process() {
        List<VectorPoint> secondVectors = vectors;
        int INC = 7000;
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

        int readStartIndex = 0;
        int readEndIndex = INC - 1;

        LOG.info("Reading second block: " + readStartIndex + " : "
                + readEndIndex + " read size: " + secondVectors.size());

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
            }
        }
    }
}