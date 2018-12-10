package edu.iu.dsc.tws.apps.kmeans;

import edu.iu.dsc.tws.common.config.Config;

import java.util.Arrays;
import java.util.logging.Logger;

public class KMeansFileReader {
    private static final Logger LOG = Logger.getLogger(KMeansFileReader.class.getName());

    private Config config;
    private String fileSystem;

    public KMeansFileReader(Config cfg, String fileSys) {
        this.config = cfg;
        this.fileSystem = fileSys;
    }

    /**
     * It reads the datapoints from the corresponding file system and store the data in a two
     * -dimensional array for the later processing.
     */
    public double[][] readDataPoints(String fName, int dimension) {

        double[][] dataPoints = null;

        if ("local".equals(fileSystem)) {
            KMeansLocalFileReader kMeansLocalFileReader = new KMeansLocalFileReader();
            dataPoints = kMeansLocalFileReader.readDataPoints(fName, dimension);
        } else if ("hdfs".equals(fileSystem)) {
            KMeansHDFSFileReader kMeansHDFSFileReader = new KMeansHDFSFileReader(this.config);
            dataPoints = kMeansHDFSFileReader.readDataPoints(fName, dimension);
        }
        LOG.fine("%%%% Datapoints:" + Arrays.deepToString(dataPoints));
        return dataPoints;
    }

    /**
     * It reads the datapoints from the corresponding file system and store the data in a two
     * -dimensional array for the later processing.
     */
    public double[][] readCentroids(String fileName, int dimension, int numberOfClusters) {

        double[][] centroids = null;

        if ("local".equals(fileSystem)) {
            KMeansLocalFileReader kMeansLocalFileReader = new KMeansLocalFileReader();
            centroids = kMeansLocalFileReader.readCentroids(fileName, dimension, numberOfClusters);
        } else if ("hdfs".equals(fileSystem)) {
            KMeansHDFSFileReader kMeansHDFSFileReader = new KMeansHDFSFileReader(this.config);
            centroids = kMeansHDFSFileReader.readCentroids(fileName, dimension, numberOfClusters);
        }
        LOG.fine("%%%% Centroids:" + Arrays.deepToString(centroids));
        return centroids;
    }
}
