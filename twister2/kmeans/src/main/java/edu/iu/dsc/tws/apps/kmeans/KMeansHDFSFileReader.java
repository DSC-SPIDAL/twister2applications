package edu.iu.dsc.tws.apps.kmeans;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.hdfs.HadoopFileSystem;
import edu.iu.dsc.tws.data.utils.HdfsUtils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class KMeansHDFSFileReader {
    private Config config;
    private HdfsUtils hdfsUtils;

    public KMeansHDFSFileReader(Config cfg) {
        this.config = cfg;
    }

    /**
     * It reads the datapoints from the corresponding file and store the data in a two-dimensional
     * array for the later processing.
     */
    public double[][] readDataPoints(String fName, int dimension) {

        hdfsUtils = new HdfsUtils(this.config, fName);
        HadoopFileSystem hadoopFileSystem = hdfsUtils.createHDFSFileSystem();
        Path path = hdfsUtils.getPath();

        int lengthOfFile = hdfsUtils.getLengthOfFile(fName);
        double[][] dataPoints = new double[lengthOfFile][dimension];
        BufferedReader bufferedReader = null;
        try {
            int value = 0;
            String line = "";
            if (hadoopFileSystem.exists(path)) {
                bufferedReader = new BufferedReader(new InputStreamReader(hadoopFileSystem.open(path)));
                while ((line = bufferedReader.readLine()) != null) {
                    String[] data = line.split(",");
                    for (int i = 0; i < dimension; i++) {
                        dataPoints[value][i] = Double.parseDouble(data[i].trim());
                    }
                    value++;
                }
            } else {
                throw new FileNotFoundException("File Not Found In HDFS");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bufferedReader.close();
                hadoopFileSystem.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return dataPoints;
    }

    /**
     * It reads the centroids from the corresponding file and store the data in a two-dimensional
     * array for the later processing. The size of the two-dimensional array should be equal to the
     * number of clusters and the dimension considered for the clustering process.
     */
    public double[][] readCentroids(String fName, int dimension, int numberOfClusters) {

        hdfsUtils = new HdfsUtils(this.config, fName);
        HadoopFileSystem hadoopFileSystem = hdfsUtils.createHDFSFileSystem();
        Path path = hdfsUtils.getPath();

        double[][] centroids = new double[numberOfClusters][dimension];
        BufferedReader bufferedReader = null;
        try {
            int value = 0;
            bufferedReader = new BufferedReader(new InputStreamReader(hadoopFileSystem.open(path)));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] data = line.split(",");
                for (int i = 0; i < dimension - 1; i++) {
                    centroids[value][i] = Double.parseDouble(data[i].trim());
                    centroids[value][i + 1] = Double.parseDouble(data[i + 1].trim());
                }
                value++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bufferedReader.close();
                hadoopFileSystem.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return centroids;
    }
}
