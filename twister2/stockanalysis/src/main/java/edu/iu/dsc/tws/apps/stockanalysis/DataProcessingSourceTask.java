package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.apps.stockanalysis.utils.CleanMetric;
import edu.iu.dsc.tws.apps.stockanalysis.utils.Record;
import edu.iu.dsc.tws.apps.stockanalysis.utils.Utils;
import edu.iu.dsc.tws.apps.stockanalysis.utils.VectorPoint;
import edu.iu.dsc.tws.task.window.BaseWindowSource;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class DataProcessingSourceTask extends BaseWindowSource {
    private static final Logger LOG = Logger.getLogger(DataPreprocessingSourceTask.class.getName());

    private Map<Integer, VectorPoint> currentPoints = new HashMap<>();
    private Map<String, CleanMetric> metrics = new HashMap<>();
    private TreeMap<String, List<Date>> dates = new TreeMap<>();

    private int vectorCounter = 0;

    private String inputFile;
    private String outputFile;
    private String startDate;

    public DataProcessingSourceTask(String inputfile, String outputfile, String startdate) {
        this.inputFile = inputfile;
        this.outputFile = outputfile;
        this.startDate = startdate;
    }

    @Override
    public void execute() {
        readFile();
        //initiateProcess();
    }

    private void readFile() {
        BufferedReader bufRead = null;
        try {
            FileReader input = new FileReader(inputFile);
            bufRead = new BufferedReader(input);
            Record record;
            while ((record = Utils.parseFile(bufRead, null, false)) != null) {
                LOG.info("Record to write:" + record);
                context.write(Context.TWISTER2_DIRECT_EDGE, record);
            }
            context.end(Context.TWISTER2_DIRECT_EDGE);
        } catch (IOException ioe) {
            throw new RuntimeException("IO Exception Occured" + ioe.getMessage());
        }
    }

    private void initiateProcess() {
        BufferedReader bufRead = null;
        int size = -1;
        int noOfDays = 1;
        int capCount = 0;
        int index = 0;

        outputFile = outputFile + "/" + "output.csv";
        CleanMetric metric = this.metrics.get(outputFile);
        if (metric == null) {
            metric = new CleanMetric();
            this.metrics.put(outputFile, metric);
        }

        Date sDate = Utils.parseDateString(startDate);
        try {
            FileReader input = new FileReader(inputFile);
            bufRead = new BufferedReader(input);

            Record record;
            int count = 0;
            int fullCount = 0;
            double totalCap = 0;
            int splitCount = 0;

            while ((record = Utils.parseFile(bufRead, null, false)) != null) {

                // not a record we are interested in
                //if (!isDateWithing(sDate, record.getDate())) {
                //    continue;
                //}

                LOG.info("start date:" + sDate + "\trecord date:" + record.getDate());
                count++;
                int key = record.getSymbol();
                if (record.getFactorToAdjPrice() > 0) {
                    splitCount++;
                }
                LOG.info("Key Value Is:" + key);
                VectorPoint point = currentPoints.get(key);
                if (point == null) {
                    point = new VectorPoint(key, noOfDays, false);
                    currentPoints.put(key, point);
                }

                if (!point.add(record.getPrice(), record.getFactorToAdjPrice(), record.getFactorToAdjVolume(), metric, index)) {
                    metric.dupRecords++;
                    LOG.info("dup: " + record.serialize());
                }
                point.addCap(record.getVolume() * record.getPrice());
                writeVectors(size, metric);

                if (point.noOfElements() == size) {
                    fullCount++;
                }
                LOG.info("Current Points Values:" + currentPoints);
                //writeVectors(size, metric);
            }
            capCount++;
            metric.stocksWithIncorrectDays = currentPoints.size();
            index++;
        } catch (IOException ioe) {
            throw new RuntimeException("IOException Occured" + ioe.getMessage());
        } finally {
            try {
                if (bufRead != null) {
                    bufRead.close();
                }
            } catch (IOException ioe) {
                throw new RuntimeException("IOException Occured" + ioe.getMessage());
            }
        }
        //context.end(Context.TWISTER2_DIRECT_EDGE);
    }

    private boolean isDateWithing(Date start, Date compare) {
        if (compare == null) {
            System.out.println("Comapre null*****************");
        }
        return compare.equals(start);
    }

    private double writeVectors(int size, CleanMetric metric) {
        double capSum = 0;
        int count = 0;
        LOG.info("Context Value:" + context.taskName() + "\tCurrent Points Size:" + currentPoints.size());
        List<String> vectorPoints = new ArrayList<>();
        for (Iterator<Map.Entry<Integer, VectorPoint>> it = currentPoints.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Integer, VectorPoint> entry = it.next();
            VectorPoint v = entry.getValue();
            //if (v.noOfElements() == size) {
            metric.totalStocks++;

            if (!v.cleanVector(metric)) {
                metric.invalidStocks++;
                it.remove();
                continue;
            }

            String sv = v.serialize();
            // if many points are missing, this can return null
            if (sv != null) {
                capSum += v.getTotalCap();
                count++;

                LOG.info("Serialized Value:" + sv);
                vectorPoints.add(sv);

                // remove it from map
                vectorCounter++;
                metric.writtenStocks++;
            } else {
                metric.invalidStocks++;
            }
            it.remove();
            // } else {
            //     metric.lenghtWrong++;
            // }
        }
        LOG.info("Vector counter value:" + vectorCounter);
        LOG.info("Writing Vector Points Size:" + vectorPoints.size());
        context.write(Context.TWISTER2_DIRECT_EDGE, vectorPoints);
        return capSum;
    }

    private boolean isDateWithing(Date start, Date end, Date compare) {
        if (compare == null) {
            System.out.println("Comapre null*****************");
        }
        return (compare.equals(start) || compare.after(start)) && compare.before(end);
    }
}
