package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.nodes.BaseCompute;
import edu.iu.dsc.tws.apps.stockanalysis.utils.CleanMetric;
import edu.iu.dsc.tws.apps.stockanalysis.utils.Record;
import edu.iu.dsc.tws.apps.stockanalysis.utils.Utils;
import edu.iu.dsc.tws.apps.stockanalysis.utils.VectorPoint;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.*;
import java.util.logging.Logger;

public class DataPreprocessingComputeTask extends BaseCompute {

    private static final Logger LOG = Logger.getLogger(DataPreprocessingComputeTask.class.getName());

    private String vectorDirectory;
    private String distanceDirectory;
    private String edgeName;

    private int windowLength;
    private int slidingLength;
    private int index = 0;
    private int distanceType;

    private Map<String, CleanMetric> metrics = new HashMap();

    private Date startDate;
    private Date endDate;

    private int totalCount = 0;

    private List<Record> recordList = new ArrayList<>();

    public DataPreprocessingComputeTask(String vectordirectory, String distancedirectory,
                                        int distancetype, int windowlength, int slidinglength,
                                        String startDate, String edgename) {
        this.vectorDirectory = vectordirectory;
        this.distanceDirectory = distancedirectory;
        this.distanceType = distancetype;
        this.edgeName = edgename;
        this.windowLength = windowlength;
        this.slidingLength = slidinglength;
        this.startDate = Utils.parseDateString(startDate);
        this.endDate = addYear(this.startDate);
    }

    @Override
    public boolean execute(IMessage message) {
        if (message.getContent() != null) {
            Record record = (Record) message.getContent();
            if (recordList.isEmpty()) {
                recordList.add((Record) message.getContent());
            }
            LOG.info("Before Processing start date:" + startDate + "\t" + "enddate:" + endDate);
            if (record.getDate().after(startDate) && record.getDate().before(endDate)) {
                recordList.add((Record) message.getContent());
            } else if (record.getDate().after(endDate)) {
                LOG.info("%%%%% Record List Size:%%%%%%" + recordList.size());
                process(recordList);
                ++totalCount;
                startDate = addDate(startDate, slidingLength);
                endDate = addDate(endDate, slidingLength);
                LOG.info("After Processing start date:" + startDate + "\t" + "enddate:" + endDate);
                // send the list to matrix computation
            }
        }
        LOG.fine("total count value:" + totalCount);
        return true;
    }

    private boolean process(List<Record> recordList) {

        Map<Date, Integer> dateIntegerMap = new LinkedHashMap<>();
        List<Record> windowRecordList = new LinkedList<>();

        for (int i = 0; i < recordList.size(); i++) {
            if (!dateIntegerMap.containsKey(recordList.get(i).getDate())) {
                dateIntegerMap.put(recordList.get(i).getDate(), i);
            }
        }
        LOG.info("%%% Window Record List:%%%" + recordList.size() + "\t" + dateIntegerMap.size());

        List<Date> slidingList = new LinkedList<>();
        for (Map.Entry<Date, Integer> dateIntegerEntry : dateIntegerMap.entrySet()) {
            Date start = dateIntegerEntry.getKey();
            LOG.info("date list:" + Utils.dateToString(start));
            slidingList.add(start);
            if (slidingList.size() == slidingLength) {
                break;
            }
        }

        for (int i = 0; i < slidingList.size(); i++) {
            Date date = slidingList.get(i);
            LOG.info("sliding date:" + i + "\t" + date);
            for (int j = 0; j < recordList.size(); j++) {
                if (recordList.get(j).getDate().equals(date)) {
                    recordList.remove(j);
                } else {
                    windowRecordList.add(recordList.get(j));
                }
            }
        }
        LOG.info("window record list:" + windowRecordList.size());
        //processData(windowRecordList, dateIntegerMap);
        return true;
    }

    private int vectorCounter = 0;

    private void processData(List<Record> recordList, Map<Date, Integer> dateIntegerMap) {
        BufferedWriter bufWriter = null;
        BufferedReader bufRead = null;
        Map<Integer, VectorPoint> currentPoints = new HashMap();
        int noOfDays = dateIntegerMap.size();
        LOG.info("%%% Window Record List:%%%" + recordList.size() + "\tnumber of days:" + noOfDays);
        int size = -1;
        int splitCount = 0;
        int count = 0;
        int fullCount = 0;
        int capCount = 0;
        double totalCap = 0;

        String outFileName = "/home/kannan/~stockbench/out.csv";
        CleanMetric metric = this.metrics.get(outFileName);
        if (metric == null) {
            metric = new CleanMetric();
            this.metrics.put(outFileName, metric);
        }

        //Vector generation
        for (int i = 0; i < recordList.size(); i++) {
            count++;
            Record record = recordList.get(i);
            int key = record.getSymbol();
            if (record.getFactorToAdjPrice() > 0) {
                splitCount++;
            }
            VectorPoint point = currentPoints.get(key);
            if (point == null) {
                point = new VectorPoint(key, noOfDays, true);
                currentPoints.put(key, point);
            }
            LOG.fine("Received record value is:" + record.getSymbol()
                    + "\trecord date string:" + record.getDateString()
                    + "\tand its date:" + record.getDate()
                    + "\tNumber Of Days:" + noOfDays
                    + "\tvector:" + point);

            // figure out the index
            int index = dateIntegerMap.get(record.getDate());
            LOG.fine("index value is:" + index);
            if (!point.add(record.getPrice(), record.getFactorToAdjPrice(), record.getFactorToAdjVolume(), metric, index)) {
                metric.dupRecords++;
                LOG.info("dup: " + record.serialize());
            }

            point.addCap(record.getVolume() * record.getPrice());

            if (point.noOfElements() == size) {
                fullCount++;
            }

            /*if (currentPoints.size() > 2000 && size == -1) {
                List<Integer> pointSizes = new ArrayList<>();
                for (VectorPoint v : currentPoints.values()) {
                    pointSizes.add(v.noOfElements());
                }
                size = mostCommon(pointSizes);
                LOG.info("Number of stocks per period: " + size);
            }*/

            // now write the current vectors, also make sure we have the size determined correctly
            if (currentPoints.size() > 1000 && size != -1 && fullCount > 750) {
                LOG.info("Processed: " + count);
                //totalCap += writeVectors(noOfDays, metric);
                writeVectors(noOfDays, metric, currentPoints);
                capCount++;
                fullCount = 0;
            }

            LOG.fine("Total stocks: " + vectorCounter + " bad stocks: " + currentPoints.size());
            metric.stocksWithIncorrectDays = currentPoints.size();
            //currentPoints.clear();
        }
    }

    private double writeVectors(int size,
                                CleanMetric metric, Map<Integer, VectorPoint> currentPoints) {
        List<String> vectorsPoint = new LinkedList<>();
        double capSum = 0;
        int count = 0;
        LOG.info("I am entering write vectors method:" + size + "\t" + currentPoints.entrySet());
        for (Iterator<Map.Entry<Integer, VectorPoint>> it = currentPoints.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Integer, VectorPoint> entry = it.next();
            VectorPoint v = entry.getValue();
            if (v.noOfElements() == size) {
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
                    vectorsPoint.add(sv);
                    // remove it from map
                    LOG.info("Serialized value:" + sv);
                    //bufWriter.write(sv);
                    //bufWriter.newLine();
                    vectorCounter++;
                    metric.writtenStocks++;
                } else {
                    metric.invalidStocks++;
                }
                it.remove();
            } else {
                metric.lenghtWrong++;
            }
        }
        //context.write(Context.TWISTER2_DIRECT_EDGE, vectorsPoint);
        return capSum;
    }

   /* private void remove(int startindex, int endindex) {
        LOG.info("start index:" + startindex + "\t" + endindex);
        for (int i = 0; i < endindex - startindex; i++) {
            recordList.remove(startindex);
        }
    }*/

    private boolean isDateWithing(Date start, Date end, Date compare) {
        if (compare == null) {
            System.out.println("Comapre null*****************");
        }
        return (compare.equals(start) || compare.after(start)) && compare.before(end);
    }

    private static Date addYear(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.YEAR, 1);
        return cal.getTime();
    }

    private static Date addDate(Date date, int slidingLength) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, slidingLength);
        return cal.getTime();
    }

    public static <T> T mostCommon(List<T> list) {
        Map<T, Integer> map = new HashMap<T, Integer>();
        for (T t : list) {
            Integer val = map.get(t);
            map.put(t, val == null ? 1 : val + 1);
        }
        Map.Entry<T, Integer> max = null;
        for (Map.Entry<T, Integer> e : map.entrySet()) {
            if (max == null || e.getValue() > max.getValue())
                max = e;
        }
        LOG.info("max key is:" + max.getKey());
        return max.getKey();
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
        super.prepare(cfg, context);
    }
}
