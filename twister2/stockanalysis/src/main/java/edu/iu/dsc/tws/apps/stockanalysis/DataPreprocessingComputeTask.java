package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.nodes.BaseCompute;
import edu.iu.dsc.tws.apps.stockanalysis.utils.CleanMetric;
import edu.iu.dsc.tws.apps.stockanalysis.utils.Record;
import edu.iu.dsc.tws.apps.stockanalysis.utils.Utils;
import edu.iu.dsc.tws.apps.stockanalysis.utils.VectorPoint;

import java.util.*;
import java.util.logging.Logger;

public class DataPreprocessingComputeTask extends BaseCompute {

    private static final Logger LOG = Logger.getLogger(DataPreprocessingComputeTask.class.getName());

    private String vectorDirectory;
    private String distanceDirectory;
    private String edgeName;

    private Date startDate;
    private Date endDate;

    private int windowLength;
    private int slidingLength;
    private int distanceType;
    private int totalCount = 0;
    private int counter = 0;
    private int index = 0;
    private int vectorCounter = 0;

    private int getTotalList = 1;

    private Map<String, CleanMetric> metrics = new HashMap();
    private List<Record> recordList = new ArrayList<>();
    private Map<Integer, VectorPoint> currentPoints = new HashMap();

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
        Record tempRecord;
        if (message.getContent() != null) {
            Record record = (Record) message.getContent();
            counter++;
            if (record.getDate().compareTo(startDate) > 0 && record.getDate().before(endDate)) {
            //if ((record.getDate().equals(startDate) || record.getDate().after(startDate))
            //        && record.getDate().before(endDate)) {
                recordList.add((Record) message.getContent());
                //counter++;
            } else if (record.getDate().after(endDate)) {
                tempRecord = ((Record) message.getContent());
                //counter = 0;
                LOG.info("Before Processing start date:" + startDate + "\t" + "enddate:" + endDate);
                LOG.info("%%%%% Before Processing Record List Size:%%%%%%" + recordList.size());
                startDate = addDate(startDate, slidingLength);
                endDate = addDate(endDate, slidingLength);
                processRecord(recordList);
                LOG.info("After Processing start date:" + startDate + "\t" + "enddate:" + endDate);
                LOG.info("%%%%% After Processing Record List Size:%%%%%%" + recordList.size());
                //recordList.add(tempRecord);
                // send the list to matrix computation
            }
        }
        return true;
    }

    private boolean processRecord(List<Record> recordList) {
        boolean process = process(recordList);
        if (process) {
            removeSlidingList();
        }
        return true;
    }

    private void removeSlidingList() {
        int count = 0;
        while (true) {
            if (recordList.get(0).getDate().compareTo(startDate) < 0) {
                recordList.remove(0);
                count++;
            } else {
                break;
            }
        }
        LOG.info("count of records removed:" + count);
    }

    private boolean process(List<Record> recordList) {
        Map<Date, Integer> dateIntegerMap = new LinkedHashMap<>();
        for (int i = 0; i < recordList.size(); i++) {
            if (!dateIntegerMap.containsKey(recordList.get(i).getDate())) {
                dateIntegerMap.put(recordList.get(i).getDate(), i);
            }
        }
        LOG.fine("Date IntegerMap Size:" + dateIntegerMap.entrySet().size());
        processData(recordList, dateIntegerMap);
        return true;
    }

    private void processData(List<Record> recordList, Map<Date, Integer> dateIntegerMap) {
        LOG.info("I am processing " + getTotalList + "window data segements");
        this.getTotalList++;
        int noOfDays = dateIntegerMap.size();
        int size = -1;
        int splitCount = 0;
        int count = 0;
        int fullCount = 0;
        int capCount = 0;
        double totalCap = 0;

        Map.Entry<Date, Integer> entry = dateIntegerMap.entrySet().iterator().next();
        Date date = entry.getKey();

        String outFileName = "/home/kannan/~stockbench" + "/" + Utils.dateToString(date) + ".csv";
        CleanMetric metric = this.metrics.get(outFileName);
        if (metric == null) {
            metric = new CleanMetric();
            this.metrics.put(outFileName, metric);
        }

        //Vector generation
        for (Record record : recordList) {
            count++;
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
            if (!point.add(record.getPrice(), record.getFactorToAdjPrice(), record.getFactorToAdjVolume(), metric, index)) {
                metric.dupRecords++;
                //LOG.fine("dup: " + record.serialize());
            }
            point.addCap(record.getVolume() * record.getPrice());
            if (point.noOfElements() == size) {
                fullCount++;
            }

            // now write the current vectors, also make sure we have the size determined correctly
            //if (currentPoints.size() > 1000 && size != -1 && fullCount > 750) {
            LOG.fine("Processed: " + count);
            totalCap += writeVectors(noOfDays, metric);
            capCount++;
            fullCount = 0;
            //}
        }
        LOG.info("Split count: " + " = " + splitCount);
        LOG.info("Total stocks: " + currentPoints.size());
        //metric.stocksWithIncorrectDays = currentPoints.size();
        //currentPoints.clear();
    }

    private Map<Integer, String> vectorsMap = new LinkedHashMap<>();

    private double writeVectors(int size, CleanMetric metric) {
        double capSum = 0;
        int count = 0;
        for (Iterator<Map.Entry<Integer, VectorPoint>> it = currentPoints.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Integer, VectorPoint> entry = it.next();
            VectorPoint v = entry.getValue();
            String sv = v.serialize();
            vectorCounter++;
            vectorsMap.put(getTotalList, sv);

            /*if (v.noOfElements() == size) {
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
            }*/
            count++;
        }
        return capSum;
    }

    private void remove(int startindex, int endindex) {
        LOG.info("start index:" + startindex + "\t" + endindex);
        for (int i = 0; i < endindex - startindex; i++) {
            recordList.remove(startindex);
        }
    }

    private List<Date> getSlidingList(Map<Date, Integer> dateIntegerMap) {
        List<Date> slidingList = new LinkedList<>();
        for (Map.Entry<Date, Integer> dateIntegerEntry : dateIntegerMap.entrySet()) {
            Date start = dateIntegerEntry.getKey();
            slidingList.add(start);
            if (slidingList.size() == slidingLength) {
                break;
            }
        }
        return slidingList;
    }

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