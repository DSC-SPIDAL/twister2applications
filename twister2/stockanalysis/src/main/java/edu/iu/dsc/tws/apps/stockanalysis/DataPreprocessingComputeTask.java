package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.nodes.BaseCompute;
import edu.iu.dsc.tws.apps.stockanalysis.utils.CleanMetric;
import edu.iu.dsc.tws.apps.stockanalysis.utils.Record;
import edu.iu.dsc.tws.apps.stockanalysis.utils.VectorPoint;

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

    private List<Record> recordList = new ArrayList<>();
    private List<Record> windowRecordList;

    private Map<String, CleanMetric> metrics = new HashMap();
    private Map<Integer, VectorPoint> currentPoints = new HashMap();

    private Date startDate;
    private Date endDate;
    private int numberOfDays;

    public DataPreprocessingComputeTask(String vectordirectory, String distancedirectory, int distancetype,
                                        int windowlength, int slidinglength, String edgename) {
        this.vectorDirectory = vectordirectory;
        this.distanceDirectory = distancedirectory;
        this.distanceType = distancetype;
        this.edgeName = edgename;
        this.windowLength = windowlength;
        this.slidingLength = slidinglength;
    }

    @Override
    public boolean execute(IMessage message) {
        int startIndex = 0;
        Record recordMessage;
        if (message.getContent() != null) {
            if (startIndex == 0) {
                recordMessage = (Record) message.getContent();
                recordList.add(recordMessage);
                startDate = recordList.get(0).getDate();
                endDate = addYear(startDate);
                startIndex++;
            } else if (startIndex >= 1) {
                recordMessage = (Record) message.getContent();
                //if (recordMessage.getDate().after(startDate) && recordMessage.getDate().before(endDate)) {
                recordList.add(recordMessage);
                //}
            }
        }
        LOG.info("record list size:" + recordList.size());
        if (recordList.size() > windowLength) {
            process(recordList);
        }
        return true;
    }

    private void process(List<Record> recordList) {
        Map<Date, Integer> dateIntegerMap = null;
        List<Record> windowRecordList = new ArrayList<>();
        for (int i = 0; i < recordList.size(); i++) {
            if (recordList.get(i).getDate().after(startDate) && recordList.get(i).getDate().before(endDate)) {
                windowRecordList.add(recordList.get(i));
                dateIntegerMap = new HashMap<>();
                if (!dateIntegerMap.containsKey(recordList.get(i).getDate())) {
                    dateIntegerMap.put(recordList.get(i).getDate(), i);
                }
                LOG.fine("R-Date:" + recordList.get(i).getDate()
                        + "\tS-Date:" + startDate + "\tE-Date:" + endDate);
                //recordList.remove(recordList.get(i));
            } else {
                endDate = addDate(endDate, slidingLength);
            }
        }
        LOG.info("Date List:" + dateIntegerMap.entrySet().size() + "\t" + windowRecordList.size());
        //processData(windowRecordList, dateIntegerMap);

//        if (recordList.size() == windowLength) {
//            windowRecordList = new ArrayList<>(windowLength);
//            windowRecordList.addAll(recordList.subList(0, windowLength));
//            remove(0, windowLength);
//            processData(windowRecordList, dateList);
//        }
    }

    private void remove(int startindex, int endindex) {
        for (int i = 0; i < endindex - startindex; i++) {
            recordList.remove(startindex);
        }
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

    int vectorCounter = 0;

    private void processData(List<Record> windowRecordList, Map<Date, Integer> dateList) {
        LOG.info("%%% Window Record List:%%%" + windowRecordList.size());
        int noOfDays = dateList.size();
        int splitCount = 0;
        int count = 0;
        int fullCount = 0;
        int capCount = 0;
        int size = -1;

        double totalCap = 0;

        String outFileName = "/home/kannan/out.csv";
        CleanMetric metric = this.metrics.get(outFileName);
        if (metric == null) {
            metric = new CleanMetric();
            this.metrics.put(outFileName, metric);
        }

        //Vector generation
        for (int i = 0; i < windowRecordList.size(); i++) {
            Record record = windowRecordList.get(i);
            int key = record.getSymbol();
            if (record.getFactorToAdjPrice() > 0) {
                splitCount++;
            }
            VectorPoint point = currentPoints.get(key);
            if (point == null) {
                point = new VectorPoint(key, noOfDays, false);
                currentPoints.put(key, point);
            }
            LOG.info("Received record value is:" + record.getSymbol() + "\t" + record.getDateString()
                    + "\tand its date:" + record.getDate() + "\t" + "Number Of Days:" + noOfDays
                    + "\tvector:" + point);

            // figure out the index
            int index = dateList.get(record.getDate());
            LOG.info("index value is:" + index);
            if (!point.add(record.getPrice(), record.getFactorToAdjPrice(), record.getFactorToAdjVolume(), metric, index)) {
                metric.dupRecords++;
                LOG.info("dup: " + record.serialize());
            }
            point.addCap(record.getVolume() * record.getPrice());

            if (point.noOfElements() == size) {
                fullCount++;
            }

            if (currentPoints.size() > 2000 && size == -1) {
                List<Integer> pointSizes = new ArrayList<>();
                for (VectorPoint v : currentPoints.values()) {
                    pointSizes.add(v.noOfElements());
                }
                size = mostCommon(pointSizes);
                LOG.info("Number of stocks per period: " + size);
            }
            // now write the current vectors, also make sure we have the size determined correctly
            if (currentPoints.size() > 1000 && size != -1 && fullCount > 750) {
                System.out.println("Processed: " + count);
                totalCap += writeVectors(noOfDays, metric);
                capCount++;
                fullCount = 0;
            }
        }
        System.out.println("Total stocks: " + vectorCounter + " bad stocks: " + currentPoints.size());
        metric.stocksWithIncorrectDays = currentPoints.size();
        System.out.println("Metrics for file: " + outFileName + " " + metric.serialize());
        currentPoints.clear();
        writeData(windowRecordList);
    }

    private double writeVectors(int size, CleanMetric metric) {
        double capSum = 0;
        int count = 0;
        for (Iterator<Map.Entry<Integer, VectorPoint>> it = currentPoints.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Integer, VectorPoint> entry = it.next();
            VectorPoint v = entry.getValue();
            if (v.noOfElements() == size) {
                metric.totalStocks++;

                if (!v.cleanVector(metric)) {
                    // System.out.println("Vector not valid: " + outFileName + ", " + v.serialize());
                    metric.invalidStocks++;
                    it.remove();
                    continue;
                }
                String sv = v.serialize();

                // if many points are missing, this can return null
                if (sv != null) {
                    capSum += v.getTotalCap();
                    count++;
                    context.write(Context.TWISTER2_DIRECT_EDGE, sv);
                    // remove it from map
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
        return capSum;
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
        return max.getKey();
    }

    private void writeData(List<Record> windowRecordList) {
        context.write(edgeName, windowRecordList);
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
        super.prepare(cfg, context);
    }

//    private Map<String, Map<Date, Integer>> findDates(String inFile) {
//
//        FileReader input = null;
//        Map<String, Map<Date, Integer>> outDates = new HashMap<>();
//        Map<String, Set<Date>> tempDates = new HashMap<>();
//
//        for (String dateRange : this.dates.keySet()) {
//            tempDates.put(dateRange, new TreeSet<Date>());
//        }
//
//        try {
//            input = new FileReader(inFile);
//            BufferedReader bufRead = new BufferedReader(input);
//            Record record;
//            while ((record = Utils.parseFile(bufRead, null, false)) != null) {
//                for (Map.Entry<String, List<Date>> ed : this.dates.entrySet()) {
//                    Date start = ed.getValue().get(0);
//                    Date end = ed.getValue().get(1);
//                    if (isDateWithing(start, end, record.getDate())) {
//                        Set<Date> tempDateList = tempDates.get(ed.getKey());
//                        tempDateList.add(record.getDate());
//                    }
//                }
//            }
//
//            for (Map.Entry<String, Set<Date>> ed : tempDates.entrySet()) {
//                Set<Date> datesSet = ed.getValue();
//                int i = 0;
//                Map<Date, Integer> dateIntegerMap = new HashMap<Date, Integer>();
//                for (Date d : datesSet) {
//                    dateIntegerMap.put(d, i);
//                    i++;
//                }
//                System.out.println("%%%% Key and date integer map size:" + ed.getKey() + "\t" + dateIntegerMap.size());
//                outDates.put(ed.getKey(), dateIntegerMap);
//            }
//        } catch (FileNotFoundException e) {
//            if (input != null) {
//                try {
//                    input.close();
//                } catch (IOException ignore) {
//                }
//            }
//        }
//
//        for (Map.Entry<String, Set<Date>> ed : tempDates.entrySet()) {
//            StringBuilder sb = new StringBuilder();
//            for (Date d : ed.getValue()) {
//                sb.append(Utils.formatter.format(d)).append(" ");
//            }
//            System.out.println(ed.getKey() + ":"  + sb.toString());
//        }
//        return outDates;
//    }
//
//
//    private boolean isDateWithing(Date start, Date end, Date compare) {
//        if (compare == null) {
//            System.out.println("Comapre null*****************");
//        }
//        return (compare.equals(start) || compare.after(start)) && compare.before(end);
//    }

    //    private List<String> vectorPoints;

//    public DataPreprocessingComputeTask(String vectordirectory, String distancedirectory,
//                                        int distancetype, String edgename) {
//        this.vectorDirectory = vectordirectory;
//        this.distanceDirectory = distancedirectory;
//        this.distanceType = distancetype;
//        this.edgeName = edgename;
//    }
//
//    @Override
//    public boolean execute(IMessage message) {
//        if (message.getContent() != null) {
//            LOG.fine("message content:" + message.getContent());
//            vectorPoints = new ArrayList<>();
//            if (message.getContent() != null) {
//                vectorPoints.add(String.valueOf(message.getContent()));
//            }
//        }
//
//        if (vectorPoints != null) {
//            context.write(edgeName, vectorPoints);
//        }
//        return true;
//    }

}
