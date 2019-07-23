package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.apps.stockanalysis.utils.CleanMetric;
import edu.iu.dsc.tws.apps.stockanalysis.utils.Record;
import edu.iu.dsc.tws.apps.stockanalysis.utils.VectorPoint;
import edu.iu.dsc.tws.task.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.window.collectives.ProcessWindow;
import edu.iu.dsc.tws.task.window.function.ProcessWindowedFunction;

import java.util.*;
import java.util.logging.Logger;

public class DataProcessingStreamingWindowCompute extends ProcessWindow<Record> {

    private static final long serialVersionUID = -343442338342343424L;

    private static final Logger LOG = Logger.getLogger(DataProcessingStreamingWindowCompute.class.getName());

    private int vectorCounter = 0;

    private OperationMode operationMode;

    private Map<Integer, VectorPoint> currentPoints = new HashMap();
    private Map<String, CleanMetric> metrics = new HashMap();

    //private List<IMessage<Record>> messageList;
    private List<String> vectorPointsList = new ArrayList<>();

    public DataProcessingStreamingWindowCompute(ProcessWindowedFunction<Record> processWindowedFunction) {
        super(processWindowedFunction);
    }

    public DataProcessingStreamingWindowCompute(ProcessWindowedFunction<Record> processWindowedFunction,
                                                OperationMode operationMode) {
        super(processWindowedFunction);
        this.operationMode = operationMode;
    }

    @Override
    public boolean process(IWindowMessage<Record> windowMessage) {
        LOG.info("window size:" + windowMessage.getWindow().size());
        List<IMessage<Record>> messageList = windowMessage.getWindow();
        LOG.info("Received Message:" + messageList + "and its size:" + messageList.size());
        if (messageList != null) {
            processRecords(messageList);
        }
        return true;
    }

    public void processRecords(List<IMessage<Record>> recordList) {

        int noOfDays = recordList.size();
        int splitCount = 0;
        int count = 0;
        int fullCount = 0;
        int capCount = 0;
        int index = 0;
        int size = -1;

        double totalCap = 0;

        String outFileName = "/home/kannan/out.csv";
        CleanMetric metric = this.metrics.get(outFileName);
        if (metric == null) {
            metric = new CleanMetric();
            this.metrics.put(outFileName, metric);
        }

        Date startDate = recordList.get(0).getContent().getDate();
        Date endDate = recordList.get(0 + 6).getContent().getDate();

        for (int i = 0; i < recordList.size(); i++) {
            Record record = recordList.get(i).getContent();
            LOG.fine("Received record value is:" + record + "\tand its date:" + record.getDate()
            + "\t" + "Number Of Days:" + noOfDays);

            int key = record.getSymbol();
            if (record.getFactorToAdjPrice() > 0) {
                splitCount++;
            }
            VectorPoint point = currentPoints.get(key);
            if (point == null) {
                point = new VectorPoint(key, noOfDays, false);
                currentPoints.put(key, point);
            }

            // figure out the index
            //int index = datesList.get(record.getDate());
            if (!point.add(record.getPrice(), record.getFactorToAdjPrice(), record.getFactorToAdjVolume(), metric, index)) {
                metric.dupRecords++;
                LOG.fine("dup: " + record.serialize());
                index++;
            }
            point.addCap(record.getVolume() * record.getPrice());

            if (point.noOfElements() == size) {
                fullCount++;
            }

            if (currentPoints.size() > 2000 && size == -1) {
                List<Integer> pointSizes = new ArrayList<Integer>();
                for (VectorPoint v : currentPoints.values()) {
                    pointSizes.add(v.noOfElements());
                }
                size = mostCommon(pointSizes);
                LOG.info("Number of stocks per period: " + size);
            }
            LOG.fine("Processed: " + count);
            /*if (currentPoints.size() > 1000 && size != -1 && fullCount > 750) {
                System.out.println("Processed: " + count);
                totalCap += writeVectors(noOfDays, metric);
                capCount++;
                fullCount = 0;
            }*/
            //totalCap += writeVectors(noOfDays, metric);
            context.write(Context.TWISTER2_DIRECT_EDGE, currentPoints);
        }
        metric.stocksWithIncorrectDays = currentPoints.size();
        System.out.println("Metrics for file: " + outFileName + " " + metric.serialize());
        currentPoints.clear();
        LOG.info("I finished processing");
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

    private double writeVectors(int size, CleanMetric metric) {
        double capSum = 0;
        int count = 0;
        LOG.info("Context Value:" + context.taskName() + "\tCurrent Points Size:" + currentPoints.size());
        if (currentPoints != null) {
            context.write(Context.TWISTER2_DIRECT_EDGE, currentPoints);
        }
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
                    vectorPointsList.add(sv);

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
        LOG.info("Vector counter value:" + vectorCounter);
        LOG.info("Writing Vector Points Size:" + vectorPointsList.size());
        if (vectorPointsList != null) {
            context.write(Context.TWISTER2_DIRECT_EDGE, vectorPointsList);
        }
        return capSum;
    }

    @Override
    public boolean processLateMessages(IMessage<Record> lateMessage) {
        return true;
    }


    @Override
    public void prepare(Config cfg, TaskContext ctx) {
        super.prepare(cfg, ctx);
    }
}
