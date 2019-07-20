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

    private OperationMode operationMode;
    private List<IMessage<Record>> messageList;

    private Map<Integer, VectorPoint> currentPoints = new HashMap();
    private Map<String, CleanMetric> metrics = new HashMap();

    private int vectorCounter = 0;

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
    public void prepare(Config cfg, TaskContext ctx) {
        super.prepare(cfg, ctx);
    }

    @Override
    public boolean process(IWindowMessage<Record> windowMessage) {
        messageList = windowMessage.getWindow();
        LOG.info("Received Message:" + messageList + "and its size:" + messageList.size());
        processRecords(messageList);
        return true;
    }

    public void processRecords(List<IMessage<Record>> recordList) {

        int noOfDays = recordList.size();

        int splitCount = 0;
        int count = 0;
        int fullCount = 0;

        double totalCap = 0;

        int index = 0;
        int size = -1;

        String outFileName = "/home/kannan/out.csv";
        CleanMetric metric = this.metrics.get(outFileName);
        if (metric == null) {
            metric = new CleanMetric();
            this.metrics.put(outFileName, metric);
        }

        for (int i = 0; i < recordList.size(); i++) {
            Record record = recordList.get(i).getContent();
            LOG.info("Received record value is:" + record + "\tand its date:" + record.getDate()
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
                LOG.info("dup: " + record.serialize());
                index++;
            }
            point.addCap(record.getVolume() * record.getPrice());

            if (point.noOfElements() == size) {
                fullCount++;
            }
            writeVectors(0, metric);
        }
        context.write(Context.TWISTER2_DIRECT_EDGE, recordList);
    }

    private double writeVectors(int size, CleanMetric metric) {
        double capSum = 0;
        int count = 0;
        LOG.info("Context Value:" + context.taskName() + "\tCurrent Points Size:" + currentPoints.size());
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
        context.write(Context.TWISTER2_DIRECT_EDGE, vectorPointsList);
        return capSum;
    }

    @Override
    public boolean processLateMessages(IMessage<Record> lateMessage) {
        return true;
    }
}
