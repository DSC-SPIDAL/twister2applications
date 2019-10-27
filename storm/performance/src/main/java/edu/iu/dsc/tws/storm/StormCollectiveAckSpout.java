package edu.iu.dsc.tws.storm;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class StormCollectiveAckSpout extends BaseRichSpout {

    private static Logger LOG = LoggerFactory.getLogger(StormCollectiveAckSpout.class);

    private long noOfMessages = 0;
    private long noOfEmptyMessages = 1000;
    private List<Long> messageSizes = new ArrayList<Long>();
    private int currentSendIndex = 0;
    private SpoutOutputCollector collector;
    private int currentSendCount = 0;
    private byte []data = null;
    private int outstandingTuples = 0;
    private long maxOutstandingTuples = 100;
    private boolean debug;
    private int totalSendCount = 0;
    private int ackReceiveCount = 0;
    private long firstThroughputSendTime = 0;
    private String fileName;
    private String id;
    private long start = 0;
    private long printInveral = 0;
    private long lastSendTime = 0;
    private boolean startFailing = false;
    private int totalAckCount = 0;
    private int totalFailCount = 0;
    private boolean fileWritten = false;
    private long spoutParallel = 1;
    private long parallel = 1;
    private Map<String, Long> emitTimes = new HashMap<>();
    private boolean latency = false;
    private List<Long> times = new ArrayList<>();
    private long streamManagers = 0;
    private long sendGap = 0;
    private long workers = 1;
    private long getLastSendTime = 0;
    private TopologyContext context;

    private enum SendingType {
        DATA,
        EMPTY
    }

    private SendingType sendState = SendingType.EMPTY;

    @Override
    public void open(Map<String, Object> stormConf, TopologyContext topologyContext, SpoutOutputCollector outputCollector) {
        try {
            noOfMessages = (long) stormConf.get(Constants.ARGS_THRPUT_NO_MSGS);
            messageSizes = (List<Long>) stormConf.get(Constants.ARGS_THRPUT_SIZES);
            noOfEmptyMessages = (long) stormConf.get(Constants.ARGS_THRPUT_NO_EMPTY_MSGS);
            System.out.println("OutputCollector : " + outputCollector.getClass());
            this.collector = outputCollector;
            this.debug = (boolean) stormConf.get(Constants.ARGS_DEBUG);
            fileName = (String) stormConf.get(Constants.ARGS_THRPUT_FILENAME);
            printInveral = (long) stormConf.get(Constants.ARGS_PRINT_INTERVAL);
            id = topologyContext.getThisComponentId() + "_" + topologyContext.getThisTaskId();
            start = System.currentTimeMillis();
            lastSendTime = System.currentTimeMillis();
            spoutParallel = (long) stormConf.get(Constants.ARGS_SPOUT_PARALLEL);
            parallel = (long) stormConf.get(Constants.ARGS_PARALLEL);
            maxOutstandingTuples = (long) stormConf.get(Constants.ARGS_MAX_PENDING);
            streamManagers = (long) stormConf.get(Constants.ARGS_SREAM_MGRS);
            String mode = (String) stormConf.get(Constants.ARGS_MODE);
            long messagesPerSecond = (long) stormConf.get(Constants.ARGS_RATE);
            latency = true;
            if (messagesPerSecond > 0) {
                sendGap = 1000000000 / messagesPerSecond;
            }
            lastSendTime = System.nanoTime();
            context = topologyContext;
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        try {
            if (System.currentTimeMillis() - start < 15000 ) {
                return;
            }

            if (currentSendIndex >= messageSizes.size()) {
                return;
            }

            // we cannot send anything until we get enough acks
            if (outstandingTuples >= maxOutstandingTuples) {
                if (debug) {
                    LOG.info("Next tuple return, Send cound: " + totalSendCount + " outstanding: " + outstandingTuples);
                }
                return;
            }

            if (sendState == StormCollectiveAckSpout.SendingType.DATA && currentSendCount >= noOfMessages) {
                return;
            }

            if (sendState == StormCollectiveAckSpout.SendingType.EMPTY && currentSendCount >= noOfEmptyMessages) {
                return;
            }

            long now = System.nanoTime();
            if (sendGap != 0 && sendGap > now - lastSendTime) {
                return;
            }
            lastSendTime = now;

            lastSendTime = System.currentTimeMillis();
            long size = 1;
            if (currentSendCount == 0) {
                if (sendState == StormCollectiveAckSpout.SendingType.EMPTY) {
                    // LOG.info("Empty message generate");
                    data = Utils.generateData(1);
                } else {
                    // LOG.info("Data message generate");
                    size = messageSizes.get(currentSendIndex);
                    data = Utils.generateData(size);
                }
                firstThroughputSendTime = System.currentTimeMillis();
            } else {
                if (sendState == StormCollectiveAckSpout.SendingType.DATA) {
                    size = messageSizes.get(currentSendIndex);
                }
            }
            currentSendCount++;

            List<Object> list = new ArrayList<Object>();
            list.add(data);
            list.add(currentSendCount);
            list.add(size);
            long e = System.nanoTime();
            list.add(e);
            list.add(e);
            String id = UUID.randomUUID().toString();
            if (latency) {
                emitTimes.put(id, e);
            }
            //System.out.println(list.size() + ", " + id + ", " + this.collector.getClass());
            collector.emit(Constants.Fields.CHAIN_STREAM, list, id);
            if (debug) {
                if (totalSendCount % printInveral == 0) {
                    LOG.info("Send cound: " + totalSendCount + " outstanding: " + outstandingTuples + " id: " + id);
                }
            }
            totalSendCount++;
            outstandingTuples++;
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.Fields.CHAIN_STREAM,
                new Fields(
                        Constants.Fields.BODY,
                        Constants.Fields.MESSAGE_INDEX_FIELD,
                        Constants.Fields.MESSAGE_SIZE_FIELD,
                        Constants.Fields.TIME_FIELD,
                        Constants.Fields.TIME_FIELD2));
    }

    @Override
    public void ack(Object o) {
        if ((debug && ackReceiveCount % printInveral == 0) || startFailing) {
            LOG.info("Acked tuple: " + o.toString() + " total acked: " + totalAckCount + " send: "
                    + totalSendCount + " faile: " + totalFailCount + " "
                    + o.toString() + " outstanding: " + outstandingTuples);
        }
        if (latency) {
            Long time = emitTimes.remove(o.toString());
            if (time != null) {
                times.add(System.nanoTime() - time);
            }
        }
        totalAckCount++;
        handleAck(false, 0);
    }

    @Override
    public void fail(Object o) {
        LOG.info("Failed to process tuple: " + o.toString() + " total acked: " + totalAckCount + " send: "
                + totalSendCount + " faile: " + totalFailCount + " "
                + o.toString() + " outstanding: " + outstandingTuples);
        if (latency) {
            emitTimes.remove(o.toString());
        }
        startFailing = true;
        totalFailCount++;
        handleAck(true, o);
    }

    public void handleAck(boolean fail, Object ack) {
        outstandingTuples--;
        ackReceiveCount++;
        if (sendState == SendingType.EMPTY) {
            if (currentSendCount >= noOfEmptyMessages && ackReceiveCount >= noOfEmptyMessages) {
                currentSendCount = 0;
                if (currentSendIndex < messageSizes.size()) {
                    LOG.info("Started processing size: " + messageSizes.get(currentSendIndex));
                    System.out.println("Started processing size: " + messageSizes.get(currentSendIndex));
                }
                ackReceiveCount = 0;
                sendState = SendingType.DATA;
            }
        } else if (sendState == SendingType.DATA) {
            if (currentSendCount >= noOfMessages - noOfEmptyMessages && ackReceiveCount
                    >= noOfMessages - noOfEmptyMessages && !fileWritten) {
                long size = messageSizes.get(currentSendIndex);
                System.out.println("Write file for size: " + size +
                        String.format("sendCount: %d ackReceive: %d", currentSendCount, ackReceiveCount));
                long time = System.currentTimeMillis() - firstThroughputSendTime;
                String average = calculateStats();
                String currentOutPut = streamManagers + "x" + spoutParallel + "x" + parallel + " " +
                        noOfMessages + " " + size + " " + time + " " + average;
                writeFile(fileName + id, currentOutPut);
                writeListToFile(fileName + id + "_" + streamManagers + "x" + spoutParallel + "x" +
                        parallel + "_" + noOfMessages + "_" + size, times);
                times.clear();
                fileWritten = true;
            } else if (currentSendCount >= noOfMessages && ackReceiveCount >= noOfMessages) {
                long size = messageSizes.get(currentSendIndex);
                LOG.info("Finished message size: " + size);
                currentSendCount = 0;
                currentSendIndex++;
                ackReceiveCount = 0;
                sendState = SendingType.EMPTY;
                fileWritten = false;
            }
        }
    }

    private String calculateStats() {
        double ave = 0;
        for (int i = 0; i < times.size(); i++) {
            ave += (times.get(i) + 0.0) / 1000000;
        }
        ave = ave / times.size();

        double standardDev = 0;
        for (int i = 0; i < times.size(); i++) {
            double v = (times.get(i) + 0.0) / 1000000 - ave;
            standardDev += v * v;
        }
        standardDev = standardDev / times.size();
        standardDev = Math.sqrt(standardDev);
        return ave + " " + standardDev;
    }

    private void writeListToFile(String fileName, List<Long> list) {
        try(FileWriter fw = new FileWriter(fileName, true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw)) {
            for (Long l : list) {
                out.println(l);
            }
        } catch (IOException e) {
            //exception handling left as an exercise for the reader
            LOG.error("Failed to write to the file", e);
            e.printStackTrace();
        }
    }

    private void writeFile(String fileName, String line) {
        try(FileWriter fw = new FileWriter(fileName, true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw)) {
            out.println(line);
        } catch (IOException e) {
            //exception handling left as an exercise for the reader
            LOG.error("Failed to write to the file", e);
            e.printStackTrace();
        }
    }
}
