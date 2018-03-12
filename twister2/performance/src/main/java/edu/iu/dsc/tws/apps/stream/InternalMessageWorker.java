package edu.iu.dsc.tws.apps.stream;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.data.DataSave;
import edu.iu.dsc.tws.apps.data.DataType;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.apps.utils.Utils;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InternalMessageWorker implements Runnable {
  private static final Logger LOG = Logger.getLogger(InternalMessageWorker.class.getName());

  private long startSendingTime;

  private int task;

  private DataFlowOperation operation;

  private DataGenerator generator;

  private JobParameters jobParameters;

  private List<Long> emitTimes;

  Object data;

  private Queue<Integer> acks;

  private int ackCount = 0;

  private List<Long> finalTimes = new ArrayList<>();

  private boolean stop = false;

  private int executor;

  public InternalMessageWorker(int executor, int task, JobParameters jobParameters, DataFlowOperation op, DataGenerator dataGenerator, DataType genString, Queue<Integer> acks) {
    this.task = task;
    this.jobParameters = jobParameters;
    this.operation = op;
    this.generator = dataGenerator;
    this.emitTimes = new ArrayList<>();
    this.acks = acks;
    this.executor = executor;

    if (genString == DataType.STRING) {
      data = generator.generateStringData();
    } else if (genString == DataType.INT_OBJECT) {
      data = generator.generateData();
    } else if (genString == DataType.INT_ARRAY) {
      data = generator.generateByteData();
    } else {
      throw new RuntimeException("Un-expected data type");
    }
  }

  @Override
  public void run() {
    startSendingTime = System.currentTimeMillis();
    int iterations = jobParameters.getIterations();
    for (int i = 0; i < iterations; i++) {
      ack();
      int flag = 0;
      if (i == iterations - 1) {
        flag = MessageFlags.FLAGS_LAST;
      }
//      LOG.info("Sending message");
      while (!operation.send(task, data, flag)) {
        // lets wait a litte and try again
        if (jobParameters.getGap() > 0) {
          try {
            Thread.sleep(jobParameters.getGap());
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
      emitTimes.add(Utils.getTime());
    }

    while (true) {
      ack();
    }
  }

  public long getStartSendingTime() {
    return startSendingTime;
  }

  public List<Long> getStartOfEachMessage() {
    return emitTimes;
  }

  public void ack() {
    long time;
//    LOG.log(Level.INFO, "Acks size ** " + acks.size());
    Integer ack = acks.poll();
    while (ack != null) {
      time = emitTimes.get(ackCount);
      ackCount++;
//      LOG.log(Level.INFO, "Ack received index" + ackCount);
      finalTimes.add(Utils.getTime() - time);
      long totalTime = System.currentTimeMillis() - startSendingTime;
      if (ackCount >= jobParameters.getIterations() - 1) {
        long average = 0;
        int half = finalTimes.size() / 2;
        int count = 0;
        for (int i = half; i < finalTimes.size() - half / 4; i++) {
          average += finalTimes.get(i);
          count++;
        }
        average = average / count;
        LOG.info(String.format("%d %d Finished %d total: %d average: %f", executor, task, ackCount, totalTime, average / 1000000.0));
        try {
          DataSave.saveList(jobParameters.getFileName() + "" + task + "partition_" + jobParameters.getSize() + "x" + jobParameters.getIterations(), finalTimes);
        } catch (FileNotFoundException e) {
          e.printStackTrace();
        }
        stop = true;
      }
      ack = acks.poll();
    }
  }
}
