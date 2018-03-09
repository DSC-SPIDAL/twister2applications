package edu.iu.dsc.tws.apps.storm;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.data.DataSave;
import edu.iu.dsc.tws.apps.data.PartitionData;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.apps.utils.Utils;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.logging.Logger;

public class PartitionSource {
  private static final Logger LOG = Logger.getLogger(PartitionSource.class.getName());

  private long startSendingTime;

  private int task;

  private DataFlowOperation operation;

  private DataGenerator generator;

  private JobParameters jobParameters;

  private int gap;

  private boolean genString;

  private List<Integer> destinations;

  private long lastMessageTime = 0;

  private long currentIteration = 0;

  private int nextIndex = 0;

  private int executorId;

  private byte[] data;

  private int ackCount = 0;

  private Map<Long, Long> emitTimes = new HashMap<>();

  private List<Long> finalTimes = new ArrayList<>();

  private int noOfIterations;

  private int outstanding;

  private boolean stop = false;

  private Random random;

  public PartitionSource(int task, JobParameters jobParameters, DataGenerator dataGenerator, int executorId) {
    this.task = task;
    this.jobParameters = jobParameters;
    this.generator = dataGenerator;
    this.gap = jobParameters.getGap();
    this.genString = false;
    this.destinations = new ArrayList<>();
    this.executorId = executorId;
    this.noOfIterations = jobParameters.getIterations();
    int fistStage = jobParameters.getTaskStages().get(0);
    int secondStage = jobParameters.getTaskStages().get(1);
    for (int i = 0; i < secondStage; i++) {
      destinations.add(i + fistStage);
    }
    startSendingTime = System.currentTimeMillis();
    data = dataGenerator.generateByteData();
    this.outstanding = 0;
    this.random = new Random(System.nanoTime());
  }

  public void setOperation(DataFlowOperation operation) {
    this.operation = operation;
  }

  public synchronized boolean execute() {
    int noOfDestinations = destinations.size();
    long currentTime = System.currentTimeMillis();

    if (outstanding > jobParameters.getOutstanding()) {
      return false;
    }

    if (gap > (currentTime - lastMessageTime)) {
      return false;
    }

    if (currentIteration >= noOfIterations) {
      stop = true;
      return false;
    }

    nextIndex = nextIndex % noOfDestinations;
    int dest = destinations.get(nextIndex);
    int flag = 0;
    long time = Utils.getTime();
    PartitionData partitionData = new PartitionData(data, time, currentIteration);
    if (!operation.send(task, partitionData, flag, dest)) {
      return false;
    }
    emitTimes.put(currentIteration, time);
    nextIndex++;
    currentIteration++;
    outstanding++;
    lastMessageTime = System.currentTimeMillis();
//    LOG.info(String.format("%d source sending message from %d to %d", executorId,  task, dest));
    return true;
  }

  public synchronized void ack(long id) {
    LOG.info(String.format("%d %d Ack received %d", executorId, task, id));
    long time = 0;
    try {
      time = emitTimes.remove(id);
    } catch (NullPointerException e) {
      LOG.info(String.format("%d %d ******* Ack received %d", executorId, task, id));
    }
    ackCount++;
    outstanding--;
    finalTimes.add(Utils.getTime() - time);
    long totalTime = System.currentTimeMillis() - startSendingTime;
    if (ackCount >= noOfIterations - 1) {
      long average = 0;
      int half = finalTimes.size() / 2;
      int count = 0;
      for (int i = half; i < finalTimes.size() - half / 4; i++) {
        average += finalTimes.get(i);
        count++;
      }
      average = average / count;
      LOG.info(String.format("%d %d Finished %d total: %d average: %f", executorId, task, ackCount, totalTime, average / 1000000.0));
      try {
        DataSave.saveList(jobParameters.getFileName() + "" + task + "partition_" + jobParameters.getSize() + "x" + noOfIterations, finalTimes);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
    }
//    if (ackCount % 10 == 0 && executorId == 0) {
//      LOG.info(String.format("%d received task %d ack %d %d %d", executorId, task, id, ackCount, noOfIterations));
//    }
  }

  public void progress() {
    operation.progress();
  }

  public long getStartSendingTime() {
    return startSendingTime;
  }

  public List<Long> getFinalMessages() {
    return finalTimes;
  }

  public boolean isStop() {
    return stop;
  }
}
