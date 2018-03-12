package edu.iu.dsc.tws.apps.stream;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.data.DataSave;
import edu.iu.dsc.tws.apps.data.DataType;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.apps.utils.Utils;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ExternalSource {
  private static final Logger LOG = Logger.getLogger(ExternalSource.class.getName());

  private long startSendingTime;

  private int task;

  private DataFlowOperation operation;

  private DataGenerator generator;

  private JobParameters jobParameters;

  private int gap;

  private DataType genString;

  private List<Integer> destinations;

  private long lastMessageTime = 0;

  private long currentIteration = 0;

  private int nextIndex = 0;

  private int executorId;

  private Object data;

  private int ackCount = 0;

  private List<Long> emitTimes = new LinkedList<>();

  private List<Long> finalTimes = new ArrayList<>();

  private int noOfIterations;

  private int outstanding;

  private boolean stop = false;

  private Random random;

  private boolean acked;

  private boolean destEnabled;

  public ExternalSource(int task, DataType genString, JobParameters jobParameters, DataGenerator dataGenerator, int executorId, boolean acked, boolean destEnabled) {
    this.task = task;
    this.acked = acked;
    this.jobParameters = jobParameters;
    this.generator = dataGenerator;
    this.genString = genString;
    this.gap = jobParameters.getGap();
    this.destinations = new ArrayList<>();
    this.executorId = executorId;
    this.noOfIterations = jobParameters.getIterations();
    int fistStage = jobParameters.getTaskStages().get(0);
    int secondStage = jobParameters.getTaskStages().get(1);
    for (int i = 0; i < secondStage; i++) {
      destinations.add(i + fistStage);
    }
    startSendingTime = System.currentTimeMillis();
    this.outstanding = 0;
    this.random = new Random(System.nanoTime());
    this.destEnabled = destEnabled;

    startSendingTime = System.currentTimeMillis();
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

  public void setOperation(DataFlowOperation operation) {
    this.operation = operation;
  }

  public boolean execute() {
    int noOfDestinations = destinations.size();
    long currentTime = System.currentTimeMillis();

    if (acked && outstanding > jobParameters.getOutstanding()) {
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
    if (destEnabled) {
      if (!operation.send(task, data, flag, dest)) {
        return false;
      }
    } else {
      if (!operation.send(task, data, flag)) {
        return false;
      }
    }
    emitTimes.add(time);
    nextIndex++;
    currentIteration++;
    outstanding++;
    lastMessageTime = System.currentTimeMillis();
//    LOG.info(String.format("%d source sending message from %d to %d", executorId,  task, dest));
    return true;
  }

  public void ack(long id) {
    long time;
    time = emitTimes.get(ackCount);
//    LOG.log(Level.INFO, "Ack received index" + ackCount);
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
