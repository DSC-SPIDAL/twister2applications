package edu.iu.dsc.tws.apps.storm;

import edu.iu.dsc.tws.apps.batch.Source;
import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.data.DataSave;
import edu.iu.dsc.tws.apps.data.PartitionData;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class PartitionSource {
  private static final Logger LOG = Logger.getLogger(Source.class.getName());

  private long startSendingTime;

  private int task;

  private DataFlowOperation operation;

  private DataGenerator generator;

  private JobParameters jobParameters;

  private List<Long> startOfMessages;

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


  public PartitionSource(int task, JobParameters jobParameters, DataGenerator dataGenerator, int executorId) {
    this.task = task;
    this.jobParameters = jobParameters;
    this.generator = dataGenerator;
    this.startOfMessages = new ArrayList<>();
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

    data = dataGenerator.generateByteData();
  }

  public void setOperation(DataFlowOperation operation) {
    this.operation = operation;
  }

  public void execute() {
    int noOfDestinations = destinations.size();
    startSendingTime = System.currentTimeMillis();

    operation.progress();

    long currentTime = System.currentTimeMillis();
    if (gap > (currentTime - lastMessageTime)) {
      return;
    }

    if (currentIteration < noOfIterations) {
      startOfMessages.add(System.nanoTime());
      nextIndex = nextIndex % noOfDestinations;
      int dest = destinations.get(nextIndex);
      nextIndex++;
      int flag = 0;
      if (currentIteration >= noOfIterations - destinations.size()) {
        flag = MessageFlags.FLAGS_LAST;
      }
      lastMessageTime = System.currentTimeMillis();
      long time = System.nanoTime();
      PartitionData partitionData = new PartitionData(data, time, currentIteration);
      if (!operation.send(task, partitionData, flag, dest)) {
        // lets wait a litte and try again
        return;
      }
      emitTimes.put(currentIteration, time);
//      LOG.fine(String.format("%d task %d sends %d", executorId, task, currentIteration));
      currentIteration++;
    }
  }

  public void ack(long id) {
    long time = emitTimes.get(id);
    ackCount++;
    finalTimes.add(System.nanoTime() - time);
    if (ackCount >= noOfIterations - 1) {
      LOG.info(String.format("Finished %d", ackCount));
      try {
        DataSave.saveList(jobParameters.getFileName() + "" + task + "partition_" + jobParameters.getSize() + "x" + noOfIterations, finalTimes);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
    }
    if (ackCount % 100 == 0 && executorId == 0) {
      LOG.info(String.format("%d received task %d ack %d %d %d", executorId, task, id, ackCount, noOfIterations));
    }
  }

  public long getStartSendingTime() {
    return startSendingTime;
  }

  public List<Long> getFinalMessages() {
    return finalTimes;
  }
}
