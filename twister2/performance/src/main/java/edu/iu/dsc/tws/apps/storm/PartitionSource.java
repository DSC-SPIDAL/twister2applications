package edu.iu.dsc.tws.apps.storm;

import edu.iu.dsc.tws.apps.batch.Source;
import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;

import java.util.ArrayList;
import java.util.List;
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

  public PartitionSource(int task, JobParameters jobParameters, DataFlowOperation op, DataGenerator dataGenerator, boolean getString) {
    this.task = task;
    this.jobParameters = jobParameters;
    this.operation = op;
    this.generator = dataGenerator;
    this.startOfMessages = new ArrayList<>();
    this.gap = jobParameters.getGap();
    this.genString = getString;
    this.destinations = new ArrayList<>();
  }

  public PartitionSource(int task, JobParameters jobParameters, DataFlowOperation op, DataGenerator dataGenerator) {
    this.task = task;
    this.jobParameters = jobParameters;
    this.operation = op;
    this.generator = dataGenerator;
    this.startOfMessages = new ArrayList<>();
    this.gap = jobParameters.getGap();
    this.genString = false;
    this.destinations = new ArrayList<>();

    int fistStage = jobParameters.getTaskStages().get(0);
    int secondStage = jobParameters.getTaskStages().get(1);
    for (int i = 0; i < secondStage; i++) {
      destinations.add(i + fistStage);
    }
  }

  public void execute() {
    int noOfDestinations = destinations.size();
    startSendingTime = System.currentTimeMillis();
    Object data;
    if (genString) {
      data = generator.generateStringData();
    } else {
      data = generator.generateData();
    }
    int iterations = jobParameters.getIterations();
    int nextIndex = 0;

    for (int i = 0; i < iterations; i++) {
      startOfMessages.add(System.nanoTime());
      nextIndex = nextIndex % noOfDestinations;
      if (i >= iterations - destinations.size()) {
        nextIndex = iterations - i - 1;
      }
      int dest = destinations.get(nextIndex);
      nextIndex++;
      int flag = 0;
      if (i >= iterations - destinations.size()) {
        flag = MessageFlags.FLAGS_LAST;
      }
      while (!operation.send(task, data, flag, dest)) {
        // lets wait a litte and try again
        operation.progress();
      }
      if (gap > 0) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public long getStartSendingTime() {
    return startSendingTime;
  }

  public List<Long> getStartOfMessages() {
    return startOfMessages;
  }
}
