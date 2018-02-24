package edu.iu.dsc.tws.apps.batch;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.mpi.io.IntData;

import java.util.ArrayList;
import java.util.List;

public class MultiReduceWorker implements Runnable {
  private long startSendingTime;

  private int task;

  private DataFlowOperation operation;

  private DataGenerator generator;

  private JobParameters jobParameters;

  private List<Integer> destinations;

  public MultiReduceWorker(int task, JobParameters jobParameters, DataFlowOperation op, DataGenerator dataGenerator) {
    this.task = task;
    this.jobParameters = jobParameters;
    this.operation = op;
    this.generator = dataGenerator;
    this.destinations = new ArrayList<>();
    int fistStage = jobParameters.getTaskStages().get(0);
    int secondStage = jobParameters.getTaskStages().get(1);
    for (int i = 0; i < secondStage; i++) {
      destinations.add(i + fistStage);
    }
  }

  @Override
  public void run() {
    int noOfDestinations = destinations.size();

    startSendingTime = System.nanoTime();
    IntData data = generator.generateData();
    int iterations = jobParameters.getIterations();
    int nextIndex = 0;
    for (int i = 0; i < iterations; i++) {
      nextIndex = nextIndex % noOfDestinations;
      int dest = destinations.get(nextIndex);
      nextIndex++;

      int flag = 0;
      if (i == iterations - 1) {
        flag = MessageFlags.FLAGS_LAST;
      }
      while (!operation.send(task, data, flag, dest)) {
        // lets wait a litte and try again
        operation.progress();
      }
    }
  }

  public long getStartSendingTime() {
    return startSendingTime;
  }
}
