package edu.iu.dsc.tws.apps.batch;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.mpi.io.IntData;

public class ReduceWorker implements Runnable {
  private long startSendingTime;

  private int task;

  private DataFlowOperation operation;

  private DataGenerator generator;

  private JobParameters jobParameters;

  public ReduceWorker(int task, JobParameters jobParameters, DataFlowOperation op, DataGenerator dataGenerator) {
    this.task = task;
    this.jobParameters = jobParameters;
    this.operation = op;
    this.generator = dataGenerator;
  }

  @Override
  public void run() {
    startSendingTime = System.nanoTime();
    IntData data = generator.generateData();
    int iterations = jobParameters.getIterations();
    for (int i = 0; i < iterations; i++) {
      int flag = 0;
      if (i == iterations - 1) {
        flag = MessageFlags.FLAGS_LAST;
      }
      while (!operation.send(task, data, flag)) {
        // lets wait a litte and try again
        operation.progress();
      }
    }
  }

  public long getStartSendingTime() {
    return startSendingTime;
  }
}
