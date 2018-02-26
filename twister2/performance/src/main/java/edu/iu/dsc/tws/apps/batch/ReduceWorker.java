package edu.iu.dsc.tws.apps.batch;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.mpi.io.IntData;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class ReduceWorker implements Runnable {
  private static final Logger LOG = Logger.getLogger(ReduceWorker.class.getName());

  private long startSendingTime;

  private int task;

  private DataFlowOperation operation;

  private DataGenerator generator;

  private JobParameters jobParameters;

  private List<Long> startOfMessages;

  public ReduceWorker(int task, JobParameters jobParameters, DataFlowOperation op, DataGenerator dataGenerator) {
    this.task = task;
    this.jobParameters = jobParameters;
    this.operation = op;
    this.generator = dataGenerator;
    this.startOfMessages = new ArrayList<>();
  }

  @Override
  public void run() {
    startSendingTime = System.currentTimeMillis();
    IntData data = generator.generateData();
    int iterations = jobParameters.getIterations();
    for (int i = 0; i < iterations; i++) {
      startOfMessages.add(System.currentTimeMillis());
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

  public List<Long> getStartOfEachMessage() {
    return startOfMessages;
  }
}
