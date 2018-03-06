package edu.iu.dsc.tws.apps.batch;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import org.omg.CORBA.PUBLIC_MEMBER;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class Source implements Runnable {
  private static final Logger LOG = Logger.getLogger(Source.class.getName());

  private long startSendingTime;

  private int task;

  private DataFlowOperation operation;

  private DataGenerator generator;

  private JobParameters jobParameters;

  private List<Long> startOfMessages;

  private int gap;

  private boolean genString;

  private volatile int inFlightMessages = 0;

  private boolean acked = false;

  public Source(int task, JobParameters jobParameters, DataFlowOperation op, DataGenerator dataGenerator, boolean getString, boolean acked) {
    this.task = task;
    this.jobParameters = jobParameters;
    this.operation = op;
    this.generator = dataGenerator;
    this.startOfMessages = new ArrayList<>();
    this.gap = jobParameters.getGap();
    this.genString = getString;
    this.acked = acked;
  }

  public Source(int task, JobParameters jobParameters, DataFlowOperation op, DataGenerator dataGenerator) {
    this.task = task;
    this.jobParameters = jobParameters;
    this.operation = op;
    this.generator = dataGenerator;
    this.startOfMessages = new ArrayList<>();
    this.gap = jobParameters.getGap();
    this.genString = false;
    this.acked = false;
  }

  @Override
  public void run() {
    startSendingTime = System.currentTimeMillis();
    Object data;
    if (genString) {
      data = generator.generateStringData();
    } else {
      data = generator.generateData();
    }
    int iterations = jobParameters.getIterations();
    for (int i = 0; i < iterations; i++) {
      int flag = 0;
      if (i == iterations - 1) {
        flag = MessageFlags.FLAGS_LAST;
      }

      while (acked && inFlightMessages > jobParameters.getOutstanding());

      while (!operation.send(task, data, flag)) {
        // lets wait a litte and try again
        operation.progress();
      }
      inFlightMessages++;
      startOfMessages.add(System.nanoTime());
      if (gap > 0) {
        try {
          Thread.sleep(gap);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public void ack(int source) {
    inFlightMessages--;
  }

  public long getStartSendingTime() {
    return startSendingTime;
  }

  public List<Long> getStartOfEachMessage() {
    return startOfMessages;
  }
}
