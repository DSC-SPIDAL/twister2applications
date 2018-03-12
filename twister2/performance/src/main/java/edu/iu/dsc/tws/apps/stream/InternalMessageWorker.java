package edu.iu.dsc.tws.apps.stream;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.data.DataType;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.apps.utils.Utils;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class InternalMessageWorker implements Runnable {
  private static final Logger LOG = Logger.getLogger(InternalMessageWorker.class.getName());

  private long startSendingTime;

  private int task;

  private DataFlowOperation operation;

  private DataGenerator generator;

  private JobParameters jobParameters;

  private List<Long> startOfMessages;

  Object data;

  public InternalMessageWorker(int task, JobParameters jobParameters, DataFlowOperation op, DataGenerator dataGenerator, DataType genString) {
    this.task = task;
    this.jobParameters = jobParameters;
    this.operation = op;
    this.generator = dataGenerator;
    this.startOfMessages = new ArrayList<>();

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
      startOfMessages.add(Utils.getTime());
    }
  }

  public long getStartSendingTime() {
    return startSendingTime;
  }

  public List<Long> getStartOfEachMessage() {
    return startOfMessages;
  }
}
