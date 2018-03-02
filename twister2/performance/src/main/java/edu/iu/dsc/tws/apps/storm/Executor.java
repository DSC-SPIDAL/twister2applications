package edu.iu.dsc.tws.apps.storm;

import edu.iu.dsc.tws.apps.data.AckData;
import edu.iu.dsc.tws.apps.data.DataSave;
import edu.iu.dsc.tws.apps.utils.JobParameters;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Executor implements Runnable {
  private static final Logger LOG = Logger.getLogger(Executor.class.getName());

  private PartitionSource source;

  private SecondBolt secondBolt;

  private Queue<Message> workerQueue;
  private Queue<Message> ackMessages;

  private long startSendingTime;

  List<Long> timesForTarget = new ArrayList<>();

  private JobParameters jobParameters;

  private int executorId;

  public Executor(int executorId, PartitionSource source, SecondBolt secondBolt, JobParameters jobParameters) {
    this.source = source;
    this.executorId = executorId;
    this.jobParameters = jobParameters;
    this.secondBolt = secondBolt;
  }

  public void addWorkerQueue(Queue<Message> msgQueue) {
    workerQueue = msgQueue;
  }

  public void setAckMessages(Queue<Message> ackMessages) {
    this.ackMessages = ackMessages;
  }

  @Override
  public void run() {
    try {
      while (true) {
        source.execute();
        startSendingTime = source.getStartSendingTime();
        Message message = workerQueue.peek();
        if (message != null) {
          if (secondBolt.execute(message)) {
            workerQueue.poll();
          }
        }

        Message ackMessage = ackMessages.poll();
        if (ackMessage != null) {
          AckData ackData = (AckData) ackMessage.getMessage();
          source.ack(ackData.getId());
        }
      }
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "Error occured", t);
    }
  }

  private void handleDataMessage(Message message) {

  }

  private void handleMessage(Message message) {
    long time = (System.currentTimeMillis() - startSendingTime);
    timesForTarget.add(System.nanoTime());
    int target = message.getTarget();
    try {
      if (executorId == 0) {
        if (timesForTarget.size() % 100 == 0) {
          LOG.info(String.format("%d Finished %d %d %d", executorId, target, time, timesForTarget.size()));
        }

        if (timesForTarget.size() >= jobParameters.getIterations() - jobParameters.getTaskStages().get(0)) {
          List<Long> times = source.getFinalMessages();
          List<Long> latencies = new ArrayList<>();
          long average = 0;
          for (int i = 0; i < times.size(); i++) {
            average += (timesForTarget.get(i) - times.get(i));
            latencies.add(timesForTarget.get(i) - times.get(i));
          }
          LOG.info(String.format("%d Average: %d", executorId, average / (times.size())));
          LOG.info(String.format("%d Finished %d %d %d", executorId, target, time, timesForTarget.size()));

          DataSave.saveList("reduce", latencies);
        }
      }
    } catch (Throwable r) {
      LOG.log(Level.SEVERE, String.format("%d excpetion", executorId), r);
    }
  }

  private void sendMessageBack() {

  }
}
