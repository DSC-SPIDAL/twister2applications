package edu.iu.dsc.tws.apps.storm;

import edu.iu.dsc.tws.apps.data.DataSave;
import edu.iu.dsc.tws.apps.utils.JobParameters;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Worker implements Runnable {
  private static final Logger LOG = Logger.getLogger(Worker.class.getName());

  private PartitionSource source;

  private Map<Integer, Queue<Message>> messages;

  private long startSendingTime;

  List<Long> timesForTarget = new ArrayList<>();

  private JobParameters jobParameters;

  private int executorId;

  public Worker(int executorId, PartitionSource source, JobParameters jobParameters) {
    this.source = source;
    this.messages = new HashMap<>();
    this.executorId = executorId;
    this.jobParameters = jobParameters;
  }

  public void addQueue(int target, Queue<Message> msgQueue) {
    messages.put(target, msgQueue);
  }

  @Override
  public void run() {
    while (true) {
      source.execute();
      startSendingTime = source.getStartSendingTime();
      for (Map.Entry<Integer, Queue<Message>> e : messages.entrySet()) {
        Queue<Message> messageQueue = e.getValue();
        Message message = messageQueue.poll();
        if (message != null) {
          handleMessage(message);
        }
      }
    }
  }

  private void handleMessage(Message message) {
    long time = (System.currentTimeMillis() - startSendingTime);
    timesForTarget.add(System.nanoTime());
    int target = message.getTarget();
    try {
      if (timesForTarget.size() >= jobParameters.getIterations()) {
        List<Long> times = source.getStartOfMessages();
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
    } catch (Throwable r) {
      LOG.log(Level.SEVERE, String.format("%d excpetion", executorId), r);
    }
  }
}
