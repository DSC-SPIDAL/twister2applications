package edu.iu.dsc.tws.apps.storm;

import edu.iu.dsc.tws.apps.data.AckData;
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

  public void run2() {
    try {
      while (true) {
        if (!source.isStop()) {
          while (source.execute());
        }

        while (true) {
          Message message = workerQueue.peek();
          if (message != null) {
            if (secondBolt.execute(message)) {
              workerQueue.poll();
            } else {
              break;
            }
          } else {
            break;
          }
        }

        while (true) {
          Message ackMessage = ackMessages.poll();
          if (ackMessage != null) {
            AckData ackData = (AckData) ackMessage.getMessage();
            source.ack(ackData.getId());
          } else {
            break;
          }
        }
      }
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "Error occured", t);
    }
  }

  public void run() {
    try {
      while (true) {
        if (!source.isStop()) {
          source.execute();
        }

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
}
