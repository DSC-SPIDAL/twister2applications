package edu.iu.dsc.tws.apps.storm;

import edu.iu.dsc.tws.apps.data.AckData;
import edu.iu.dsc.tws.apps.utils.JobParameters;

import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BroadcastExecutor implements Runnable {
  private static final Logger LOG = Logger.getLogger(Executor.class.getName());

  private BroadcastSource source;

  private SecondBolt secondBolt;

  private Queue<Message> workerQueue;
  private Queue<Message> ackMessages;

  private JobParameters jobParameters;

  private int executorId;

  private boolean workerProgress;

  private boolean ackProgress;

  private boolean sourceProgress;

  public BroadcastExecutor(int executorId, boolean workerProgress, boolean ackProgress, boolean sourceProgress,
                        BroadcastSource source, SecondBolt secondBolt, JobParameters jobParameters) {
    this.source = source;
    this.executorId = executorId;
    this.jobParameters = jobParameters;
    this.secondBolt = secondBolt;
    this.workerProgress = workerProgress;
    this.ackProgress = ackProgress;
    this.sourceProgress = sourceProgress;

  }

  public void addWorkerQueue(Queue<Message> msgQueue) {
    workerQueue = msgQueue;
  }

  public void setAckMessages(Queue<Message> ackMessages) {
    this.ackMessages = ackMessages;
  }

  public void run() {
    try {
      while (true) {
        if (sourceProgress) {
          if (!source.isStop()) {
            while (source.execute()) ;
          }
          source.progress();
        }

        if (workerProgress) {
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
          secondBolt.progress();
        }

        while (ackProgress) {
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
}
