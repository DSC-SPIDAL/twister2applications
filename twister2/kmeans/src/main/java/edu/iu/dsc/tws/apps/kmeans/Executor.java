package edu.iu.dsc.tws.apps.kmeans;

import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class Executor implements Runnable {
  private static final Logger LOG = Logger.getLogger(Executor.class.getName());

  private PipelinedTask task;

  private BlockingQueue<Message> messages;

  private int taskId;

  public Executor(PipelinedTask task, BlockingQueue<Message> messages, int taskId) {
    this.task = task;
    this.messages = messages;
    this.taskId = taskId;
  }

  @Override
  public void run() {
    // execute first time

    while (true) {
      try {
        if (!task.executeMap()) {
          break;
        }

        task.progress();
        // wait for the results
        Message m = messages.take();
        // update the centers
        task.updateCenters((double []) m.getMessage());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
