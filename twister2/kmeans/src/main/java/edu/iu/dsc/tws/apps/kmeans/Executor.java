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
    long start = System.nanoTime();
    long communicateTime = 0;
    long computeTime = 0;
    while (true) {
      try {
        long computeStart = System.nanoTime();
        if (!task.executeMap()) {
          break;
        }
        computeTime += (System.nanoTime() - computeStart);

        long communicateStart = System.nanoTime();
        Message m = null;
        while (m == null) {
          task.progress();
          // wait for the results
          m = messages.poll();
        }
        // update the centers
        task.updateCenters((Centers) m.getMessage());
        communicateTime += (System.nanoTime() - communicateStart);
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
    LOG.info(String.format("%d K-Means time %d communicate %d compute %d", taskId, (System.nanoTime() - start) / 1000000,
        communicateTime / 1000000, computeTime / 1000000));
  }
}
