package edu.iu.dsc.tws.apps.storm;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.data.DataSave;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.apps.utils.Utils;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class BroadcastSource extends PartitionSource {
  private static final Logger LOG = Logger.getLogger(BroadcastSource.class.getName());

  private Map<Long, Integer> ackCounts = new HashMap<>();

  private int destinationsCount = 0;

  public BroadcastSource(int task, JobParameters jobParameters, DataGenerator dataGenerator, int executorId) {
    super(task, jobParameters, dataGenerator, executorId);
    this.destinationsCount = jobParameters.getTaskStages().get(1);
  }

  @Override
  public synchronized void ack(long id) {
    long time = 0;

    try {
//      int count = addAckCount(id);
//      if (count == destinationsCount) {
//        removeAckCount(id);
//
//        time = emitTimes.remove(id);
//        ackCount++;
//        outstanding--;
//        finalTimes.add(Utils.getTime() - time);
//      }
      time = emitTimes.remove(id);
      ackCount++;
      outstanding--;
      finalTimes.add(Utils.getTime() - time);
    } catch (NullPointerException e) {
      LOG.info(String.format("%d %d ******* Ack received %d", executorId, task, id));
    }

    if (ackCount >= noOfIterations - 1) {
      long totalTime = System.currentTimeMillis() - startSendingTime;
      long average = 0;
      int half = finalTimes.size() / 2;
      int count = 0;
      for (int i = half; i < finalTimes.size() - half / 4; i++) {
        average += finalTimes.get(i);
        count++;
      }
      average = average / count;
      LOG.info(String.format("%d %d Finished %d total: %d average: %f", executorId, task, ackCount, totalTime, average / 1000000.0));
      try {
        DataSave.saveList(jobParameters.getFileName() + "" + task + "partition_" + jobParameters.getSize() + "x" + noOfIterations, finalTimes);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
    }
  }

  private int addAckCount(long id) {
    int count = 0;
    if (ackCounts.containsKey(id)) {
      count = ackCounts.get(id);
    }
    count++;
    ackCounts.put(id, count);
    return count;
  }

  private void removeAckCount(long id) {
    ackCounts.remove(id);
  }
}
