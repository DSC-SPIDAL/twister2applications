package edu.iu.dsc.tws.apps.storm;

import edu.iu.dsc.tws.apps.data.AckData;
import edu.iu.dsc.tws.apps.data.PartitionData;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SecondBolt {
  private static final Logger LOG = Logger.getLogger(SecondBolt.class.getName());

  private int task;

  private DataFlowOperation operation;

  private JobParameters jobParameters;

  private int executorId;

  private Map<Integer, Integer> sourceToAck;

  public SecondBolt(int task, JobParameters jobParameters, int executorId, Map<Integer, Integer> sourceToAck) {
    this.task = task;
    this.jobParameters = jobParameters;
    this.executorId = executorId;
    this.sourceToAck = sourceToAck;
  }

  public void setOperation(DataFlowOperation operation) {
    this.operation = operation;
  }

  public boolean execute(Message message) {
    operation.progress();
    PartitionData data = (PartitionData) message.getMessage();
    AckData ackData = new AckData(data.getTime(), data.getId());
    try {
      long time = System.currentTimeMillis() - data.getTime();
//      LOG.log(Level.INFO, String.format("%d ****** Received Message for acking: source %d target %d %s %d", executorId, message.getSource(), message.getTarget(), sourceToAck, time));
      boolean send = this.operation.send(task, ackData, 0, sourceToAck.get(message.getSource()));
      operation.progress();

//      Thread.sleep(1);
      return send;

    } catch (NullPointerException e) {
      LOG.log(Level.INFO, String.format("%d ****** Received Message for acking: source %d target %d %s", executorId, message.getSource(), message.getTarget(), sourceToAck));
    }
    return false;
  }
}
