package edu.iu.dsc.tws.apps.stream;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.apps.utils.Utils;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PingPong implements IContainer {
  private static final Logger LOG = Logger.getLogger(PingPong.class.getName());

  private DataFlowOperation partition;
  private DataFlowOperation partition2;

  private JobParameters jobParameters;

  private byte[] data;

  private int sendCount = 0;

  private int itrCount = 0;

  private int id;

  @Override
  public void init(Config cfg, int containerId, ResourcePlan plan) {
    this.id = containerId;
    LOG.log(Level.INFO, "Starting the example with container id: " + plan.getThisId());

    this.jobParameters = JobParameters.build(cfg);
    DataGenerator dataGenerator = new DataGenerator(jobParameters);

    int id = containerId;

    // lets create the task plan
    TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, plan, jobParameters.getContainers() * 2);
    //first get the communication config file
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);

    TWSCommunication channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    Set<Integer> dests = new HashSet<>();
    sources.add(0);
    dests.add(2);

    Set<Integer> sources1 = new HashSet<>();
    Set<Integer> dests1 = new HashSet<>();
    sources1.add(3);
    dests1.add(1);

    data = (byte[]) Utils.generateData(Utils.getDataType(jobParameters.getDataType()), dataGenerator);

    Map<String, Object> newCfg = new HashMap<>();

    LOG.info("Setting up partition dataflow operation: " + sources + " " + dests);
    try {
      FirstReciver finalPartitionRec = new FirstReciver();
      SecondReceiver finalPartitionRec2 = new SecondReceiver();
      partition = channel.partition(newCfg, MessageType.BYTE, 1, sources, dests, finalPartitionRec);
      partition2 = channel.partition(newCfg, MessageType.BYTE, 2, sources1, dests1, finalPartitionRec2);

      if (id == 0) {
        putTime(data, System.nanoTime());
        partition.send(0, data, 0, 2);
      }
      // we need to progress the communication
      while (true) {
        // we should progress the communication directive
        partition.progress();
        partition2.progress();
        // progress the channel
        channel.progress();
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  public static byte[] longToBytes(long l) {
    byte[] result = new byte[8];
    for (int i = 7; i >= 0; i--) {
      result[i] = (byte)(l & 0xFF);
      l >>= 8;
    }
    return result;
  }

  public static long bytesToLong(byte[] b) {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result <<= 8;
      result |= (b[i] & 0xFF);
    }
    return result;
  }

  private void putTime(byte []data, long t) {
    byte[] time = longToBytes(t);
    for (int i = 0; i < time.length; i++) {
      data[i] = time[i];
    }
//    System.out.println("Time : " + getTime(data) + " actual: " + t);
  }

  private long getTime(byte[] data) {
    byte[]b = new byte[8];
    for (int i = 0; i < b.length; i++) {
      b[i] = data[i];
    }
    return bytesToLong(b);
  }

  private class FirstReciver implements MessageReceiver {
    private long start = System.nanoTime();

    private int count = 0;

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      count++;
      if (jobParameters.getPrintInterval() > 0 && count % jobParameters.getPrintInterval() == 0) {
        LOG.info("Received: " + count + " " + ((byte [])object).length);
      }
      if (count >= jobParameters.getIterations()) {
        long time = System.nanoTime();

        LOG.info("Finished: " + (time - start) / 1000000);
      }
      byte[] send = new byte[8];
      byte[] data = (byte[]) object;
      // extract time
      long time = getTime(data);
//      LOG.info(String.format("%d %d received %d %d", id, target, data.length, time));

      // now lets put this to reply
      putTime(send, time);
      // now send it back
      if (!partition2.send(3, send, 0, 1)) {
        throw new RuntimeException("We should always send");
      }
      return true;
    }


    public void progress() {
    }
  }

  private class SecondReceiver implements MessageReceiver {
    private long start = System.nanoTime();

    private int count = 0;

    private long sum = 0;

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      count++;

      byte[] d = (byte[]) object;
      long time = getTime(d);
//      LOG.info("Received time: " + time);
      sum += (System.nanoTime() - time);

      if (jobParameters.getPrintInterval() > 0 && count % jobParameters.getPrintInterval() == 0) {
        LOG.info("Received: " + count + " " + ((byte [])object).length);
      }
      if (count >= jobParameters.getIterations()) {
        time = System.nanoTime();

        LOG.info(String.format("Finished: %d latency %d", (time - start) / 1000000, sum / count));
        return true;
      }

      putTime(data, System.nanoTime());
      partition.send(0, data, 0, 2);

      return true;
    }


    public void progress() {
    }
  }

  public static void main(String[] args) {
    byte[] b = longToBytes(100);
    System.out.println(bytesToLong(b));
  }
}
