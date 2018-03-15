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

public class BenchmarkStream implements IContainer {
  private static final Logger LOG = Logger.getLogger(BenchmarkStream.class.getName());

  private DataFlowOperation partition;

  private JobParameters jobParameters;

  private Object data;

  private int sendCount = 0;

  @Override
  public void init(Config cfg, int containerId, ResourcePlan plan) {
    LOG.log(Level.INFO, "Starting the example with container id: " + plan.getThisId());

    this.jobParameters = JobParameters.build(cfg);
    DataGenerator dataGenerator = new DataGenerator(jobParameters);

    int id = containerId;

    // lets create the task plan
    TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, plan, jobParameters.getContainers());
    //first get the communication config file
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);

    TWSCommunication channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    Set<Integer> dests = new HashSet<>();
    sources.add(0);
    dests.add(1);

    data = Utils.generateData(Utils.getDataType(jobParameters.getDataType()), dataGenerator);

    Map<String, Object> newCfg = new HashMap<>();

    LOG.info("Setting up partition dataflow operation: " + sources + " " + dests);
    try {
      FinalPartitionReciver finalPartitionRec = new FinalPartitionReciver();
      partition = channel.partition(newCfg, MessageType.BYTE, 1, sources, dests, finalPartitionRec);
      // we need to progress the communication
      while (true) {
        if (id == 0) {
          sendMessage(0, 1);
        }
        // we should progress the communication directive
        partition.progress();
        // progress the channel
        channel.progress();
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  private void sendMessage(int source, int dest) {
    while (sendCount < jobParameters.getIterations()) {
      if (!partition.send(source, data, 0, dest)) {
        break;
      }
      sendCount++;
    }
  }

  private class FinalPartitionReciver implements MessageReceiver {
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
      return true;
    }


    public void progress() {
    }
  }
}
