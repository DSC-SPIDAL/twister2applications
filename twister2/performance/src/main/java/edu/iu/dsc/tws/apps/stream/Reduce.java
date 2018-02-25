package edu.iu.dsc.tws.apps.stream;

import edu.iu.dsc.tws.apps.data.IntData;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.apps.utils.Utils;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.*;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.reduce.ReduceStreamingFinalReceiver;
import edu.iu.dsc.tws.comms.mpi.io.reduce.ReduceStreamingPartialReceiver;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Reduce implements IContainer {
  private static final Logger LOG = Logger.getLogger(Reduce.class.getName());

  private DataFlowOperation reduce;

  private int id;

  private JobParameters jobParameters;

  @Override
  public void init(Config cfg, int containerId, ResourcePlan plan) {
    this.jobParameters = JobParameters.build(cfg);
    this.id = containerId;
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    int noOfTasksPerExecutor = noOfSourceTasks / plan.noOfContainers();

    // lets create the task plan
    TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, plan, jobParameters.getTaskStages());
    //first get the communication config file
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);
    TWSCommunication channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }

    int dest = noOfSourceTasks;
    Map<String, Object> newCfg = new HashMap<>();

    LOG.info("Setting up reduce dataflow operation");
    try {
      // this method calls the init method
      // I think this is wrong
      reduce = channel.reduce(newCfg, MessageType.OBJECT, 0, sources,
          dest, new ReduceStreamingFinalReceiver(new IdentityFunction(), new FinalReduceReceiver()),
          new ReduceStreamingPartialReceiver(dest,  new IdentityFunction()));

      for (int i = 0; i < noOfTasksPerExecutor; i++) {
        // the map thread where data is produced
        LOG.info(String.format("%d Starting %d", id, i + id * noOfTasksPerExecutor));
        Thread mapThread = new Thread(new MapWorker(i + id * noOfTasksPerExecutor));
        mapThread.start();
      }

      // we need to progress the communication
      while (true) {
        try {
          // progress the channel
          channel.progress();
          // we should progress the communication directive
          reduce.progress();
          Thread.yield();
        } catch (Throwable t) {
          t.printStackTrace();
        }
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  /**
   * We are running the map in a separate thread
   */
  private class MapWorker implements Runnable {
    private int task = 0;
    MapWorker(int task) {
      this.task = task;
    }

    @Override
    public void run() {
      try {
        LOG.log(Level.INFO, "Starting map worker: " + id);
        IntData data = generateData();
        for (int i = 0; i < 11000; i++) {
          // lets generate a message
          while (!reduce.send(task, data, 0)) {
            // lets wait a litte and try again
            try {
              Thread.sleep(1);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          if (i % 1000 == 0) {
            LOG.info(String.format("%d sent %d", id, i));
          }
          Thread.yield();
        }
        LOG.info(String.format("%d Done sending", id));
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

  /**
   * Generate data with an integer array
   *
   * @return IntData
   */
  private IntData generateData() {
    int s = 64000;
    int[] d = new int[s];
    for (int i = 0; i < s; i++) {
      d[i] = i;
    }
    return new IntData(d);
  }

  public static class FinalReduceReceiver implements ReduceReceiver {
    private int count = 0;
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean receive(int target, Object object) {
      count++;
      if (count % 100 == 0) {
        LOG.info(String.format("Received %d", count));
      }
      return true;
    }
  }

  public static class IdentityFunction implements ReduceFunction {
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public Object reduce(Object t1, Object t2) {
      return t1;
    }
  }
}