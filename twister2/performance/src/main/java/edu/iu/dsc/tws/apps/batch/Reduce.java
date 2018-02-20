package edu.iu.dsc.tws.apps.batch;

import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.apps.utils.Utils;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.*;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.IntData;
import edu.iu.dsc.tws.comms.mpi.io.ReduceBatchFinalReceiver;
import edu.iu.dsc.tws.comms.mpi.io.ReduceBatchPartialReceiver;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Reduce implements IContainer {
  private static final Logger LOG = Logger.getLogger(Reduce.class.getName());

  private DataFlowOperation reduce;

  private ResourcePlan resourcePlan;

  private int id;

  private Config config;

  private JobParameters jobParameters;

  private long startSendingTime;

  private long endReceiveTime;

  @Override
  public void init(Config cfg, int containerId, ResourcePlan plan) {
    LOG.log(Level.INFO, "Starting the example with container id: " + plan.getThisId());
    this.jobParameters = JobParameters.build(cfg);

    this.config = cfg;
    this.resourcePlan = plan;
    this.id = containerId;
    int noOfTasksPerExecutor = jobParameters.getTasks() / plan.noOfContainers();

    // lets create the task plan
    TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, plan, jobParameters.getTasks());
    //first get the communication config file
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);

    TWSCommunication channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    for (int i = 0; i < jobParameters.getParallel(); i++) {
      sources.add(i);
    }
    int dest = jobParameters.getTasks();

    Map<String, Object> newCfg = new HashMap<>();

    LOG.info("Setting up reduce dataflow operation");
    try {
      // this method calls the init method
      reduce = channel.reduce(newCfg, MessageType.OBJECT, 0, sources,
          dest, new ReduceBatchFinalReceiver(new IdentityFunction(), new FinalReduceReceiver()),
          new ReduceBatchPartialReceiver(dest, new IdentityFunction()));

      for (int i = 0; i < noOfTasksPerExecutor; i++) {
        // the map thread where data is produced
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
    private int sendCount = 0;

    MapWorker(int task) {
      this.task = task;
    }

    @Override
    public void run() {
      try {
        LOG.log(Level.INFO, "Starting map worker: " + id);
        startSendingTime = System.nanoTime();
        IntData data = generateData();
        int iterations = jobParameters.getIterations();
        for (int i = 0; i < iterations; i++) {
          // lets generate a message
          int flag = 0;
          if (i == iterations - 1) {
            flag = MessageFlags.FLAGS_LAST;
          }

          while (!reduce.send(task, data, flag)) {
            // lets wait a litte and try again
            reduce.progress();
          }
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
      if (count % 1 == 0) {
        LOG.info(String.format("%d Received %d", target, count));
      }
      return true;
    }
  }

  public static class IdentityFunction implements ReduceFunction {
    private int count = 0;
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public Object reduce(Object t1, Object t2) {
      count++;
      if (count % 100 == 0) {
        LOG.info(String.format("Partial received %d", count));
      }
      return t1;
    }
  }
}
