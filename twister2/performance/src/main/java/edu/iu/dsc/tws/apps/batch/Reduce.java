package edu.iu.dsc.tws.apps.batch;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.apps.utils.Utils;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.*;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.reduce.ReduceBatchFinalReceiver;
import edu.iu.dsc.tws.comms.mpi.io.reduce.ReduceBatchPartialReceiver;
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

  private long startSendingTime;

  @Override
  public void init(Config cfg, int containerId, ResourcePlan plan) {
    LOG.log(Level.INFO, "Starting the example with container id: " + plan.getThisId());
    this.jobParameters = JobParameters.build(cfg);
    this.id = containerId;
    DataGenerator dataGenerator = new DataGenerator(jobParameters);

    // lets create the task plan
    TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, plan, jobParameters.getTaskStages());
    LOG.info("Task plan: " + taskPlan);

    //first get the communication config file
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);
    TWSCommunication channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    int dest = noOfSourceTasks;

    Map<String, Object> newCfg = new HashMap<>();

    LOG.info(String.format("Setting up reduce dataflow operation %s %d", sources, dest));
    // this method calls the init method
    FinalReduceReceiver reduceReceiver = new FinalReduceReceiver();
    reduce = channel.reduce(newCfg, MessageType.OBJECT, 0, sources,
        dest, new ReduceBatchFinalReceiver(new IdentityFunction(), reduceReceiver),
        new ReduceBatchPartialReceiver(dest, new IdentityFunction()));

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(id, taskPlan, jobParameters.getTaskStages(), 0);
    ReduceWorker reduceWorker = null;
    for (int i : tasksOfExecutor) {
      reduceWorker = new ReduceWorker(i, jobParameters, reduce, dataGenerator);
      // the map thread where datacols is produced
      Thread mapThread = new Thread(reduceWorker);
      mapThread.start();
    }

    // we need to progress the communication
    while (!reduceReceiver.isDone()) {
        // progress the channel
        channel.progress();
        // we should progress the communication directive
        reduce.progress();
        if (reduceWorker != null) {
          startSendingTime = reduceWorker.getStartSendingTime();
        }
    }
  }

  public class FinalReduceReceiver implements ReduceReceiver {
    boolean done = false;
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean receive(int target, Object object) {
      long time = (System.nanoTime() - startSendingTime) / 1000000;
      LOG.info(String.format("%d Finished %d", target, time));
      done = true;
      return true;
    }

    private boolean isDone() {
      return done;
    }
  }

  public class IdentityFunction implements ReduceFunction {
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
