package edu.iu.dsc.tws.apps.batch;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.apps.utils.Utils;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.*;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.gather.GatherBatchFinalReceiver;
import edu.iu.dsc.tws.comms.mpi.io.gather.GatherBatchPartialReceiver;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Gather implements IContainer {
  private static final Logger LOG = Logger.getLogger(Gather.class.getName());

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
    reduce = channel.gather(newCfg, MessageType.OBJECT, 0, sources,
        dest, new GatherBatchFinalReceiver(new FinalReduceReceiver()),
        new GatherBatchPartialReceiver(dest));

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(id, taskPlan, jobParameters.getTaskStages(), 0);
    Source source = null;
    for (int i : tasksOfExecutor) {
      source = new Source(i, jobParameters, reduce, dataGenerator);
      // the map thread where datacols is produced
      Thread mapThread = new Thread(source);
      mapThread.start();
    }

    // we need to progress the communication
    while (!reduceReceiver.isDone()) {
      // progress the channel
      channel.progress();
      // we should progress the communication directive
      reduce.progress();
      if (source != null) {
        startSendingTime = source.getStartSendingTime();
      }
    }
  }

  public class FinalReduceReceiver implements GatherBatchReceiver {
    boolean done = false;
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public void receive(int i, Iterator<Object> iterator) {
      long time = (System.nanoTime() - startSendingTime) / 1000000;
      LOG.info(String.format("%d Finished ", time));
      done = true;
    }

    private boolean isDone() {
      return done;
    }
  }
}
