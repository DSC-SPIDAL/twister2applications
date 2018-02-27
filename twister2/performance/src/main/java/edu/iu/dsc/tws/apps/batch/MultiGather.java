package edu.iu.dsc.tws.apps.batch;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.apps.utils.Utils;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.GatherBatchReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.gather.GatherMultiBatchFinalReceiver;
import edu.iu.dsc.tws.comms.mpi.io.gather.GatherMultiBatchPartialReceiver;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MultiGather implements IContainer {
  private static final Logger LOG = Logger.getLogger(MultiGather.class.getName());

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
    int middle = jobParameters.getTaskStages().get(0);
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    Set<Integer> dests = new HashSet<>();
    int noOfDestTasks = jobParameters.getTaskStages().get(1);
    for (int i = 0; i < noOfDestTasks; i++) {
      dests.add(i + sources.size());
    }

    Map<String, Object> newCfg = new HashMap<>();

    LOG.info(String.format("Setting up reduce dataflow operation %s %s", sources, dests));
    // this method calls the init method
    FinalReduceReceiver reduceReceiver = new FinalReduceReceiver();
    reduce = channel.keyedGather(newCfg, MessageType.OBJECT, dests, sources,
        dests, new GatherMultiBatchFinalReceiver(new FinalReduceReceiver()),
        new GatherMultiBatchPartialReceiver());

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(id, taskPlan, jobParameters.getTaskStages(), 0);
    MultiSource reduceWorker = null;
    for (int i : tasksOfExecutor) {
      reduceWorker = new MultiSource(i, jobParameters, reduce, dataGenerator);
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

  public class FinalReduceReceiver implements GatherBatchReceiver {
    boolean done = false;
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public void receive(int target, Iterator<Object> iterator) {
      long time = (System.nanoTime() - startSendingTime) / 1000000;
      LOG.info(String.format("%d Finished %d", target, time));
      done = true;
    }

    private boolean isDone() {
      return done;
    }
  }
}
