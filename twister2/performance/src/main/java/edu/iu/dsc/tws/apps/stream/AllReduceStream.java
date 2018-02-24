package edu.iu.dsc.tws.apps.stream;

import edu.iu.dsc.tws.apps.batch.AllReduce;
import edu.iu.dsc.tws.apps.batch.IdentityFunction;
import edu.iu.dsc.tws.apps.batch.ReduceWorker;
import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.apps.utils.Utils;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.ReduceReceiver;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AllReduceStream implements IContainer {
  private static final Logger LOG = Logger.getLogger(AllReduceStream.class.getName());

  private DataFlowOperation reduce;

  private int id;

  private JobParameters jobParameters;

  private long startSendingTime;

  private Map<Integer, ReduceWorker> reduceWorker = new HashMap<>();

  private List<Integer> tasksOfThisExec;

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
    int middle = jobParameters.getTaskStages().get(0) + jobParameters.getTaskStages().get(1);
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
    reduce = channel.allReduce(newCfg, MessageType.OBJECT, 0, 1, sources,
        dests, middle, new IdentityFunction(), reduceReceiver, false);

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(id, taskPlan, jobParameters.getTaskStages(), 0);
    tasksOfThisExec = new ArrayList<Integer>(tasksOfExecutor);
    for (int i : tasksOfExecutor) {
      ReduceWorker worker = new ReduceWorker(i, jobParameters, reduce, dataGenerator);
      reduceWorker.put(i, worker);
      // the map thread where data is produced
      Thread mapThread = new Thread(worker);
      mapThread.start();
    }

    // we need to progress the communication
    while (!reduceReceiver.isDone()) {
      // progress the channel
      channel.progress();
      // we should progress the communication directive
      reduce.progress();
      if (reduceWorker != null) {
        startSendingTime = reduceWorker.get(0).getStartSendingTime();
      }
    }
  }

  public class FinalReduceReceiver implements ReduceReceiver {
    boolean done = false;

    Map<Integer, List<Long>> times = new HashMap<>();

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        times.put(e.getKey(), new ArrayList<>());
      }
    }

    @Override
    public boolean receive(int target, Object object) {
      long time = (System.nanoTime() - startSendingTime) / 1000000;

      List<Long> timesForTarget = times.get(target);
      timesForTarget.add(System.nanoTime());
      LOG.info(String.format("%d Finished %d", target, time));

      if (timesForTarget.size() == jobParameters.getIterations()) {
        List<Long> times = reduceWorker.get(tasksOfThisExec.get(0)).getStartOfEachMessage();

        long average = 0;
        for (int i = 0; i < times.size(); i++) {
          average += (timesForTarget.get(i) - times.get(i));
        }
        LOG.info("Average: " + average / times.size());
      }

      return true;
    }

    private boolean isDone() {
      return done;
    }
  }
}
