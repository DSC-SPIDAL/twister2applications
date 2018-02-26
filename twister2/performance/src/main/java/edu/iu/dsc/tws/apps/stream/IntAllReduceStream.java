package edu.iu.dsc.tws.apps.stream;

import edu.iu.dsc.tws.apps.batch.IdentityFunction;
import edu.iu.dsc.tws.apps.batch.IntReduceWorker;
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

public class IntAllReduceStream implements IContainer {
  private static final Logger LOG = Logger.getLogger(IntAllReduceStream.class.getName());

  private DataFlowOperation reduce;

  private int id;

  private JobParameters jobParameters;

  private long startSendingTime;

  private Map<Integer, IntReduceWorker> reduceWorkers = new HashMap<>();

  private List<Integer> tasksOfThisExec;

  @Override
  public void init(Config cfg, int containerId, ResourcePlan plan) {
    LOG.log(Level.INFO, "Starting AllReduceStreaming Example");
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
    reduce = channel.allReduce(newCfg, MessageType.INTEGER, 0, 1, sources,
        dests, middle, new IdentityFunction(), reduceReceiver, true);

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(id, taskPlan, jobParameters.getTaskStages(), 0);
    tasksOfThisExec = new ArrayList<>(tasksOfExecutor);
    IntReduceWorker reduceWorker = null;
    for (int i : tasksOfExecutor) {
      reduceWorker = new IntReduceWorker(i, jobParameters, reduce, dataGenerator);
      reduceWorkers.put(i, reduceWorker);
      // the map thread where data is produced
      Thread mapThread = new Thread(reduceWorker);
      mapThread.start();
    }

    // we need to progress the communication
    while (true) {
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

    Map<Integer, List<Long>> times = new HashMap<>();

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      LOG.info(String.format("Initialize: " + expectedIds));
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        times.put(e.getKey(), new ArrayList<>());
      }
    }

    @Override
    public boolean receive(int target, Object object) {
      long time = (System.currentTimeMillis() - startSendingTime);
      List<Long> timesForTarget = times.get(target);
      timesForTarget.add(System.currentTimeMillis());

      try {
        if (timesForTarget.size() >= jobParameters.getIterations()) {
          List<Long> times = reduceWorkers.get(tasksOfThisExec.get(0)).getStartOfEachMessage();

          long average = 0;
          for (int i = 0; i < times.size(); i++) {
            average += (timesForTarget.get(i) - times.get(i));
          }
          LOG.info(String.format("%d Average: %d", id, average / (times.size())));
          done = true;
        }
      } catch (Throwable r) {
        LOG.log(Level.SEVERE, String.format("%d excpetion %s %s", id, tasksOfThisExec, reduceWorkers.keySet()), r);
      }

      return true;
    }

    private boolean isDone() {
      return done;
    }
  }
}
