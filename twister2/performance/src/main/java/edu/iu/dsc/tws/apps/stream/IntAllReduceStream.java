package edu.iu.dsc.tws.apps.stream;

import edu.iu.dsc.tws.apps.batch.IdentityFunction;
import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.data.DataType;
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

  private Map<Integer, InternalMessageWorker> reduceWorkers = new HashMap<>();

  private List<Integer> tasksOfThisExec;

  private Map<Integer, Queue<Integer>> times = new HashMap<>();

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
    reduce = channel.allReduce(newCfg, MessageType.OBJECT, 0, 1, sources,
        dests, middle, new IdentityFunction(), reduceReceiver, true);

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(id, taskPlan, jobParameters.getTaskStages(), 0);
    tasksOfThisExec = new ArrayList<>(tasksOfExecutor);

    Set<Integer> reduceTasksOfExecutor = Utils.getTasksOfExecutor(id, taskPlan, jobParameters.getTaskStages(), 1);
    List<Integer> taskOfExecutorList = new ArrayList<>(tasksOfExecutor);
    List<Integer> reduceTaskOfExecutorList = new ArrayList<>(reduceTasksOfExecutor);

    InternalMessageWorker reduceWorker = null;
    for (int i : tasksOfExecutor) {
      LinkedList<Integer> ackQueue = new LinkedList<>();
      reduceWorker = new InternalMessageWorker(id, i, jobParameters, reduce, dataGenerator, DataType.INT_OBJECT, ackQueue);
      reduceWorkers.put(i, reduceWorker);

      int sourceIndex = taskOfExecutorList.indexOf(i);
      times.put(reduceTaskOfExecutorList.get(sourceIndex), ackQueue);

      // the map thread where datacols is produced
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

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      LOG.info(String.format("Initialize: " + expectedIds));
    }

    @Override
    public boolean receive(int target, Object object) {
//      LOG.info(String.format("%d Received message %d keyset  %s", id, target, times.keySet()));
//      LOG.info(String.format("%d Received message %d size %d", id, target, times.get(target).size()));
      times.get(target).add(0);
      return true;
    }

    private boolean isDone() {
      return done;
    }
  }
}
