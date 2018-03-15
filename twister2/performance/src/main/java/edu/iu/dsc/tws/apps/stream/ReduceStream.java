package edu.iu.dsc.tws.apps.stream;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.data.DataType;
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

public class ReduceStream implements IContainer {
  private static final Logger LOG = Logger.getLogger(ReduceStream.class.getName());
  private DataFlowOperation reduce;

  private ResourcePlan resourcePlan;

  private int id;

  private Config config;

  private JobParameters jobParameters;

  private long startSendingTime;

  private Map<Integer, ExternalSource> reduceWorkers = new HashMap<>();

  private List<Integer> tasksOfThisExec;

  private boolean executorWithDest = false;

  private DataType dataType;

  @Override
  public void init(Config cfg, int containerId, ResourcePlan plan) {
    LOG.log(Level.FINE, "Starting the example with container id: " + plan.getThisId());

    this.jobParameters = JobParameters.build(cfg);
    this.id = containerId;
    DataGenerator dataGenerator = new DataGenerator(jobParameters);

    // lets create the task plan
    TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, plan, jobParameters.getTaskStages());
    LOG.log(Level.FINE,"Task plan: " + taskPlan);
    //first get the communication config file
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);

    TWSCommunication channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    int dest = jobParameters.getTaskStages().get(0);

    int destExecutor = taskPlan.getExecutorForChannel(dest);
    if (destExecutor == id) {
      executorWithDest = true;
    }
    Map<String, Object> newCfg = new HashMap<>();

    LOG.log(Level.FINE,"Setting up reduce dataflow operation");
    try {
      dataType = Utils.getDataType(jobParameters.getDataType());
      // this method calls the init method
      // I think this is wrong
      reduce = channel.reduce(newCfg, Utils.getMessageTupe(jobParameters.getDataType()), 0, sources,
          dest, new ReduceStreamingFinalReceiver(new IdentityFunction(), new FinalReduceReceiver()),
          new ReduceStreamingPartialReceiver(dest, new IdentityFunction()), new SendCompletion());

      Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(id, taskPlan, jobParameters.getTaskStages(), 0);
      tasksOfThisExec = new ArrayList<>(tasksOfExecutor);
      ExternalSource source = null;
      int destExector = taskPlan.getExecutorForChannel(dest);
      boolean acked = destExector == id;
      for (int i : tasksOfExecutor) {
        source = new ExternalSource(i, dataType, jobParameters, dataGenerator, id, acked, true);
        reduceWorkers.put(i, source);

        source.setOperation(reduce);

        StreamExecutor executor = new StreamExecutor(id, source, jobParameters);
        // the map thread where datacols is produced
        Thread mapThread = new Thread(executor);
        mapThread.start();
      }

      // we need to progress the communication
      while (true) {
        try {
          // progress the channel
          channel.progress();
          if (source != null) {
            startSendingTime = source.getStartSendingTime();
          }
        } catch (Throwable t) {
          t.printStackTrace();
        }
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  public class FinalReduceReceiver implements ReduceReceiver {
    Map<Integer, List<Long>> times = new HashMap<>();

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      LOG.log(Level.FINE, String.format("Initialize: %s", expectedIds));
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        times.put(e.getKey(), new ArrayList<>());
      }
    }

    @Override
    public boolean receive(int target, Object object) {
      if (dataType == DataType.INT_ARRAY) {
        int[] data = (int[]) object;
        for (int i = 0; i < data.length; i++) {
          if (data[i] != jobParameters.getTaskStages().get(0)) {
            LOG.info("SUM NOT EQUAL" + data[i]);
          }
        }
      }
      if (executorWithDest) {
        for (ExternalSource s : reduceWorkers.values()) {
          s.ack(0);
        }
      }
      return true;
    }
  }

  public class IdentityFunction implements ReduceFunction {
    int count = 0;
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public Object reduce(Object t1, Object t2) {
      count++;
      if (jobParameters.getPrintInterval() > 0) {
        if (count % jobParameters.getPrintInterval() == 0) {
          LOG.info(String.format("%d Identity function: %d", id, count));
        }
      }
      if (dataType == DataType.INT_ARRAY) {
        if (jobParameters.getPrintInterval() > 0) {
          if (count % jobParameters.getPrintInterval() == 0) {
            LOG.info(String.format("%d Reducing ints: %d", id, count));
          }
        }
        int[] data1 = (int[]) t1;
        int[] data2 = (int[]) t2;
        int[] data3 = new int[data1.length];
        for (int i = 0; i < data1.length; i++) {
          data3[i] = data1[i] + data2[i];
        }
        return data3;
      } else {
        return t1;
      }
    }
  }

  public class SendCompletion implements CompletionListener {
    @Override
    public void completed(int i) {
//      reduceWorkers.get(i).ack(i);
    }
  }
}