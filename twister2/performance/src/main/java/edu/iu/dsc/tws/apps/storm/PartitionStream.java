package edu.iu.dsc.tws.apps.storm;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.data.DataSave;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.apps.utils.Utils;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.*;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PartitionStream implements IContainer {
  private static final Logger LOG = Logger.getLogger(edu.iu.dsc.tws.apps.stream.ReduceStream.class.getName());
  private DataFlowOperation firstPartition;

  private ResourcePlan resourcePlan;

  private int id;

  private Config config;

  private JobParameters jobParameters;

  private long startSendingTime;

  private Map<Integer, PartitionSource> partitionSources = new HashMap<>();

  private List<Integer> tasksOfThisExec;

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

    LOG.log(Level.FINE,"Setting up firstPartition dataflow operation");
    try {
      // this method calls the init method
      // I think this is wrong
      firstPartition = channel.partition(newCfg, MessageType.OBJECT, 0, sources,
          dests, new FinalReduceReceiver());

      Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(id, taskPlan, jobParameters.getTaskStages(), 0);
      tasksOfThisExec = new ArrayList<>(tasksOfExecutor);
      PartitionSource source = null;
      for (int i : tasksOfExecutor) {
        source = new PartitionSource(i, jobParameters, firstPartition, dataGenerator);
        partitionSources.put(i, source);
        // the map thread where datacols is produced
        Thread mapThread = new Thread(new Worker(source));
        mapThread.start();
      }

      // we need to progress the communication
      while (true) {
        try {
          // progress the channel
          channel.progress();
          // we should progress the communication directive
          firstPartition.progress();

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

  public class FinalReduceReceiver implements MessageReceiver {
    Map<Integer, List<Long>> times = new HashMap<>();

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      LOG.log(Level.FINE, String.format("Initialize: %s", expectedIds));
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        times.put(e.getKey(), new ArrayList<>());
      }
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      long time = (System.currentTimeMillis() - startSendingTime);
      List<Long> timesForTarget = times.get(target);
      timesForTarget.add(System.nanoTime());

      try {
        if (timesForTarget.size() >= jobParameters.getIterations()) {
          List<Long> times = partitionSources.get(tasksOfThisExec.get(0)).getStartOfMessages();
          List<Long> latencies = new ArrayList<>();
          long average = 0;
          for (int i = 0; i < times.size(); i++) {
            average += (timesForTarget.get(i) - times.get(i));
            latencies.add(timesForTarget.get(i) - times.get(i));
          }
          LOG.info(String.format("%d Average: %d", id, average / (times.size())));
          LOG.info(String.format("%d Finished %d %d", id, target, time));

          DataSave.saveList("firstPartition", latencies);
        }
      } catch (Throwable r) {
        LOG.log(Level.SEVERE, String.format("%d excpetion %s %s", id, tasksOfThisExec, partitionSources.keySet()), r);
      }

      return true;
    }

    @Override
    public void progress() {

    }
  }
}
