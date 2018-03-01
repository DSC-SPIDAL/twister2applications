package edu.iu.dsc.tws.apps.storm;

import edu.iu.dsc.tws.apps.data.DataGenerator;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PartitionStream implements IContainer {
  private static final Logger LOG = Logger.getLogger(PartitionStream.class.getName());
  private DataFlowOperation firstPartition;

  private int id;

  private JobParameters jobParameters;

  private Map<Integer, PartitionSource> partitionSources = new HashMap<>();
  private Map<Integer, Worker> partitionWorkers = new HashMap<>();

  private Map<Integer, Queue<Message>> messageQueue = new HashMap<>();

  private Map<Integer, Integer> sourcesToReceiveMapping = new HashMap<>();

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
    Set<Integer> dests = new HashSet<>();
    int noOfDestTasks = jobParameters.getTaskStages().get(1);
    for (int i = 0; i < noOfDestTasks; i++) {
      dests.add(i + sources.size());
    }

    Map<String, Object> newCfg = new HashMap<>();

    LOG.log(Level.FINE,"Setting up firstPartition dataflow operation");
    try {

      List<Integer> sourceTasksOfExecutor = new ArrayList<>(Utils.getTasksOfExecutor(id, taskPlan, jobParameters.getTaskStages(), 0));
      List<Integer> receiveTasksOfExecutor = new ArrayList<>(Utils.getTasksOfExecutor(id, taskPlan, jobParameters.getTaskStages(), 1));
      for (int k = 0; k < sourceTasksOfExecutor.size(); k++) {
        int i = sourceTasksOfExecutor.get(k);
        int receiveTask = receiveTasksOfExecutor.get(k);

        sourcesToReceiveMapping.put(i, receiveTask);

        PartitionSource source = new PartitionSource(i, jobParameters, dataGenerator, id);
        partitionSources.put(i, source);
      }
      firstPartition = channel.partition(newCfg, MessageType.OBJECT, 0, sources,
          dests, new FinalReduceReceiver());


      for (int k = 0; k < sourceTasksOfExecutor.size(); k++) {
        int i = sourceTasksOfExecutor.get(k);
        PartitionSource source = partitionSources.get(i);
        source.setOperation(firstPartition);
        // the map thread where datacols is produced
        Worker worker = new Worker(id, source, jobParameters);
        partitionWorkers.put(i, worker);

        Thread mapThread = new Thread(worker);
        mapThread.start();

        int targetTasks = sourcesToReceiveMapping.get(i);
        partitionWorkers.get(i).addQueue(targetTasks, messageQueue.get(targetTasks));
      }

      LOG.fine(String.format("%d source to receive %s", id, sourcesToReceiveMapping));

      // we need to progress the communication
      while (true) {
        try {
          // progress the channel
          channel.progress();
          // we should progress the communication directive
          firstPartition.progress();
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
      LOG.log(Level.FINE, String.format("%d Initialize: %s", id, expectedIds));
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        times.put(e.getKey(), new ArrayList<>());
        Queue<Message> queue = new ArrayBlockingQueue<>(1024);
        messageQueue.put(e.getKey(), queue);
      }
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      return messageQueue.get(target).offer(new Message(target, 0, object));
    }

    @Override
    public void progress() {

    }
  }
}
