package edu.iu.dsc.tws.apps.storm;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.apps.utils.Utils;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.*;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowPartition;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PartitionStream implements IContainer {
  private static final Logger LOG = Logger.getLogger(PartitionStream.class.getName());
  private MPIDataFlowPartition firstPartition;

  private MPIDataFlowPartition secondPartition;

  private int id;

  private JobParameters jobParameters;

  private Map<Integer, PartitionSource> partitionSources = new HashMap<>();
  private Map<Integer, SecondBolt> workerTasks = new HashMap<>();

  private Map<Integer, Executor> partitionWorkers = new HashMap<>();

  private Map<Integer, Queue<Message>> workerMessageQueue = new HashMap<>();
  private Map<Integer, Queue<Message>> ackMessageQueue = new HashMap<>();

  private Map<Integer, Integer> sourcesToReceiveMapping = new HashMap<>();
  private Map<Integer, Integer> sourceToAckMapping = new HashMap<>();
  private Map<Integer, Integer> completeSourceToAckMapping = new HashMap<>();

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

    Set<Integer> secondDests = new HashSet<>();
    int start = sources.size() + dests.size();
    int secondDestsTasks = jobParameters.getTaskStages().get(2);
    for (int i = 0; i < secondDestsTasks; i++) {
      secondDests.add(i + start);
    }

    Map<String, Object> newCfg = new HashMap<>();

    LOG.log(Level.FINE,"Setting up firstPartition dataflow operation");
    try {
      for (ResourceContainer c : plan.getContainers()) {
        List<Integer> sourceTasksOfExecutor = new ArrayList<>(Utils.getTasksOfExecutor(c.getId(), taskPlan, jobParameters.getTaskStages(), 0));
        List<Integer> workerTasksOfExecutor = new ArrayList<>(Utils.getTasksOfExecutor(c.getId(), taskPlan, jobParameters.getTaskStages(), 1));
        List<Integer> ackTasksOfExecutor = new ArrayList<>(Utils.getTasksOfExecutor(c.getId(), taskPlan, jobParameters.getTaskStages(), 2));

        for (int k = 0; k < sourceTasksOfExecutor.size(); k++) {
          int sourceTask = sourceTasksOfExecutor.get(k);
          int workerTask = workerTasksOfExecutor.get(k);
          int ackTask = ackTasksOfExecutor.get(k);

          completeSourceToAckMapping.put(sourceTask, ackTask);
        }
      }

      List<Integer> sourceTasksOfExecutor = new ArrayList<>(Utils.getTasksOfExecutor(id, taskPlan, jobParameters.getTaskStages(), 0));
      List<Integer> workerTasksOfExecutor = new ArrayList<>(Utils.getTasksOfExecutor(id, taskPlan, jobParameters.getTaskStages(), 1));
      List<Integer> ackTasksOfExecutor = new ArrayList<>(Utils.getTasksOfExecutor(id, taskPlan, jobParameters.getTaskStages(), 2));

      for (int k = 0; k < sourceTasksOfExecutor.size(); k++) {
        int sourceTask = sourceTasksOfExecutor.get(k);
        int workerTask = workerTasksOfExecutor.get(k);
        int ackTask = ackTasksOfExecutor.get(k);

        sourcesToReceiveMapping.put(sourceTask, workerTask);
        sourceToAckMapping.put(sourceTask, ackTask);
      }

      for (int k = 0; k < sourceTasksOfExecutor.size(); k++) {
        int sourceTask = sourceTasksOfExecutor.get(k);
        int workerTask = workerTasksOfExecutor.get(k);

        PartitionSource source = new PartitionSource(sourceTask, jobParameters, dataGenerator, id);
        partitionSources.put(sourceTask, source);

        SecondBolt secondBolt = new SecondBolt(workerTask, jobParameters, id, completeSourceToAckMapping);
        workerTasks.put(workerTask, secondBolt);
      }

      firstPartition = (MPIDataFlowPartition) channel.partition(newCfg, MessageType.OBJECT,
          0, sources, dests, new FinalReduceReceiver());
      secondPartition = (MPIDataFlowPartition) channel.partition(newCfg, MessageType.OBJECT,
          1, dests, secondDests, new AckReduceReceiver());

      for (int k = 0; k < sourceTasksOfExecutor.size(); k++) {
        int sourceTask = sourceTasksOfExecutor.get(k);
        int workerTask = workerTasksOfExecutor.get(k);

        PartitionSource source = partitionSources.get(sourceTask);
        source.setOperation(firstPartition);

        SecondBolt secondBolt = workerTasks.get(workerTask);
        secondBolt.setOperation(secondPartition);

        // the map thread where datacols is produced
        Executor executor = new Executor(id, source, secondBolt, jobParameters);
        partitionWorkers.put(sourceTask, executor);

        int targetTasks = sourcesToReceiveMapping.get(sourceTask);
        int acTask = sourceToAckMapping.get(sourceTask);

        partitionWorkers.get(sourceTask).addWorkerQueue(workerMessageQueue.get(targetTasks));
        partitionWorkers.get(sourceTask).setAckMessages(ackMessageQueue.get(acTask));

        Thread mapThread = new Thread(executor);
        mapThread.start();
      }

      LOG.fine(String.format("%d source to receive %s", id, sourcesToReceiveMapping));

      // we need to progress the communication
      while (true) {
        try {
          // progress the channel
//          for (int i = 0; i < 10; i++) {
            channel.progress();
//          }
//          for (int i = 0; i < 1; i++) {
            // we should progress the communication directive
            firstPartition.progress();
            secondPartition.progress();
//          }
        } catch (Throwable t) {
          t.printStackTrace();
        }
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  public class FinalReduceReceiver implements MessageReceiver {
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      LOG.log(Level.FINE, String.format("%d Initialize worker: %s", id, expectedIds));
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        Queue<Message> queue = new ArrayBlockingQueue<>(2);
        workerMessageQueue.put(e.getKey(), queue);
      }
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
//      LOG.log(Level.INFO, String.format("%d Received worker: source %d target %d", id, source, target));
      Queue<Message> messageQueue = workerMessageQueue.get(target);
      return messageQueue.offer(new Message(target, source, object));
    }

    @Override
    public void progress() {
    }
  }

  public class AckReduceReceiver implements MessageReceiver {
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      LOG.log(Level.FINE, String.format("%d Initialize ack: %s", id, expectedIds));
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        Queue<Message> queue = new ArrayBlockingQueue<>(2);
        ackMessageQueue.put(e.getKey(), queue);
      }
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
//      LOG.log(Level.INFO, String.format("%d Received ack: source %d target %d", id, source, target));
      Queue<Message> messageQueue = ackMessageQueue.get(target);
      return messageQueue.offer(new Message(target, source, object));
    }

    @Override
    public void progress() {
    }
  }
}
