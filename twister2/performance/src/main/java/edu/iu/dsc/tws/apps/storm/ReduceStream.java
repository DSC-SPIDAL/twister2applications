package edu.iu.dsc.tws.apps.storm;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.data.PartitionData;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.apps.utils.Utils;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.*;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowBroadcast;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowReduce;
import edu.iu.dsc.tws.comms.mpi.io.reduce.ReduceStreamingFinalReceiver;
import edu.iu.dsc.tws.comms.mpi.io.reduce.ReduceStreamingPartialReceiver;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReduceStream implements IContainer {
  private static final Logger LOG = Logger.getLogger(edu.iu.dsc.tws.apps.stream.ReduceStream.class.getName());
  private MPIDataFlowReduce reduceOperation;

  private MPIDataFlowBroadcast broadcast;
  private int id;
  private JobParameters jobParameters;

  private Map<Integer, PartitionSource> partitionSources = new HashMap<>();
  private Map<Integer, SecondBolt> workerTasks = new HashMap<>();

  private Map<Integer, ReduceExecutor> executors = new HashMap<>();

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
    int dest1 = sources.size();

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

      reduceOperation = (MPIDataFlowReduce) channel.reduce(newCfg, MessageType.OBJECT, 0, sources,
          dest1, new ReduceStreamingFinalReceiver(new IdentityFunction(), new FinalReduceReceiver()),
          new ReduceStreamingPartialReceiver(dest1, new edu.iu.dsc.tws.apps.stream.ReduceStream.IdentityFunction()));
      broadcast = (MPIDataFlowBroadcast) channel.broadCast(newCfg, MessageType.OBJECT,
          1, dest1, secondDests, new AckReduceReceiver());

      for (int k = 0; k < sourceTasksOfExecutor.size(); k++) {
        int sourceTask = sourceTasksOfExecutor.get(k);
        int workerTask = workerTasksOfExecutor.get(k);
        int ackTask = ackTasksOfExecutor.get(k);

        PartitionSource source = partitionSources.get(sourceTask);
        source.setOperation(reduceOperation);

        SecondBolt secondBolt = workerTasks.get(workerTask);
        secondBolt.setOperation(broadcast);

        int reduceReceiveExecutor = taskPlan.getExecutorForChannel(dest1);
        boolean work = false;
        if (reduceReceiveExecutor == id && workerTask == dest1) {
          work = true;
        }

        // the map thread where datacols is produced
        ReduceExecutor executor = new ReduceExecutor(id, work, true, source, secondBolt, jobParameters);
        executors.put(sourceTask, executor);

        int targetTasks = sourcesToReceiveMapping.get(sourceTask);
        int acTask = sourceToAckMapping.get(sourceTask);

        executors.get(sourceTask).addWorkerQueue(workerMessageQueue.get(targetTasks));
        executors.get(sourceTask).setAckMessages(ackMessageQueue.get(acTask));

        Thread mapThread = new Thread(executor);
        mapThread.start();
      }

      LOG.fine(String.format("%d source to receive %s", id, sourcesToReceiveMapping));

      // we need to progress the communication
      while (true) {
        try {
          // progress the channel
          channel.progress();
          reduceOperation.progress();
          broadcast.progress();
        } catch (Throwable t) {
          t.printStackTrace();
        }
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  public class FinalReduceReceiver implements ReduceReceiver {
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      LOG.log(Level.FINE, String.format("%d Initialize worker: %s", id, expectedIds));
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        Queue<Message> queue = new ArrayBlockingQueue<>(1);
        workerMessageQueue.put(e.getKey(), queue);
      }
    }

    @Override
    public boolean receive(int i, Object o) {
//      LOG.log(Level.INFO, String.format("%d Received msg: target %d", id, i));
      Queue<Message> messageQueue = workerMessageQueue.get(i);
      return messageQueue.offer(new Message(i, 0, o));
    }
  }

  public class AckReduceReceiver implements MessageReceiver {
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      LOG.log(Level.FINE, String.format("%d Initialize ack: %s", id, expectedIds));
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        Queue<Message> queue = new ArrayBlockingQueue<>(1);
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

  public static class IdentityFunction implements ReduceFunction {
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public Object reduce(Object t1, Object t2) {
      PartitionData data1 = (PartitionData) t1;
      PartitionData data2 = (PartitionData) t2;

      if (data1.getId() != data2.getId()) {
        throw new RuntimeException("Data 1 and data2 not equal" + data1.getId() + " " + data2.getId());
      }
      return t1;
    }
  }
}
