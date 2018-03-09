package edu.iu.dsc.tws.apps.storm;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.data.PartitionData;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.apps.utils.Utils;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.*;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.MPIContext;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowBroadcast;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowReduce;
import edu.iu.dsc.tws.comms.mpi.RoutingParameters;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReduceStreamDebug implements IContainer {
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
          new ReduceStreamingPartialReceiver(dest1, new IdentityFunction()));
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
        LOG.log(Level.SEVERE, "Error", new RuntimeException("Data 1 and data2 not equal" + data1.getId() + " " + data2.getId()));
      }
      return t1;
    }
  }

  public class ReduceStreamingFinalReceiver extends ReduceStreamingReceiver {
    private ReduceReceiver reduceReceiver;

    public ReduceStreamingFinalReceiver(ReduceFunction function, ReduceReceiver receiver) {
      super(function);
      this.reduceReceiver = receiver;
    }

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      super.init(cfg, op, expectedIds);
      this.reduceReceiver.init(cfg, op, expectedIds);
    }

    @Override
    public boolean handleMessage(int source, Object message, int flags, int dest) {
      return reduceReceiver.receive(source, message);
    }
  }

  public class ReduceStreamingPartialReceiver extends ReduceStreamingReceiver {
    public ReduceStreamingPartialReceiver(int dst, ReduceFunction function) {
      super(dst, function);
      this.reduceFunction = function;
    }

    @Override
    public boolean handleMessage(int source, Object message, int flags, int dest) {
      return this.operation.sendPartial(source, message, flags, dest);
    }
  }

  public abstract class ReduceStreamingReceiver implements MessageReceiver {
    protected ReduceFunction reduceFunction;
    // lets keep track of the messages
    // for each task we need to keep track of incoming messages
    protected Map<Integer, Map<Integer, Queue<Object>>> messages = new HashMap<>();
    protected Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();
    protected int executor;
    protected int count = 0;
    protected DataFlowOperation operation;
    protected int sendPendingMax = 128;
    protected int destination;
    protected Queue<Object> reducedValues;
    private int onMessageAttempts = 0;
    protected Map<Integer, Map<Integer, Integer>> totalCounts = new HashMap<>();
    private Table<Integer, Integer, Integer> sequences
        = HashBasedTable.create();

    public ReduceStreamingReceiver(ReduceFunction function) {
      this(0, function);
    }

    public ReduceStreamingReceiver(int dst, ReduceFunction function) {
      this.reduceFunction = function;
      this.destination = dst;
    }

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      this.executor = op.getTaskPlan().getThisExecutor();
      this.operation = op;
      this.sendPendingMax = MPIContext.sendPendingMax(cfg);
      this.reducedValues = new ArrayBlockingQueue<>(sendPendingMax);
      LOG.log(Level.INFO, String.format("%d Sending max %d", executor, sendPendingMax));
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        Map<Integer, Queue<Object>> messagesPerTask = new HashMap<>();
        Map<Integer, Integer> countsPerTask = new HashMap<>();
        Map<Integer, Integer> totalCountsPerTask = new HashMap<>();

        for (int i : e.getValue()) {
          messagesPerTask.put(i, new ArrayBlockingQueue<>(sendPendingMax));
          countsPerTask.put(i, 0);
          totalCountsPerTask.put(i, 0);
          sequences.put(e.getKey(), i, 0);
        }

        LOG.fine(String.format("%d Final Task %d receives from %s",
            executor, e.getKey(), e.getValue().toString()));

        messages.put(e.getKey(), messagesPerTask);
        counts.put(e.getKey(), countsPerTask);
        totalCounts.put(e.getKey(), totalCountsPerTask);
      }
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      // add the object to the map
      boolean canAdd = true;
      Queue<Object> m = messages.get(target).get(source);
      Integer c = counts.get(target).get(source);
      if (m.size() >= sendPendingMax) {
        canAdd = false;
        //LOG.info(String.format("%d ADD FALSE %d %d", executor, m.size(), counts.get(target).get(source)));
        onMessageAttempts++;
      } else {
        int seq = sequences.get(target, source);
        PartitionData d = (PartitionData) object;
        if (seq != d.getId()) {
          LOG.log(Level.SEVERE, "ERROR", new RuntimeException(String.format("%d %d %d out of order element %d %d", executor, target, source, seq, d.getId())));
        }
        sequences.put(target, source, seq + 1);
        //LOG.info(String.format("%d ADD TRUE %d %d", executor, m.size(), counts.get(target).get(source)));
        onMessageAttempts = 0;
        if (!m.offer(object)) {
          throw new RuntimeException("Failed to offer");
        }
        counts.get(target).put(source, c + 1);

        Integer tc = totalCounts.get(target).get(source);
        totalCounts.get(target).put(source, tc + 1);
      }

      return canAdd;
    }

    private int progressAttempts = 0;

    @Override
    public void progress() {
      for (int t : messages.keySet()) {
        boolean canProgress = true;
        // now check weather we have the messages for this source
        Map<Integer, Queue<Object>> messagePerTarget = messages.get(t);
        Map<Integer, Integer> countsPerTarget = counts.get(t);
        Map<Integer, Integer> totalCountMap = totalCounts.get(t);
//      if (onMessageAttempts > 1000000 || progressAttempts > 1000000) {
//        LOG.info(String.format("%d REDUCE %s %s", executor, counts, totalCountMap));
//      }
//      LOG.info(String.format("%d REDUCE %s %s", executor, counts, totalCountMap));

        while (canProgress) {
          boolean found = true;
          for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
            if (e.getValue().size() == 0) {
              found = false;
              canProgress = false;
            }
          }
          if (found && reducedValues.size() < sendPendingMax) {
            Object previous = null;
            for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
              if (previous == null) {
                previous = e.getValue().poll();
              } else {
                Object current = e.getValue().poll();
                previous = reduce(previous, current, t);
              }
            }
            if (previous != null) {
              reducedValues.offer(previous);
            }
            progressAttempts = 0;
          } else {
            progressAttempts++;
          }

          if (reducedValues.size() > 0) {
            Object previous = reducedValues.peek();
            boolean handle = handleMessage(t, previous, 0, destination);
            if (handle) {
              reducedValues.poll();
              for (Map.Entry<Integer, Integer> e : countsPerTarget.entrySet()) {
                Integer i = e.getValue();
                countsPerTarget.put(e.getKey(), i - 1);
              }
            } else {
              canProgress = false;
            }
          }
        }
      }
    }

    public Object reduce(Object t1, Object t2, int t) {
      PartitionData data1 = (PartitionData) t1;
      PartitionData data2 = (PartitionData) t2;

      if (data1.getId() != data2.getId()) {
        LOG.log(Level.SEVERE, "Error", new RuntimeException(String.format("%d %d NOT equal %d %d %s %s", executor, t, data1.getId(), data2.getId(), counts.get(t), totalCounts.get(t))));
      } else {
//        LOG.log(Level.INFO, String.format("%d %d EQUAL %d %d %s %s", executor, t, data1.getId(), data2.getId(), counts.get(t), totalCounts.get(t)));
      }
      return t1;
    }

    public abstract boolean handleMessage(int source, Object message, int flags, int dest);
  }
}


