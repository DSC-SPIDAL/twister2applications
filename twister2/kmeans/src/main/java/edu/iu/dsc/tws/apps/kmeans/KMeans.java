package edu.iu.dsc.tws.apps.kmeans;

import edu.iu.dsc.tws.apps.kmeans.utils.JobParameters;
import edu.iu.dsc.tws.apps.kmeans.utils.Utils;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.*;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowAllReduce;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

public class KMeans implements IContainer {
  private static final Logger LOG = Logger.getLogger(KMeans.class.getName());

  private MPIDataFlowAllReduce reduceOperation;

  private int id;

  private JobParameters jobParameters;

  private Map<Integer, PipelinedTask> partitionSources = new HashMap<>();

  private Map<Integer, BlockingQueue<Message>> workerMessageQueue = new HashMap<>();

  private Map<Integer, Integer> sourcesToReceiveMapping = new HashMap<>();

  private Map<Integer, Executor> executors = new HashMap<>();

  @Override
  public void init(Config cfg, int containerId, ResourcePlan plan) {
    LOG.log(Level.FINE, "Starting the example with container id: " + plan.getThisId());

    this.jobParameters = JobParameters.build(cfg);
    this.id = containerId;

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

    int middle = jobParameters.getTaskStages().get(0) + jobParameters.getTaskStages().get(1);
    double[][] points = null;
    double[] centers = null;
    Set<Integer> mapTasksOfExecutor = Utils.getTasksOfExecutor(id, taskPlan, jobParameters.getTaskStages(), 0);
    Set<Integer> reduceTasksOfExecutor = Utils.getTasksOfExecutor(id, taskPlan, jobParameters.getTaskStages(), 1);
    int pointsPerTask = jobParameters.getNumPoints() / (jobParameters.getContainers() * mapTasksOfExecutor.size());

    try {
      points = PointReader.readPoints(jobParameters.getPointFile(), jobParameters.getNumPoints(),
          jobParameters.getContainers(), id, mapTasksOfExecutor.size(), jobParameters.getDimension());
      centers = PointReader.readClusters(jobParameters.getCenerFile(), jobParameters.getDimension(), jobParameters.getK());
    } catch (IOException e) {
      throw new RuntimeException("File read error", e);
    }

    Map<String, Object> newCfg = new HashMap<>();
    LOG.log(Level.FINE,"Setting up firstPartition dataflow operation");
    try {
      List<Integer> sourceTasksOfExecutor = new ArrayList<>(mapTasksOfExecutor);
      List<Integer> workerTasksOfExecutor = new ArrayList<>(reduceTasksOfExecutor);

      for (int k = 0; k < sourceTasksOfExecutor.size(); k++) {
        int sourceTask = sourceTasksOfExecutor.get(k);
        int workerTask = workerTasksOfExecutor.get(k);

        sourcesToReceiveMapping.put(sourceTask, workerTask);
      }

      for (int k = 0; k < sourceTasksOfExecutor.size(); k++) {
        int sourceTask = sourceTasksOfExecutor.get(k);

        PipelinedTask source = new PipelinedTask(points[k], centers, sourceTasksOfExecutor.get(k),
            jobParameters.getDimension(), jobParameters.getIterations(), pointsPerTask);
        partitionSources.put(sourceTask, source);
      }

      reduceOperation = (MPIDataFlowAllReduce) channel.allReduce(newCfg, MessageType.DOUBLE, 0, 1, sources,
          dests, middle, new IdentityFunction(), new FinalReduceReceiver(), true);

      for (int k = 0; k < sourceTasksOfExecutor.size(); k++) {
        int sourceTask = sourceTasksOfExecutor.get(k);

        PipelinedTask source = partitionSources.get(sourceTask);
        source.setAllReduce(reduceOperation);

        // the map thread where datacols is produced
        Executor executor = new Executor(source, workerMessageQueue.get(sourceTask), sourceTask);
        executors.put(sourceTask, executor);

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
        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(1);
        workerMessageQueue.put(e.getKey(), queue);
      }
    }

    @Override
    public boolean receive(int i, Object o) {
      Queue<Message> messageQueue = workerMessageQueue.get(i);
      return messageQueue.offer(new Message(i, 0, o));
    }
  }

  public static class IdentityFunction implements ReduceFunction {
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public Object reduce(Object t1, Object t2) {
      double[] data1 = (double[]) t1;
      double[] data2 = (double[]) t2;
      double[] data3 = new double[data1.length];
      for (int i = 0; i < data1.length; i++) {
        data3[i] = data1[i] + data2[i];
      }
      return data3;
    }
  }

  private static void resetCenterSumsAndCounts(double[] centerSumsAndCountsForThread) {
    IntStream.range(0, centerSumsAndCountsForThread.length).forEach(i -> centerSumsAndCountsForThread[i] = 0.0);
  }
}
