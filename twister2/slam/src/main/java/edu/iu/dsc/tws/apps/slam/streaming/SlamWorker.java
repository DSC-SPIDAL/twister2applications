package edu.iu.dsc.tws.apps.slam.streaming;

import edu.iu.dsc.tws.apps.slam.streaming.msgs.Ready;
import edu.iu.dsc.tws.apps.slam.streaming.ops.BCastOperation;
import edu.iu.dsc.tws.apps.slam.streaming.ops.BarrierOperation;
import edu.iu.dsc.tws.apps.slam.streaming.ops.GatherOperation;
import edu.iu.dsc.tws.apps.slam.streaming.ops.ScatterOperation;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigReader;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowBroadcast;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowPartition;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class SlamWorker implements IContainer {
  private static final Logger LOG = Logger.getLogger(SlamWorker.class.getName());
  private MPIDataFlowBroadcast broadcast;
  private MPIDataFlowPartition partition;
  private MPIDataFlowPartition readyPartition;

  private BlockingQueue<Tuple> broadcastQueue = new ArrayBlockingQueue<>(64);
  private BlockingQueue<byte []> partitionQueue = new ArrayBlockingQueue<>(64);
  private BlockingQueue<byte []> mapQueue = new ArrayBlockingQueue<>(64);

  private GatherOperation gatherOperation;
  private ScatterOperation scatterOperation;
  private BCastOperation bCastOperation;
  private BarrierOperation barrierOperation;

  @Override
  public void init(Config config, int containerId, ResourcePlan resourcePlan) {
    // lets read the configuration file
    String configFile = config.getStringValue(Constants.CONFIG_FILE);
    Map slamConf = ConfigReader.loadFile(configFile);
    int parallel = Integer.parseInt(config.getStringValue(Constants.ARGS_PARALLEL));
    int particles = Integer.parseInt(config.getStringValue(Constants.ARGS_PARTICLES));
    String inputFile = config.getStringValue(Constants.INPUT_FILE);

    // lets create two communicators
    try {
      int rank = MPI.COMM_WORLD.getRank();
      int worldSize = MPI.COMM_WORLD.getSize();
      int color = rank == (worldSize - 1) ? 0 : 1;

      Intracomm scanMatchComm = MPI.COMM_WORLD.split(color, rank);
      LOG.info(String.format("Word rank %d com rank %d", rank, scanMatchComm.getRank()));

      int dispatchTask = parallel * 2;
      Set<Integer> scanMatcherTasks = new HashSet<>();
      Set<Integer> partitionTasks = new HashSet<>();
      Set<Integer> dispatcherTasks = new HashSet<>();
      dispatcherTasks.add(dispatchTask);
      for (int i = 0; i < parallel; i++) {
        scanMatcherTasks.add(i);
        partitionTasks.add(i + parallel);
      }

      // lets create the taskplan
      TaskPlan taskPlan = Utils.createReduceTaskPlan(config, resourcePlan, parallel);
      TWSNetwork network = new TWSNetwork(config, taskPlan);
      TWSCommunication channel = network.getDataFlowTWSCommunication();

      gatherOperation = new GatherOperation(scanMatchComm, new Serializer());
      scatterOperation = new ScatterOperation(scanMatchComm, new Serializer());
      bCastOperation = new BCastOperation(scanMatchComm, new Serializer());
      barrierOperation = new BarrierOperation(scanMatchComm, new Serializer());

      ScanMatchTask scanMatchTask = null;
      DispatcherTask dispatcherBolt = null;
      if (resourcePlan.getThisId() != resourcePlan.noOfContainers() - 1) {
        int thisTask = resourcePlan.getThisId();
        // lets create scanmatcher
        scanMatchTask = new ScanMatchTask();
        scanMatchTask.prepare(slamConf, scanMatchComm, thisTask, parallel, dispatchTask,
            gatherOperation, scatterOperation, bCastOperation, barrierOperation);
      } else {
        // lets use the dispatch bolt here
        dispatcherBolt = new DispatcherTask();
        // todo
        dispatcherBolt.prepare(slamConf, MPI.COMM_WORLD, inputFile,  dispatchTask);
      }

      broadcast = (MPIDataFlowBroadcast) channel.broadCast(new HashMap<>(), MessageType.OBJECT, 100, dispatchTask,
          scanMatcherTasks, new BCastMessageReceiver(scanMatchTask, broadcastQueue));
      if (dispatcherBolt != null) {
        dispatcherBolt.setBroadcast(broadcast);
      }

      partition = (MPIDataFlowPartition) channel.partition(new HashMap<>(), MessageType.OBJECT, 200,
          scanMatcherTasks, partitionTasks, new MapReceiver(scanMatchTask, mapQueue));
      if (scanMatchTask != null) {
        scanMatchTask.setPartition(partition);
      }

      readyPartition = (MPIDataFlowPartition) channel.partition(new HashMap<>(), MessageType.OBJECT, 300,
          scanMatcherTasks, dispatcherTasks, new ReadyReceiver(dispatcherBolt));
      if (scanMatchTask != null) {
        scanMatchTask.setReadyPartition(readyPartition);
      }

      if (resourcePlan.getThisId() != resourcePlan.noOfContainers() - 1) {
        Thread t = new Thread(new ScanMatchWorker(broadcast, partition, scanMatchTask, broadcastQueue, mapQueue));
        t.start();
      } else {
        Thread t = new Thread(new DispatchWorker(dispatcherBolt, broadcast));
        t.start();
      }

      while (true) {
        gatherOperation.op();
        scatterOperation.op();
        bCastOperation.op();
        barrierOperation.op();

        readyPartition.progress();
        broadcast.progress();
        partition.progress();
        channel.progress();
      }
    } catch (MPIException e) {
      throw new RuntimeException("Error", e);
    }
  }

  private static class DispatchWorker implements Runnable {
    DispatcherTask dispatcherTask;
    MPIDataFlowBroadcast broadcast;

    public DispatchWorker(DispatcherTask dispatcherTask, MPIDataFlowBroadcast broadcast) {
      this.dispatcherTask = dispatcherTask;
      this.broadcast = broadcast;
    }

    @Override
    public void run() {
      while (true) {
        dispatcherTask.progress();
        broadcast.progress();
      }
    }
  }

  private static class ScanMatchWorker implements Runnable {
    private MPIDataFlowBroadcast broadcast;
    private MPIDataFlowPartition partition;
    private ScanMatchTask scanMatchTask;
    private BlockingQueue<Tuple> broadcastQueue;
    private BlockingQueue<byte []> mapQueue;

    public ScanMatchWorker(MPIDataFlowBroadcast broadcast, MPIDataFlowPartition partition, ScanMatchTask scanMatchTask,
                           BlockingQueue<Tuple> broadcastQueue, BlockingQueue<byte []> mapQueue) {
      this.broadcast = broadcast;
      this.partition = partition;
      this.scanMatchTask = scanMatchTask;
      this.broadcastQueue = broadcastQueue;
      this.mapQueue = mapQueue;
    }

    @Override
    public void run() {
      while (true) {
        Tuple t = broadcastQueue.poll();
        if (t != null) {
          scanMatchTask.execute(t);
        }

        byte[] map = mapQueue.poll();
        if (map != null) {
          scanMatchTask.onMap(map);
        }

        scanMatchTask.progress();
        broadcast.progress();
        partition.progress();
      }
    }
  }

  private static class BCastMessageReceiver implements MessageReceiver {
    private ScanMatchTask scanMatchTask;
    BlockingQueue<Tuple> broadcastQueue;

    public BCastMessageReceiver(ScanMatchTask scanMatchTask, BlockingQueue<Tuple> broadcastQueue) {
      this.scanMatchTask = scanMatchTask;
      this.broadcastQueue = broadcastQueue;
    }

    @Override
    public void init(Config config, DataFlowOperation dataFlowOperation, Map<Integer, List<Integer>> map) {

    }

    @Override
    public boolean onMessage(int i, int i1, int i2, int i3, Object o) {
//      LOG.info(String.format("Bcast receive %d %d %d %d", i, i1, i2, i3));
      if (!(o instanceof Tuple)) {
        throw new RuntimeException("Un-expected object");
      }
      broadcastQueue.offer((Tuple) o);
      return true;
    }

    @Override
    public void progress() {
    }
  }

  private static class MapReceiver implements MessageReceiver {
    private ScanMatchTask scanMatchTask;
    private BlockingQueue<byte []> mapQueue;

    public MapReceiver(ScanMatchTask scanMatchTask, BlockingQueue<byte []> mapQueue) {
      this.scanMatchTask = scanMatchTask;
      this.mapQueue = mapQueue;
    }

    @Override
    public void init(Config config, DataFlowOperation dataFlowOperation, Map<Integer, List<Integer>> map) {

    }

    @Override
    public boolean onMessage(int i, int i1, int i2, int i3, Object o) {
      if (!(o instanceof byte[])) {
        throw new RuntimeException("Un-expected object");
      }
      mapQueue.offer((byte[]) o);
      return true;
    }

    @Override
    public void progress() {
    }
  }

  private static class ReadyReceiver implements MessageReceiver {
    private DispatcherTask dispatcherTask;

    public ReadyReceiver(DispatcherTask dispatcherTask) {
      this.dispatcherTask = dispatcherTask;
    }

    @Override
    public void init(Config config, DataFlowOperation dataFlowOperation, Map<Integer, List<Integer>> map) {
    }

    @Override
    public boolean onMessage(int i, int i1, int i2, int i3, Object o) {
      dispatcherTask.handleReady((Ready) o);
      return true;
    }

    @Override
    public void progress() {

    }
  }
}
