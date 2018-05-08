package edu.iu.dsc.tws.apps.slam.streaming;

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

import java.util.*;

public class SlamWorker implements IContainer {
  private MPIDataFlowBroadcast broadcast;
  private MPIDataFlowPartition partition;

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
      int color = rank == 0 ? 0 : 1;

      Intracomm scanMatchComm = MPI.COMM_WORLD.split(color, rank);

      int dispatchTask = parallel * 2;
      Set<Integer> scanMatcherTasks = new HashSet<>();
      Set<Integer> partitionTasks = new HashSet<>();
      for (int i = 0; i < parallel; i++) {
        scanMatcherTasks.add(i);
        partitionTasks.add(i + parallel);
      }

      // lets create the taskplan
      TaskPlan taskPlan = Utils.createReduceTaskPlan(config, resourcePlan, parallel);
      TWSNetwork network = new TWSNetwork(config, taskPlan);
      TWSCommunication channel = network.getDataFlowTWSCommunication();

      ScanMatchTask scanMatchTask = null;
      DispatcherTask dispatcherBolt = null;
      if (resourcePlan.getThisId() != resourcePlan.noOfContainers() - 1) {
        int thisTask = resourcePlan.getThisId();
        // lets create scanmatcher
        scanMatchTask = new ScanMatchTask();
        scanMatchTask.prepare(slamConf, scanMatchComm, thisTask, parallel);
      } else {
        // lets use the dispatch bolt here
        dispatcherBolt = new DispatcherTask();
        // todo
        dispatcherBolt.prepare(slamConf, MPI.COMM_WORLD, inputFile,  dispatchTask);
      }

      broadcast = (MPIDataFlowBroadcast) channel.broadCast(new HashMap<>(), MessageType.OBJECT, 100, dispatchTask,
          scanMatcherTasks, new BCastMessageReceiver(scanMatchTask));
      if (dispatcherBolt != null) {
        dispatcherBolt.setBroadcast(broadcast);
      }

      partition = (MPIDataFlowPartition) channel.partition(new HashMap<>(), MessageType.OBJECT, 200,
          scanMatcherTasks, partitionTasks, new MapReceiver(scanMatchTask));
      if (scanMatchTask != null) {
        scanMatchTask.setPartition(partition);
      }

      while (true) {
        if (resourcePlan.getThisId() != resourcePlan.noOfContainers() - 1) {
          // lets create scanmatcher
          if (scanMatchTask != null) {
            scanMatchTask.progress();
          }
        } else {
          // lets use the dispatch bolt here
          if (dispatcherBolt != null) {
            dispatcherBolt.progress();
          }
        }
        broadcast.progress();
        partition.progress();
        channel.progress();
      }
    } catch (MPIException e) {
      throw new RuntimeException("Error", e);
    }
  }

  private static class BCastMessageReceiver implements MessageReceiver {
    private ScanMatchTask scanMatchTask;

    public BCastMessageReceiver(ScanMatchTask scanMatchTask) {
      this.scanMatchTask = scanMatchTask;
    }

    @Override
    public void init(Config config, DataFlowOperation dataFlowOperation, Map<Integer, List<Integer>> map) {

    }

    @Override
    public boolean onMessage(int i, int i1, int i2, int i3, Object o) {
      if (!(o instanceof Tuple)) {
        throw new RuntimeException("Un-expected object");
      }
      scanMatchTask.execute((Tuple) o);
      return true;
    }

    @Override
    public void progress() {

    }
  }

  private static class MapReceiver implements MessageReceiver {
    private ScanMatchTask scanMatchTask;

    public MapReceiver(ScanMatchTask scanMatchTask) {
      this.scanMatchTask = scanMatchTask;
    }

    @Override
    public void init(Config config, DataFlowOperation dataFlowOperation, Map<Integer, List<Integer>> map) {

    }

    @Override
    public boolean onMessage(int i, int i1, int i2, int i3, Object o) {
      if (!(o instanceof byte[])) {
        throw new RuntimeException("Un-expected object");
      }
      scanMatchTask.onMap((byte[]) o);
      return true;
    }

    @Override
    public void progress() {

    }
  }
}
