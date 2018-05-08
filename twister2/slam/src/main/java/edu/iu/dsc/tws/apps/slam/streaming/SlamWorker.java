package edu.iu.dsc.tws.apps.slam.streaming;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigReader;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;

import java.util.Map;

public class SlamWorker implements IContainer {
  @Override
  public void init(Config config, int i, ResourcePlan resourcePlan) {
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

      // lets create the taskplan
      TaskPlan taskPlan = Utils.createReduceTaskPlan(config, resourcePlan, parallel);
      TWSNetwork network = new TWSNetwork(config, taskPlan);
      TWSCommunication channel = network.getDataFlowTWSCommunication();

      ScanMatchTask scanMatchTask = null;
      DispatcherBolt dispatcherBolt = null;
      if (resourcePlan.getThisId() != resourcePlan.noOfContainers() - 1) {
        // lets create scanmatcher
        scanMatchTask = new ScanMatchTask();
      } else {
        // lets use the dispatch bolt here
        dispatcherBolt = new DispatcherBolt();
        // todo
        dispatcherBolt.prepare(slamConf, MPI.COMM_WORLD, inputFile, 0);
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

        channel.progress();
      }
    } catch (MPIException e) {
      throw new RuntimeException("Error", e);
    }
  }
}
