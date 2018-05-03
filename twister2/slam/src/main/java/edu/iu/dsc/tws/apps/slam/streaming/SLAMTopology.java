package edu.iu.dsc.tws.apps.slam.streaming;

import edu.iu.dsc.tws.apps.slam.core.app.LaserScan;
import edu.iu.dsc.tws.apps.slam.core.app.Position;
import edu.iu.dsc.tws.apps.slam.core.grid.Array2D;
import edu.iu.dsc.tws.apps.slam.core.grid.GMap;
import edu.iu.dsc.tws.apps.slam.core.grid.HierarchicalArray2D;
import edu.iu.dsc.tws.apps.slam.core.gridfastsalm.Particle;
import edu.iu.dsc.tws.apps.slam.core.gridfastsalm.TNode;
import edu.iu.dsc.tws.apps.slam.core.scanmatcher.PointAccumulator;
import edu.iu.dsc.tws.apps.slam.core.sensor.RangeReading;
import edu.iu.dsc.tws.apps.slam.core.sensor.RangeSensor;
import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;
import edu.iu.dsc.tws.apps.slam.core.utils.DoublePoint;
import edu.iu.dsc.tws.apps.slam.core.utils.IntPoint;
import edu.iu.dsc.tws.apps.slam.streaming.msgs.*;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SLAMTopology implements IContainer {
    private static Logger LOG = LoggerFactory.getLogger(SLAMTopology.class);

    public static void main(String[] args) throws Exception {

        Options options = new Options();
        options.addOption(Constants.ARGS_NAME, true, "Name of the topology");
        options.addOption(Constants.ARGS_LOCAL, false, "Weather we want run locally");
        options.addOption(Constants.ARGS_DS_MODE, true, "The distributed mode, specify 0, 1, 2, 3 etc");
        options.addOption(Constants.ARGS_PARALLEL, true, "No of parallel nodes");
        options.addOption(Constants.ARGS_IOT_CLOUD, false, "Weather we run through IoTCloud");
        options.addOption(Constants.ARGS_PARTICLES, true, "No of particles");

        CommandLineParser commandLineParser = new BasicParser();
        CommandLine cmd = commandLineParser.parse(options, args);
        String name = cmd.getOptionValue(Constants.ARGS_NAME);
        boolean local = cmd.hasOption(Constants.ARGS_LOCAL);
        String dsModeValue = cmd.getOptionValue(Constants.ARGS_DS_MODE);
        int dsMode = Integer.parseInt(dsModeValue);
        String pValue = cmd.getOptionValue(Constants.ARGS_PARALLEL);
        int p = Integer.parseInt(pValue);
        boolean iotCloud = cmd.hasOption(Constants.ARGS_IOT_CLOUD);
    }

    private static void addSerializers(Config config) {
//        config.registerSerialization(DoublePoint.class);
//        config.registerSerialization(IntPoint.class);
//        config.registerSerialization(Particle.class);
//        config.registerSerialization(GMap.class);
//        config.registerSerialization(Array2D.class);
//        config.registerSerialization(HierarchicalArray2D.class);
//        config.registerSerialization(TNode.class);
//        config.registerSerialization(DoubleOrientedPoint.class);
//        config.registerSerialization(Particle.class);
//        config.registerSerialization(PointAccumulator.class);
//        config.registerSerialization(HierarchicalArray2D.class);
//        config.registerSerialization(Array2D.class);
//        config.registerSerialization(Position.class);
//        config.registerSerialization(Object[][].class);
//        config.registerSerialization(TransferMap.class);
//        config.registerSerialization(ParticleMaps.class);
//        config.registerSerialization(MapCell.class);
//        config.registerSerialization(LaserScan.class);
//        config.registerSerialization(ParticleValue.class);
//        config.registerSerialization(TNodeValue.class);
//        config.registerSerialization(RangeSensor.class);
//        config.registerSerialization(RangeReading.class);
//        config.registerSerialization(Trace.class);
//        config.registerSerialization(Ready.class);
    }

    @Override
    public void init(Config config, int i, ResourcePlan resourcePlan) {

    }
}
