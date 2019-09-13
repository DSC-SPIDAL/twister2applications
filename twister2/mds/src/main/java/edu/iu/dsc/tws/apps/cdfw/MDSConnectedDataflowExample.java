package edu.iu.dsc.tws.apps.cdfw;

import com.google.common.base.Stopwatch;
import edu.indiana.soic.spidal.configuration.ConfigurationMgr;
import edu.indiana.soic.spidal.configuration.section.DAMDSSection;
import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.BaseSink;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.apps.mds.*;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.task.cdfw.BaseDriver;
import edu.iu.dsc.tws.task.cdfw.CDFWEnv;
import edu.iu.dsc.tws.task.cdfw.DafaFlowJobConfig;
import edu.iu.dsc.tws.task.cdfw.DataFlowGraph;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.cdfw.CDFWWorker;
import mpi.MPIException;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class MDSConnectedDataflowExample {

    private static final Logger LOG = Logger.getLogger(MDSConnectedDataflowExample.class.getName());

    private static int parallel;
    private static int dsize;
    private static int dimension;
    private static int instances;

    private static String dataInput;
    private static String configFile;
    private static String dataDirectory;
    private static String byteType;
    private static String fileSystem;

    private MDSConnectedDataflowExample() {
    }

    public static class MDSExampleDriver extends BaseDriver {

        @Override
        public void execute(CDFWEnv cdfwEnv) {
            Config config = cdfwEnv.getConfig();
            DafaFlowJobConfig jobConfig = new DafaFlowJobConfig();

            generateData(config);

            DataFlowGraph job1 = generateFirstJob(config, jobConfig);
            DataFlowGraph job2 = generateSecondJob(config, jobConfig);

            cdfwEnv.executeDataFlowGraph(job1);
            cdfwEnv.executeDataFlowGraph(job2);
        }

        public void generateData(Config config) {
            MatrixGenerator matrixGen = new MatrixGenerator(config);
            LOG.info("data size:" + dsize+ "\t" + dimension + "\t" + dataDirectory + "\t" + byteType);
            matrixGen.generate(dsize, dimension, dataDirectory, byteType);
        }
    }


    private static DataFlowGraph generateFirstJob(Config config, DafaFlowJobConfig jobConfig) {

        MDSDataObjectSource mdsDataObjectSource = new MDSDataObjectSource(
                Context.TWISTER2_DIRECT_EDGE, dataDirectory, dsize);
        MDSDataObjectSink mdsDataObjectSink = new MDSDataObjectSink(dimension);
        ComputeGraphBuilder mdsDataProcessingGraphBuilder = ComputeGraphBuilder.newBuilder(config);
        mdsDataProcessingGraphBuilder.setTaskGraphName("MDSDataProcessing");
        mdsDataProcessingGraphBuilder.addSource("dataobjectsource", mdsDataObjectSource, parallel);

        ComputeConnection dataObjectComputeConnection = mdsDataProcessingGraphBuilder.addSink(
                "dataobjectsink", mdsDataObjectSink, parallel);
        dataObjectComputeConnection.direct("dataobjectsource")
                .viaEdge(Context.TWISTER2_DIRECT_EDGE)
                .withDataType(MessageTypes.OBJECT);
        mdsDataProcessingGraphBuilder.setMode(OperationMode.BATCH);
        ComputeGraph dataObjectTaskGraph = mdsDataProcessingGraphBuilder.build();
        mdsDataProcessingGraphBuilder.setTaskGraphName("datapointsTG");

        DataFlowGraph job = DataFlowGraph.newSubGraphJob("dataobjectsink", dataObjectTaskGraph)
                .setWorkers(instances).addDataFlowJobConfig(jobConfig)
                .addOutput("points", "dataobjectsink")
                .setGraphType("non-iterative");
        return job;
    }

    private static DataFlowGraph generateSecondJob(Config config, DafaFlowJobConfig jobConfig) {

        MDSSourceTask generatorTask = new MDSSourceTask();
        MDSReceiverTask receiverTask = new MDSReceiverTask();

        ComputeGraphBuilder mdsComputeProcessingGraphBuilder = ComputeGraphBuilder.newBuilder(config);
        mdsComputeProcessingGraphBuilder.setTaskGraphName("MDSCompute");
        mdsComputeProcessingGraphBuilder.addSource("generator", generatorTask, parallel);
        ComputeConnection computeConnection
                = mdsComputeProcessingGraphBuilder.addSink("receiver", receiverTask, parallel);
        computeConnection.direct("generator")
                .viaEdge(Context.TWISTER2_DIRECT_EDGE)
                .withDataType(MessageTypes.OBJECT);
        mdsComputeProcessingGraphBuilder.setMode(OperationMode.BATCH);
        ComputeGraph dataComputeTaskGraph = mdsComputeProcessingGraphBuilder.build();

        DataFlowGraph job = DataFlowGraph.newSubGraphJob("mdscomputegraph", dataComputeTaskGraph)
                .setWorkers(instances).addDataFlowJobConfig(jobConfig)
                .addInput("dataobjectsink", "points", "dataobjectink")
                .setGraphType("non-iterative");
        return job;
    }

    private static class MDSSourceTask extends BaseSource implements Receptor {

        private static final long serialVersionUID = -254264120110286748L;

        private DataObject<?> dataPointsObject = null;
        private short[] datapoints = null;

        //Config Settings
        private DAMDSSection mdsconfig;
        private ByteOrder byteOrder;
        private int blockSize;
        private int cps;
        private boolean bind;

        @Override
        public void execute() {
            DataPartition<?> dataPartition = dataPointsObject.getPartition(context.taskIndex());
            datapoints = (short[]) dataPartition.getConsumer().next();
            executeMds(datapoints);
            context.writeEnd(Context.TWISTER2_DIRECT_EDGE, "MDS_Execution");
        }

        @Override
        public void add(String name, DataObject<?> data) {
            LOG.log(Level.INFO, "Received input: " + name);
            if ("points".equals(name)) {
                this.dataPointsObject = data;
            }
        }

        public void prepare(Config config, TaskContext context) {
            super.prepare(config, context);
            String configFilename = String.valueOf(config.get("config"));
            readConfiguration(configFilename);
            String[] args = new String[]{configFilename,
                    String.valueOf(ParallelOps.nodeCount),
                    String.valueOf(ParallelOps.threadCount)};
            mdsconfig = new ConfigurationMgr(configFilename).damdsSection;
            try {
                ParallelOps.setupParallelism(args);
                ParallelOps.setParallelDecomposition(mdsconfig.numberDataPoints, mdsconfig.targetDimension);
            } catch (MPIException e) {
                throw new RuntimeException(e.getMessage());
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            }
            byteOrder = mdsconfig.isBigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
            blockSize = mdsconfig.blockSize;
        }

        private void readConfiguration(String filename) {
            mdsconfig = new ConfigurationMgr(filename).damdsSection;

            ParallelOps.nodeCount = context.getParallelism();
            ParallelOps.threadCount = Integer.parseInt(String.valueOf(config.get("twister2.exector.worker.threads")));

            byteOrder = mdsconfig.isBigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
            blockSize = mdsconfig.blockSize;

            ParallelOps.mmapsPerNode = 1;
            ParallelOps.mmapScratchDir = ".";

            cps = -1;
            if (cps == -1) {
                bind = false;
            }
            LOG.info("node count and thread count:" + ParallelOps.nodeCount + "\t"
                    + ParallelOps.threadCount + "\t" + byteOrder + "\t" + blockSize);
        }

        private void executeMds(short[] datapoints) {
            Stopwatch mainTimer = Stopwatch.createStarted();
            MDSProgramWorker mdsProgramWorker = new MDSProgramWorker(0, ParallelOps.threadComm,
                    mdsconfig, byteOrder, blockSize, mainTimer, null, datapoints);
            try {
                mdsProgramWorker.run();
            } catch (IOException e) {
                throw new RuntimeException("IOException Occured:" + e.getMessage());
            }
        }
    }

    private static class MDSReceiverTask extends BaseSink implements Collector {
        private static final long serialVersionUID = -254264120110286748L;

        @Override
        public boolean execute(IMessage content) {
            LOG.info("Received message:" + content.getContent().toString());
            return false;
        }

        @Override
        public DataPartition<?> get() {
            return null;
        }
    }

    public static void main(String[] args) throws ParseException {
        Config config = ResourceAllocator.loadConfig(new HashMap<>());

        // build JobConfig
        HashMap<String, Object> configurations = new HashMap<>();
        configurations.put(SchedulerContext.THREADS_PER_WORKER, 1);

        Options options = new Options();
        options.addOption(CDFConstants.ARGS_WORKERS, true, "Workers");
        options.addOption(CDFConstants.ARGS_PARALLELISM_VALUE, true, "parallelism");
        options.addOption(CDFConstants.ARGS_DSIZE, true, "Size of the matrix rows");
        options.addOption(CDFConstants.ARGS_DIMENSIONS, true, "dimension of the matrix");
        options.addOption(CDFConstants.ARGS_BYTE_TYPE, true, "bytetype");
        options.addOption(CDFConstants.ARGS_DATA_INPUT, true, "datainput");

        options.addOption(CDFConstants.ARGS_DINPUT_DIRECTORY, true, "2");
        options.addOption(CDFConstants.ARGS_FILE_SYSTEM, true, "2");
        options.addOption(CDFConstants.ARGS_CONFIG_FILE, true, "2");

        @SuppressWarnings("deprecation")
        CommandLineParser commandLineParser = new DefaultParser();
        CommandLine cmd = commandLineParser.parse(options, args);

        instances = Integer.parseInt(cmd.getOptionValue(CDFConstants.ARGS_WORKERS));
        parallel = Integer.parseInt(cmd.getOptionValue(CDFConstants.ARGS_PARALLELISM_VALUE));
        dsize = Integer.parseInt(cmd.getOptionValue(CDFConstants.ARGS_DSIZE));
        dimension = Integer.parseInt(cmd.getOptionValue(CDFConstants.ARGS_DIMENSIONS));
        byteType = cmd.getOptionValue(CDFConstants.ARGS_BYTE_TYPE);
        dataInput = cmd.getOptionValue(CDFConstants.ARGS_DATA_INPUT);
        dataDirectory = cmd.getOptionValue(CDFConstants.ARGS_DINPUT_DIRECTORY);
        fileSystem = cmd.getOptionValue(CDFConstants.ARGS_FILE_SYSTEM);
        configFile = cmd.getOptionValue(CDFConstants.ARGS_CONFIG_FILE);


        // build JobConfig
        configurations.put(CDFConstants.ARGS_WORKERS, Integer.toString(instances));
        configurations.put(CDFConstants.ARGS_PARALLELISM_VALUE, Integer.toString(parallel));
        configurations.put(CDFConstants.ARGS_DSIZE, Integer.toString(dsize));
        configurations.put(CDFConstants.ARGS_DIMENSIONS, Integer.toString(dimension));
        configurations.put(CDFConstants.ARGS_BYTE_TYPE, byteType);
        configurations.put(CDFConstants.ARGS_DATA_INPUT, dataInput);
        configurations.put(CDFConstants.ARGS_DINPUT_DIRECTORY, dataDirectory);
        configurations.put(CDFConstants.ARGS_FILE_SYSTEM, fileSystem);
        configurations.put(CDFConstants.ARGS_CONFIG_FILE, configFile);

        // build JobConfig
        JobConfig jobConfig = new JobConfig();
        jobConfig.putAll(configurations);

        config = Config.newBuilder().putAll(config)
                .put(SchedulerContext.DRIVER_CLASS, null).build();

        // build JobConfig
        Twister2Job twister2Job;
        twister2Job = Twister2Job.newBuilder()
                .setJobName(MDSExampleDriver.class.getName())
                .setWorkerClass(CDFWWorker.class)
                .setDriverClass(MDSExampleDriver.class.getName())
                .addComputeResource(1, 2048, instances)
                .setConfig(jobConfig)
                .build();
        // now submit the job
        Twister2Submitter.submitJob(twister2Job, config);
    }

}
