//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.apps.mds;

import com.google.common.base.Stopwatch;
import edu.indiana.soic.spidal.configuration.ConfigurationMgr;
import edu.indiana.soic.spidal.configuration.section.DAMDSSection;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.IONames;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskWorker;
import mpi.MPIException;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.logging.Logger;

public class MDSWorker extends TaskWorker {

    private static final Logger LOG = Logger.getLogger(MDSWorker.class.getName());

    //Config Settings
    private DAMDSSection mdsconfig;
    private ByteOrder byteOrder;
    private int BlockSize;
    private static boolean bind;
    private static int cps;

    @Override
    public void execute() {

        MDSWorkerParameters mdsWorkerParameters = MDSWorkerParameters.build(config);

        int parallel = mdsWorkerParameters.getParallelismValue();
        int datasize = mdsWorkerParameters.getDsize();
        int matrixColumLength = mdsWorkerParameters.getDimension();

        String dataInput = mdsWorkerParameters.getDataInput();
        String configFile = mdsWorkerParameters.getConfigFile();
        String directory = mdsWorkerParameters.getDatapointDirectory();
        String byteType = mdsWorkerParameters.getByteType();

        LOG.info("Config File:" + configFile);
        readConfiguration(configFile);
        String[] args = new String[]{configFile, String.valueOf(ParallelOps.nodeCount),
                String.valueOf(ParallelOps.threadCount)};
        mdsconfig = new ConfigurationMgr(configFile).damdsSection;
        try {
            ParallelOps.setupParallelism(args);
            ParallelOps.setParallelDecomposition(mdsconfig.numberDataPoints, mdsconfig.targetDimension);
        } catch (MPIException e) {
            throw new RuntimeException(e.getMessage());
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }

        /** Generate the Matrix for the MDS if the user input is "generate" **/
        if (Context.TWISTER2_DATA_INPUT.equalsIgnoreCase(dataInput)) {
            MatrixGenerator matrixGen = new MatrixGenerator(config, workerId);
            matrixGen.generate(datasize, matrixColumLength, directory, byteType);
        }

        long startTime = System.currentTimeMillis();

        /** Task Graph to partition the generated matrix for MDS **/
        MDSDataObjectSource mdsDataObjectSource = new MDSDataObjectSource(Context.TWISTER2_DIRECT_EDGE,
                directory, datasize);
        MDSDataObjectSink mdsDataObjectSink = new MDSDataObjectSink("points", matrixColumLength);
        ComputeGraphBuilder mdsDataProcessingGraphBuilder = ComputeGraphBuilder.newBuilder(config);
        mdsDataProcessingGraphBuilder.setTaskGraphName("MDSDataProcessing");
        mdsDataProcessingGraphBuilder.addSource("dataobjectsource", mdsDataObjectSource, parallel);

        ComputeConnection dataObjectComputeConnection = mdsDataProcessingGraphBuilder.addCompute(
                "dataobjectsink", mdsDataObjectSink, parallel);
        dataObjectComputeConnection.direct("dataobjectsource")
                .viaEdge(Context.TWISTER2_DIRECT_EDGE)
                .withDataType(MessageTypes.OBJECT);
        mdsDataProcessingGraphBuilder.setMode(OperationMode.BATCH);
        ComputeGraph dataObjectTaskGraph = mdsDataProcessingGraphBuilder.build();

        //Get the execution plan for the first task graph
        ExecutionPlan plan = taskExecutor.plan(dataObjectTaskGraph);

        //Actual execution for the first taskgraph
        taskExecutor.execute(dataObjectTaskGraph, plan);

        long endTimeData = System.currentTimeMillis();

         /** Task Graph to run the MDS **/
        MDSSourceTask generatorTask = new MDSSourceTask();
        MDSReceiverTask receiverTask = new MDSReceiverTask();

        ComputeGraphBuilder mdsComputeProcessingGraphBuilder = ComputeGraphBuilder.newBuilder(config);
        mdsComputeProcessingGraphBuilder.setTaskGraphName("MDSCompute");
        mdsComputeProcessingGraphBuilder.addSource("generator", generatorTask, parallel);
        ComputeConnection computeConnection = mdsComputeProcessingGraphBuilder.addCompute("receiver",
                receiverTask, parallel);
        computeConnection.direct("generator")
                .viaEdge(Context.TWISTER2_DIRECT_EDGE)
                .withDataType(MessageTypes.OBJECT);
        mdsComputeProcessingGraphBuilder.setMode(OperationMode.BATCH);

        ComputeGraph mdsTaskGraph = mdsComputeProcessingGraphBuilder.build();

        //Get the execution plan for the first task graph
        ExecutionPlan executionPlan = taskExecutor.plan(mdsTaskGraph);

        //Actual execution for the first taskgraph
        taskExecutor.execute(mdsTaskGraph, executionPlan);
        long endTime = System.currentTimeMillis();
        if (workerId == 0) {
            LOG.info("Data Load time : " + (endTimeData - startTime) + "\n"
                    + "Total Time : " + (endTime - startTime)
                    + "Compute Time : " + (endTime - endTimeData));
        }
    }

    private void readConfiguration(String filename) {
        mdsconfig = new ConfigurationMgr(filename).damdsSection;

        ParallelOps.nodeCount = Integer.parseInt(config.getStringValue("workers"));
        ParallelOps.threadCount = Integer.parseInt(String.valueOf(config.get("twister2.exector.worker.threads")));

        LOG.info("node count value:" + ParallelOps.nodeCount + "\t" + ParallelOps.threadCount);

        byteOrder = mdsconfig.isBigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
        BlockSize = mdsconfig.blockSize;

        ParallelOps.mmapsPerNode = 1;
        ParallelOps.mmapScratchDir = ".";

        cps = -1;
        if (cps == -1){
            bind = false;
        }
    }

    private static class MDSSourceTask extends BaseSource implements Receptor {

        private static final long serialVersionUID = -254264120110286748L;

        private short[] datapoints = null;
        private DataPartition<?> dataPartition = null;

        //Config Settings
        private DAMDSSection mdsconfig;
        private ByteOrder byteOrder;
        private int BlockSize;

        @Override
        public void execute() {
            datapoints = (short[]) dataPartition.first();
            executeMds(datapoints);
            context.writeEnd(Context.TWISTER2_DIRECT_EDGE, "MDS_Execution");
        }

        @Override
        public void add(String name, DataObject<?> data) {
        }

        @Override
        public void add(String name, DataPartition<?> data) {
            if ("points".equals(name)) {
                this.dataPartition = data;
            }
        }

        @Override
        public IONames getReceivableNames() {
            return IONames.declare("points");
        }

        public void prepare(Config config, TaskContext context) {
            super.prepare(config, context);
            String configFilename = String.valueOf(config.get("config"));
            mdsconfig = new ConfigurationMgr(configFilename).damdsSection;
            byteOrder = mdsconfig.isBigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
            BlockSize = mdsconfig.blockSize;
        }

        private void executeMds(short[] datapoints) {
            Stopwatch mainTimer = Stopwatch.createStarted();
            LOG.info("parallel ops thread:" + ParallelOps.threadComm + "\t" + byteOrder + "\t" + BlockSize);
            MDSProgramWorker mdsProgramWorker = new MDSProgramWorker(0, ParallelOps.threadComm,
                    mdsconfig, byteOrder, BlockSize, mainTimer, null, datapoints);
            try {
                mdsProgramWorker.run();
            } catch (IOException e) {
                throw new RuntimeException("IOException Occured:" + e.getMessage());
            }
        }
    }

    private static class MDSReceiverTask extends BaseCompute implements Collector {
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
}
