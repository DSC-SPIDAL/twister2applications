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
import edu.iu.dsc.tws.api.task.*;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import mpi.MPIException;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.logging.Level;
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
        int dataSize = mdsWorkerParameters.getDsize();
        int matrixColumLength = mdsWorkerParameters.getDimension();

        String dataInput = mdsWorkerParameters.getDataInput();
        String configFile = mdsWorkerParameters.getConfigFile();
        String directory = mdsWorkerParameters.getDatapointDirectory();
        String byteType = mdsWorkerParameters.getByteType();

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
            matrixGen.generate(dataSize, matrixColumLength, directory, byteType);
        }

        /** Task Graph to partition the generated matrix for MDS **/
        MDSDataObjectSource mdsDataObjectSource = new MDSDataObjectSource(Context.TWISTER2_DIRECT_EDGE, directory,
                dataSize);
        MDSDataObjectSink mdsDataObjectSink = new MDSDataObjectSink(matrixColumLength);

        TaskGraphBuilder mdsDataProcessBuilder = TaskGraphBuilder.newBuilder(config);
        mdsDataProcessBuilder.setTaskGraphName("MDSDataProcessing");
        mdsDataProcessBuilder.addSource("dataobjectsource", mdsDataObjectSource, parallel);

        ComputeConnection dataObjectComputeConnection = mdsDataProcessBuilder.addSink(
                "dataobjectsink", mdsDataObjectSink, parallel);
        dataObjectComputeConnection.direct(
                "dataobjectsource", Context.TWISTER2_DIRECT_EDGE, DataType.OBJECT);
        mdsDataProcessBuilder.setMode(OperationMode.BATCH);

        DataFlowTaskGraph dataObjectTaskGraph = mdsDataProcessBuilder.build();
        //Get the execution plan for the first task graph
        ExecutionPlan plan = taskExecutor.plan(dataObjectTaskGraph);

        //Actual execution for the first taskgraph
        taskExecutor.execute(dataObjectTaskGraph, plan);

        //Retrieve the output of the first task graph
        DataObject<Object> dataPointsObject = taskExecutor.getOutput(dataObjectTaskGraph, plan, "dataobjectsink");

        /** Task Graph to run the MDS **/
        MDSSourceTask generatorTask = new MDSSourceTask();
        MDSReceiverTask receiverTask = new MDSReceiverTask();

        TaskGraphBuilder mdsComputeGraphBuilder = TaskGraphBuilder.newBuilder(config);
        mdsComputeGraphBuilder.setTaskGraphName("MDSCompute");
        mdsComputeGraphBuilder.addSource("generator", generatorTask, parallel);
        ComputeConnection computeConnection = mdsComputeGraphBuilder.addSink("receiver", receiverTask, parallel);
        computeConnection.direct("generator", Context.TWISTER2_DIRECT_EDGE, DataType.OBJECT);
        mdsComputeGraphBuilder.setMode(OperationMode.BATCH);

        DataFlowTaskGraph mdsTaskGraph = mdsComputeGraphBuilder.build();

        //Get the execution plan for the first task graph
        ExecutionPlan executionPlan = taskExecutor.plan(mdsTaskGraph);

        //Actual execution for the first taskgraph
        taskExecutor.addInput(
                mdsTaskGraph, executionPlan, "generator", "points", dataPointsObject);
        taskExecutor.execute(mdsTaskGraph, executionPlan);
    }

    private void readConfiguration(String filename) {
        mdsconfig = new ConfigurationMgr(filename).damdsSection;

        ParallelOps.nodeCount = Integer.parseInt(config.getStringValue("workers"));
        ParallelOps.threadCount = Integer.parseInt(String.valueOf(config.get("twister2.exector.worker.threads")));

        byteOrder = mdsconfig.isBigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
        BlockSize = mdsconfig.blockSize;

        ParallelOps.mmapsPerNode = 1;
        ParallelOps.mmapScratchDir = ".";

        cps = -1;
        if (cps == -1){
            bind = false;
        }
        LOG.info("node count and thread count:" + ParallelOps.nodeCount + "\t"
                + ParallelOps.threadCount + "\t" + byteOrder + "\t" + BlockSize);
    }

    private static class MDSSourceTask extends BaseSource implements Receptor {

        private static final long serialVersionUID = -254264120110286748L;

        private DataObject<?> dataPointsObject = null;
        private short[] datapoints = null;

        //Config Settings
        private DAMDSSection mdsconfig;
        private ByteOrder byteOrder;
        private int BlockSize;

        @Override
        public void execute() {
            DataPartition<?> dataPartition = dataPointsObject.getPartitions(context.taskIndex());
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
            mdsconfig = new ConfigurationMgr(configFilename).damdsSection;
            byteOrder = mdsconfig.isBigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
            BlockSize = mdsconfig.blockSize;
        }

        private void executeMds(short[] datapoints) {
            Stopwatch mainTimer = Stopwatch.createStarted();
            MDSProgramWorker mdsProgramWorker = new MDSProgramWorker(0, ParallelOps.threadComm,
                    mdsconfig, byteOrder, BlockSize, mainTimer, null, datapoints);
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
}
