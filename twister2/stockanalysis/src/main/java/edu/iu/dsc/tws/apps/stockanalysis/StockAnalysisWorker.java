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
package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.apps.stockanalysis.utils.Record;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.TaskGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskWorker;
import edu.iu.dsc.tws.task.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.window.constant.WindowType;
import edu.iu.dsc.tws.task.window.core.BaseWindowedSink;
import edu.iu.dsc.tws.task.window.function.ProcessWindowedFunction;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


public class StockAnalysisWorker extends TaskWorker {
    private static final Logger LOG = Logger.getLogger(StockAnalysisWorker.class.getName());

    private int parallel;
    private String distanceMatrixDirectory;
    private String directory;
    private String byteType;
    private String vectorDirectory;
    private String datainputFile;
    private String configFile;
    private String numberOfDays;
    private String startDate;
    private String endDate;
    private String mode;
    private int distanceType;

    private Long windowLength;
    private Long slidingLength;

    private StockAnalysisWorkerParameters stockAnalysisWorkerParameters;

    @Override
    public void execute() {
        LOG.log(Level.FINE, "Task worker starting: " + workerId);

        stockAnalysisWorkerParameters = StockAnalysisWorkerParameters.build(config);

        parallel = stockAnalysisWorkerParameters.getParallelismValue();
        distanceMatrixDirectory = stockAnalysisWorkerParameters.getDatapointDirectory();
        configFile = stockAnalysisWorkerParameters.getConfigFile();
        directory = stockAnalysisWorkerParameters.getDatapointDirectory();
        byteType = stockAnalysisWorkerParameters.getByteType();

        datainputFile = stockAnalysisWorkerParameters.getDinputFile();
        vectorDirectory = stockAnalysisWorkerParameters.getOutputDirectory();
        numberOfDays = stockAnalysisWorkerParameters.getNumberOfDays();
        startDate = stockAnalysisWorkerParameters.getStartDate();
        endDate = stockAnalysisWorkerParameters.getEndDate();
        mode = stockAnalysisWorkerParameters.getMode();
        distanceType = Integer.parseInt(stockAnalysisWorkerParameters.getDistanceType());

        WindowingParameters windowParameters = this.stockAnalysisWorkerParameters.getWindowingParameters();
        TimeUnit timeUnit = TimeUnit.DAYS;
        if (windowParameters != null) {
            if (!windowParameters.isDuration()) {
                windowLength = windowParameters.getWindowLength();
                slidingLength = windowParameters.getSlidingLength();
            }
        }

        LOG.info("Distance Matrix Directory:" + distanceMatrixDirectory + "\t" + vectorDirectory);

        //Sequential Vector Generation
        long startTime = System.currentTimeMillis();

        DataFlowTaskGraph preprocesingTaskGraph = buildStockAnalysisDataflowGraph();

        //Get the execution plan for the first task graph
        ExecutionPlan preprocessExecutionPlan = taskExecutor.plan(preprocesingTaskGraph);

        //Actual execution for the first taskgraph
        taskExecutor.execute(preprocesingTaskGraph, preprocessExecutionPlan);

        //Retrieve the output of the first task graph
        DataObject<Object> dataPointsObject = taskExecutor.getOutput(preprocesingTaskGraph,
                preprocessExecutionPlan, "preprocessingsinktask");

        LOG.info("%%% DataPoints Object:%%%" + dataPointsObject + "\t" + dataPointsObject.getPartitions());
        long endTime = System.currentTimeMillis();
        LOG.info("Compute Time : " + (endTime - startTime));
    }

    private DataFlowTaskGraph buildStockAnalysisDataflowGraph() {

        /** Task Graph to do the preprocessing **/
        DataProcessingSourceTask preprocessingSourceTask = new DataProcessingSourceTask(datainputFile, vectorDirectory,
                startDate);
        BaseWindowedSink baseWindowedSink
                = new DataProcessingStreamingWindowCompute(new ProcessWindowFunctionImpl(),
                OperationMode.STREAMING).withSlidingCountWindow(windowLength, slidingLength);
        DataPreprocessingComputeTask dataPreprocessingCompute = new DataPreprocessingComputeTask(
                vectorDirectory, distanceMatrixDirectory, distanceType, Context.TWISTER2_DIRECT_EDGE);
        DistanceCalculatorComputeTask distanceCalculatorCompute = new DistanceCalculatorComputeTask(
                vectorDirectory, distanceMatrixDirectory, distanceType, Context.TWISTER2_DIRECT_EDGE);
        MDSWorkerComputeTask mdsProgramWorkerCompute = new MDSWorkerComputeTask(Context.TWISTER2_DIRECT_EDGE);
        StockAnalysisSinkTask stockAnalysisSinkTask = new StockAnalysisSinkTask();

        TaskGraphBuilder preprocessingTaskGraphBuilder = TaskGraphBuilder.newBuilder(config);
        preprocessingTaskGraphBuilder.setTaskGraphName("StockAnalysisTaskGraph");

        preprocessingTaskGraphBuilder.addSource("preprocessingsourcetask", preprocessingSourceTask, parallel);
        ComputeConnection windowComputeConnection = preprocessingTaskGraphBuilder.addCompute("windowsink",
                baseWindowedSink, parallel);
        ComputeConnection preprocessingComputeConnection = preprocessingTaskGraphBuilder.addCompute(
                "preprocessingcompute", dataPreprocessingCompute, parallel);
        ComputeConnection distanceCalculatorComputeConnection = preprocessingTaskGraphBuilder.addCompute(
                "distancecalculatorcompute", distanceCalculatorCompute, parallel);
        ComputeConnection mdsComputeConnection = preprocessingTaskGraphBuilder.addCompute(
                "mdsprogramcompute", mdsProgramWorkerCompute, parallel);
        ComputeConnection stockAnalysisSinkConnection = preprocessingTaskGraphBuilder.addSink(
                "dataanalysissink", stockAnalysisSinkTask, parallel);

        windowComputeConnection.direct("preprocessingsourcetask").viaEdge(Context.TWISTER2_DIRECT_EDGE)
                .withDataType(MessageTypes.OBJECT);

        preprocessingComputeConnection.direct("windowsink").viaEdge(Context.TWISTER2_DIRECT_EDGE)
                .withDataType(MessageTypes.OBJECT);

        distanceCalculatorComputeConnection.direct("preprocessingcompute").viaEdge(Context.TWISTER2_DIRECT_EDGE)
                .withDataType(MessageTypes.OBJECT);

        mdsComputeConnection.direct("distancecalculatorcompute").viaEdge(Context.TWISTER2_DIRECT_EDGE)
                .withDataType(MessageTypes.OBJECT);

        stockAnalysisSinkConnection.direct("mdsprogramcompute").viaEdge(Context.TWISTER2_DIRECT_EDGE)
                .withDataType(MessageTypes.OBJECT);

        preprocessingTaskGraphBuilder.setMode(OperationMode.STREAMING);
        DataFlowTaskGraph preprocesingTaskGraph = preprocessingTaskGraphBuilder.build();
        return preprocesingTaskGraph;
    }

    private BaseWindowedSink getWindowSinkInstance() {
        BaseWindowedSink baseWindowedSink = new DataProcessingStreamingWindowCompute(new ProcessWindowFunctionImpl(),
                OperationMode.STREAMING);
        WindowingParameters windowParameters = this.stockAnalysisWorkerParameters.getWindowingParameters();
        TimeUnit timeUnit = TimeUnit.DAYS;
        if (windowParameters != null) {
            WindowType windowType = windowParameters.getWindowType();
            if (windowParameters.isDuration()) {
                if (windowType.equals(WindowType.TUMBLING)) {
                    baseWindowedSink
                            .withTumblingDurationWindow(windowParameters.getWindowLength(), timeUnit);
                }
                if (windowType.equals(WindowType.SLIDING)) {
                    baseWindowedSink
                            .withSlidingDurationWindow(windowParameters.getWindowLength(), timeUnit,
                                    windowParameters.getSlidingLength(), timeUnit);
                }
            } else {
                if (windowType.equals(WindowType.TUMBLING)) {
                    baseWindowedSink
                            .withTumblingCountWindow(windowParameters.getWindowLength());
                }
                if (windowType.equals(WindowType.SLIDING)) {
                    baseWindowedSink
                            .withSlidingCountWindow(windowParameters.getWindowLength(),
                                    windowParameters.getSlidingLength());
                }
            }
        }
        return baseWindowedSink;
    }

    protected static class ProcessWindowFunctionImpl implements ProcessWindowedFunction<Record> {

        private static final long serialVersionUID = 8517840191276879034L;

        private static final Logger LOG = Logger.getLogger(ProcessWindowFunctionImpl.class.getName());

        @Override
        public IWindowMessage<Record> process(IWindowMessage<Record> windowMessage) {
            LOG.info("Received Message:" + windowMessage + "\t" + windowMessage.getWindow().size());
            return windowMessage;
        }

        @Override
        public IMessage<Record> processLateMessage(IMessage<Record> lateMessage) {
            return lateMessage;
        }


        @Override
        public Record onMessage(Record object1, Record object2) {
            return null;
        }
    }


//    private DataFlowTaskGraph buildStockAnalysisDataflowGraph() {
//
//        /** Task Graph to do the preprocessing **/
//        DataPreprocessingSourceTask preprocessingSourceTask = new DataPreprocessingSourceTask(
//                datainputFile, vectorDirectory, numberOfDays, startDate, endDate, mode);
//        DataPreprocessingComputeTask dataPreprocessingCompute = new DataPreprocessingComputeTask(
//                vectorDirectory, distanceMatrixDirectory, distanceType, Context.TWISTER2_DIRECT_EDGE);
//        DistanceCalculatorComputeTask distanceCalculatorCompute = new DistanceCalculatorComputeTask(
//                vectorDirectory, distanceMatrixDirectory, distanceType, Context.TWISTER2_DIRECT_EDGE);
//        MDSWorkerComputeTask mdsProgramWorkerCompute = new MDSWorkerComputeTask(Context.TWISTER2_DIRECT_EDGE);
//        StockAnalysisSinkTask stockAnalysisSinkTask = new StockAnalysisSinkTask();
//
//        TaskGraphBuilder preprocessingTaskGraphBuilder = TaskGraphBuilder.newBuilder(config);
//        preprocessingTaskGraphBuilder.setTaskGraphName("StockAnalysisTaskGraph");
//        preprocessingTaskGraphBuilder.addSource("preprocessingsourcetask", preprocessingSourceTask, parallel);
//
//        ComputeConnection preprocessingComputeConnection = preprocessingTaskGraphBuilder.addCompute(
//                "preprocessingcompute", dataPreprocessingCompute, parallel);
//        ComputeConnection distanceCalculatorComputeConnection = preprocessingTaskGraphBuilder.addCompute(
//                "distancecalculatorcompute", distanceCalculatorCompute, parallel);
//        ComputeConnection mdsComputeConnection = preprocessingTaskGraphBuilder.addCompute(
//                "mdsprogramcompute", mdsProgramWorkerCompute, parallel);
//        ComputeConnection stockAnalysisSinkConnection = preprocessingTaskGraphBuilder.addSink(
//                "dataanalysissink", stockAnalysisSinkTask, parallel);
//
//        preprocessingComputeConnection.direct("preprocessingsourcetask").viaEdge(Context.TWISTER2_DIRECT_EDGE)
//                .withDataType(MessageTypes.OBJECT);
//
//        distanceCalculatorComputeConnection.direct("preprocessingcompute").viaEdge(Context.TWISTER2_DIRECT_EDGE)
//                .withDataType(MessageTypes.OBJECT);
//
//        mdsComputeConnection.direct("distancecalculatorcompute").viaEdge(Context.TWISTER2_DIRECT_EDGE)
//                .withDataType(MessageTypes.OBJECT);
//
//        stockAnalysisSinkConnection.direct("mdsprogramcompute").viaEdge(Context.TWISTER2_DIRECT_EDGE)
//                .withDataType(MessageTypes.OBJECT);
//
//        preprocessingTaskGraphBuilder.setMode(OperationMode.STREAMING);
//        DataFlowTaskGraph preprocesingTaskGraph = preprocessingTaskGraphBuilder.build();
//        return preprocesingTaskGraph;
//    }
}
