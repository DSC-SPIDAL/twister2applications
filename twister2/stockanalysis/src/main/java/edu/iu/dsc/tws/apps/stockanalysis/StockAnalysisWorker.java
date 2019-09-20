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
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskWorker;

import java.util.logging.Level;
import java.util.logging.Logger;


public class StockAnalysisWorker extends TaskWorker {

    private static final Logger LOG = Logger.getLogger(StockAnalysisWorker.class.getName());

    private static final long serialVersionUID = -254264120110286748L;

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
        windowLength = windowParameters.getWindowLength();
        slidingLength = windowParameters.getSlidingLength();

        LOG.info("Distance Matrix Directory:" + distanceMatrixDirectory + "\t" + vectorDirectory);

        //Sequential Vector Generation
        long startTime = System.currentTimeMillis();

        ComputeGraph preprocesingTaskGraph = buildStockAnalysisDataflowGraph();

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

    private ComputeGraph buildStockAnalysisDataflowGraph() {

        DataProcessingSourceTask preprocessingSourceTask = new DataProcessingSourceTask(datainputFile, vectorDirectory,
                startDate);
        DataPreprocessingComputeTask dataPreprocessingCompute = new DataPreprocessingComputeTask(vectorDirectory,
                distanceMatrixDirectory, distanceType, windowLength.intValue(), slidingLength.intValue(),
                startDate, Context.TWISTER2_DIRECT_EDGE);
        DistanceCalculatorComputeTask distanceCalculatorCompute = new DistanceCalculatorComputeTask(vectorDirectory,
                distanceMatrixDirectory, distanceType, Context.TWISTER2_DIRECT_EDGE);
        MDSWorkerComputeTask mdsProgramWorkerCompute = new MDSWorkerComputeTask(Context.TWISTER2_DIRECT_EDGE);
        StockAnalysisSinkTask stockAnalysisSinkTask = new StockAnalysisSinkTask();

        ComputeGraphBuilder preprocessingTaskGraphBuilder = ComputeGraphBuilder.newBuilder(config);
        preprocessingTaskGraphBuilder.setTaskGraphName("StockAnalysisTaskGraph");
        preprocessingTaskGraphBuilder.addSource("preprocessingsourcetask", preprocessingSourceTask, parallel);
        ComputeConnection preprocessingComputeConnection = preprocessingTaskGraphBuilder.addCompute(
                "preprocessingcompute", dataPreprocessingCompute, parallel);
        ComputeConnection distanceCalculatorComputeConnection = preprocessingTaskGraphBuilder.addCompute(
                "distancecalculatorcompute", distanceCalculatorCompute, parallel);
        ComputeConnection mdsComputeConnection = preprocessingTaskGraphBuilder.addCompute(
                "mdsprogramcompute", mdsProgramWorkerCompute, parallel);
        ComputeConnection stockAnalysisSinkConnection = preprocessingTaskGraphBuilder.addSink(
                "dataanalysissink", stockAnalysisSinkTask, parallel);

        preprocessingComputeConnection.direct("preprocessingsourcetask").viaEdge(Context.TWISTER2_DIRECT_EDGE)
                .withDataType(MessageTypes.OBJECT);
        distanceCalculatorComputeConnection.direct("preprocessingcompute").viaEdge(Context.TWISTER2_DIRECT_EDGE)
                .withDataType(MessageTypes.OBJECT);
        mdsComputeConnection.direct("distancecalculatorcompute").viaEdge(Context.TWISTER2_DIRECT_EDGE)
                .withDataType(MessageTypes.OBJECT);
        stockAnalysisSinkConnection.direct("mdsprogramcompute").viaEdge(Context.TWISTER2_DIRECT_EDGE)
                .withDataType(MessageTypes.OBJECT);
        preprocessingTaskGraphBuilder.setMode(OperationMode.STREAMING);
        ComputeGraph preprocesingTaskGraph = preprocessingTaskGraphBuilder.build();
        return preprocesingTaskGraph;
    }
}
