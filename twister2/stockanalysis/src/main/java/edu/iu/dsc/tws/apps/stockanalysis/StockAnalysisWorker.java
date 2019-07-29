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
import edu.iu.dsc.tws.api.task.TaskMessage;
import edu.iu.dsc.tws.api.task.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.apps.stockanalysis.utils.Record;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.TaskGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskWorker;
import edu.iu.dsc.tws.task.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.window.api.WindowMessageImpl;
import edu.iu.dsc.tws.task.window.function.ProcessWindowedFunction;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
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
        if (windowParameters != null) {
            windowLength = windowParameters.getWindowLength();
            slidingLength = windowParameters.getSlidingLength();
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

        DataProcessingSourceTask preprocessingSourceTask = new DataProcessingSourceTask(datainputFile, vectorDirectory,
                startDate);
        DataPreprocessingComputeTask dataPreprocessingCompute = new DataPreprocessingComputeTask(vectorDirectory,
                distanceMatrixDirectory, distanceType, windowLength.intValue(), slidingLength.intValue(),
                startDate, Context.TWISTER2_DIRECT_EDGE);
        DistanceCalculatorComputeTask distanceCalculatorCompute = new DistanceCalculatorComputeTask(vectorDirectory,
                distanceMatrixDirectory, distanceType, Context.TWISTER2_DIRECT_EDGE);
        MDSWorkerComputeTask mdsProgramWorkerCompute = new MDSWorkerComputeTask(Context.TWISTER2_DIRECT_EDGE);
        StockAnalysisSinkTask stockAnalysisSinkTask = new StockAnalysisSinkTask();

        TaskGraphBuilder preprocessingTaskGraphBuilder = TaskGraphBuilder.newBuilder(config);
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
        DataFlowTaskGraph preprocesingTaskGraph = preprocessingTaskGraphBuilder.build();
        return preprocesingTaskGraph;
    }

//    private DataFlowTaskGraph buildStockAnalysisDataflowGraph() {
//
//        DataProcessingSourceTask preprocessingSourceTask = new DataProcessingSourceTask(
//                datainputFile, vectorDirectory, startDate);
//        LOG.info("Window length:" + windowLength + "\t" + slidingLength);
//
//        /*BaseWindowedSink baseWindowedSink
//                = new DataProcessingStreamingWindowCompute(new ProcessWindowFunctionImpl(),
//                OperationMode.STREAMING).withSlidingDurationWindow(windowLength, TimeUnit.DAYS,
//                slidingLength, TimeUnit.DAYS).withCustomTimestampExtractor(new RecordTimestampExtractor())
//                .withWatermarkInterval(7, TimeUnit.DAYS);*/
//
//        BaseWindowedSink baseWindowedSink
//                = new DataProcessingStreamingWindowCompute(new ProcessWindowFunctionImpl())
//                .withCustomTimestampExtractor(new RecordTimestampExtractor())
//                .withAllowedLateness(0, TimeUnit.MILLISECONDS)
//                .withWatermarkInterval(1, TimeUnit.MILLISECONDS)
//                .withSlidingDurationWindow(windowLength, TimeUnit.MILLISECONDS, slidingLength, TimeUnit.MILLISECONDS);
//
//        DataPreprocessingComputeTask dataPreprocessingCompute = new DataPreprocessingComputeTask(
//                vectorDirectory, distanceMatrixDirectory, distanceType, Context.TWISTER2_DIRECT_EDGE);
//        DistanceCalculatorComputeTask distanceCalculatorCompute = new DistanceCalculatorComputeTask(
//                vectorDirectory, distanceMatrixDirectory, distanceType, Context.TWISTER2_DIRECT_EDGE);
//        MDSWorkerComputeTask mdsProgramWorkerCompute = new MDSWorkerComputeTask(Context.TWISTER2_DIRECT_EDGE);
//        StockAnalysisSinkTask stockAnalysisSinkTask = new StockAnalysisSinkTask();
//
//        TaskGraphBuilder preprocessingTaskGraphBuilder = TaskGraphBuilder.newBuilder(config);
//        preprocessingTaskGraphBuilder.setTaskGraphName("StockAnalysisTaskGraph");
//
//        preprocessingTaskGraphBuilder.addSource("preprocessingsourcetask", preprocessingSourceTask, parallel);
//        ComputeConnection windowComputeConnection = preprocessingTaskGraphBuilder.addCompute("windowsink",
//                baseWindowedSink, parallel);
//        ComputeConnection preprocessingComputeConnection = preprocessingTaskGraphBuilder.addCompute(
//                "preprocessingcompute", dataPreprocessingCompute, parallel);
//        ComputeConnection distanceCalculatorComputeConnection = preprocessingTaskGraphBuilder.addCompute(
//                "distancecalculatorcompute", distanceCalculatorCompute, parallel);
//        ComputeConnection mdsComputeConnection = preprocessingTaskGraphBuilder.addCompute(
//                "mdsprogramcompute", mdsProgramWorkerCompute, parallel);
//        ComputeConnection stockAnalysisSinkConnection = preprocessingTaskGraphBuilder.addSink(
//                "dataanalysissink", stockAnalysisSinkTask, parallel);
//
//        windowComputeConnection.direct("preprocessingsourcetask").viaEdge(Context.TWISTER2_DIRECT_EDGE)
//                .withDataType(MessageTypes.OBJECT);
//        preprocessingComputeConnection.direct("windowsink").viaEdge(Context.TWISTER2_DIRECT_EDGE)
//                .withDataType(MessageTypes.OBJECT);
//        distanceCalculatorComputeConnection.direct("preprocessingcompute").viaEdge(Context.TWISTER2_DIRECT_EDGE)
//                .withDataType(MessageTypes.OBJECT);
//        mdsComputeConnection.direct("distancecalculatorcompute").viaEdge(Context.TWISTER2_DIRECT_EDGE)
//                .withDataType(MessageTypes.OBJECT);
//        stockAnalysisSinkConnection.direct("mdsprogramcompute").viaEdge(Context.TWISTER2_DIRECT_EDGE)
//                .withDataType(MessageTypes.OBJECT);
//
//        preprocessingTaskGraphBuilder.setMode(OperationMode.STREAMING);
//        DataFlowTaskGraph preprocesingTaskGraph = preprocessingTaskGraphBuilder.build();
//        return preprocesingTaskGraph;
//    }

    protected static class ProcessWindowFunctionImpl implements ProcessWindowedFunction<EventTimeData> {

        private static final long serialVersionUID = 8517840191276879034L;

        private static final Logger LOG = Logger.getLogger(ProcessWindowFunctionImpl.class.getName());

        @Override
        public IWindowMessage<EventTimeData> process(IWindowMessage<EventTimeData> windowMessage) {
            EventTimeData current = null;
            LOG.info("window message size:" + windowMessage.getWindow().size());
            List<IMessage<EventTimeData>> messages = new ArrayList<>(windowMessage.getWindow().size());
            for (IMessage<EventTimeData> msg : windowMessage.getWindow()) {
                EventTimeData value = msg.getContent();
                if (current == null) {
                    current = value;
                } else {
                    current = add(current, value);
                    messages.add(new TaskMessage<>(current));
                }
            }
            WindowMessageImpl<EventTimeData> windowMessage1 = new WindowMessageImpl<>(messages);
            return windowMessage1;
        }

        @Override
        public EventTimeData onMessage(EventTimeData object1, EventTimeData object2) {
            return add(object1, object2);
        }

        private List<Record> add(Record record1, Record record2) {
           List<Record> recordList = new ArrayList();
           recordList.add(record2);
           return recordList;
        }

        private EventTimeData add(EventTimeData d1, EventTimeData d2) {
            EventTimeData eventTimeData;
            long t1 = d1.getTime();
            long t2 = d2.getTime();
            LOG.info("t1 and t2 value:" + t1 + "\t" + t2);
            List<Record>  data = add(d1.getData(), d2.getData());
            long t = (t1 + t2) / (long) 2.0;
            eventTimeData = new EventTimeData(data, d1.getId(), t);
            return eventTimeData;
        }

        @Override
        public IMessage<EventTimeData> processLateMessage(IMessage<EventTimeData> lateMessage) {
            return null;
        }

        public static String getDateString(Date date) {
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            String month = String.format("%02d", (cal.get(Calendar.MONTH) + 1));
            String day = String.format("%02d", (cal.get(Calendar.DATE)));
            return cal.get(Calendar.YEAR) + month + day;
        }
    }

//    protected static class ProcessWindowFunctionImpl implements ProcessWindowedFunction<Record> {
//
//        private static final long serialVersionUID = 8517840191276879034L;
//
//        private static final Logger LOG = Logger.getLogger(ProcessWindowFunctionImpl.class.getName());
//
//        @Override
//        public IWindowMessage<Record> process(IWindowMessage<Record> windowMessage) {
//            LOG.info("Received Message:" + windowMessage   + "\twindow size" + windowMessage.getWindow().size());
//           /* Record current = null;
//            List<IMessage<Record>> messages = new ArrayList<>(); //windowMessage.getWindow().size()
//            for (IMessage<Record> message : windowMessage.getWindow()) {
//                Record record = message.getContent();
//                LOG.info("Record values are:" + record.getSymbol() + "\t" + record.getDate());
//                if (current == null) {
//                    current = record;
//                } else {
//                    //current = add(current, record);
//                    messages.add(new TaskMessage<>(current));
//                }
//            }
//            WindowMessageImpl<Record> windowMessage1 = new WindowMessageImpl<>(messages);
//            return windowMessage1;*/
//           return windowMessage;
//        }
//
//        @Override
//        public IMessage<Record> processLateMessage(IMessage<Record> lateMessage) {
//            return lateMessage;
//        }
//
//        @Override
//        public Record onMessage(Record object1, Record object2) {
//            return null;
//        }
//
//        private List<Record> add(List<Record> record1, Record record2) {
//            record1.add(record1.size(), record2);
//            return record1;
//        }
//    }
}
