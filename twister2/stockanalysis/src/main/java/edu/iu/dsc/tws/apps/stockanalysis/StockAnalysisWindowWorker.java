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
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.task.executor.IExecution;
import edu.iu.dsc.tws.api.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.TaskGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskWorker;
import edu.iu.dsc.tws.task.window.BaseWindowSource;
import edu.iu.dsc.tws.task.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.window.config.SlidingDurationWindow;
import edu.iu.dsc.tws.task.window.config.WindowConfig;
import edu.iu.dsc.tws.task.window.core.BaseWindowedSink;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * This is the starting point for moving window based stock analysis...
 */

public class StockAnalysisWindowWorker extends TaskWorker {

    private static final Logger LOG = Logger.getLogger(StockAnalysisWindowWorker.class.getName());

    protected static final String SOURCE = "source";
    protected static final String SINK = "sink";

    protected static AtomicInteger sendersInProgress = new AtomicInteger();
    protected static AtomicInteger receiversInProgress = new AtomicInteger();
    protected static int[] inputDataArray;

    @Override
    public void execute() {

        int sourceParallelism = 2;
        int sinkParallelism = 2;

        String edgeName = "edge";
        BaseWindowSource g = new SourceWindowTask(edgeName);

        WindowConfig.Duration windowLength = new WindowConfig.Duration(3000, TimeUnit.DAYS);
        WindowConfig.Duration slidingLength = new WindowConfig.Duration(7, TimeUnit.DAYS);

        BaseWindowedSink sdwDuration = new DirectCustomWindowReceiver()
                .withWindow(SlidingDurationWindow.of(windowLength, slidingLength));

        TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
        taskGraphBuilder.setTaskGraphName("StockAnalysisGraph");

        taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
        ComputeConnection computeConnection = taskGraphBuilder.addSink(SINK, sdwDuration,
                sinkParallelism);
        computeConnection.direct(SOURCE)
                .viaEdge(edgeName)
                .withDataType(MessageTypes.INTEGER);
        DataFlowTaskGraph dataFlowTaskGraph = taskGraphBuilder.build();
        ExecutionPlan executionPlan = taskExecutor.plan(dataFlowTaskGraph);
        IExecution execution = taskExecutor.iExecute(dataFlowTaskGraph, executionPlan);

        //if (jobParameters.isStream()) {
        while (execution.progress()
                && (sendersInProgress.get() != 0 || receiversInProgress.get() != 0)) {
            //do nothing
        }
        LOG.info("Stopping execution....");
        execution.stop();
        execution.close();
    }

    private static class SourceWindowTask extends BaseWindowSource {

        private String edgeName;
        private int count = 0;
        private int iterations;
        private boolean keyed = false;
        private boolean endNotified = false;

        public SourceWindowTask(String edgename) {
            this.edgeName = edgename;
        }

        @Override
        public void prepare(Config cfg, TaskContext ctx) {
            super.prepare(cfg, ctx);
            sendersInProgress.incrementAndGet();
        }

        @Override
        public void execute() {
            inputDataArray = new int[] {1, 2};
            if (count < iterations) {
                if ((this.keyed && context.write(this.edgeName, context.taskIndex(), inputDataArray))
                        || (!this.keyed && context.write(this.edgeName, inputDataArray))) {
                    count++;
                }
            } else {
                context.end(this.edgeName);
                this.notifyEnd();
            }
        }

        private void notifyEnd() {
            if (endNotified) {
                return;
            }
            sendersInProgress.decrementAndGet();
            endNotified = true;
            LOG.info(String.format("Source : %d done sending.", context.taskIndex()));
        }
    }

    private static class DirectCustomWindowReceiver extends BaseWindowedSink<int[]> {

        public DirectCustomWindowReceiver() {
        }

        @Override
        public boolean execute(IWindowMessage<int[]> windowMessage) {
            LOG.info(String.format("Items : %d ", windowMessage.getWindow().size()));
            return true;
        }

        @Override
        public boolean getExpire(IWindowMessage<int[]> expiredMessages) {
            return true;
        }

        @Override
        public boolean getLateMessages(IMessage<int[]> lateMessage) {
            LOG.info(String.format("Late Message : %s",
                    lateMessage.getContent() != null ? Arrays.toString(lateMessage.getContent()) : "null"));
            return true;
        }
    }
}
