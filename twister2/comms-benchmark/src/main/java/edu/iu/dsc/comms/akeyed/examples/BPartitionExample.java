package edu.iu.dsc.comms.akeyed.examples;

import com.google.common.collect.Iterators;
import edu.iu.dsc.comms.common.BenchWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.batch.BPartition;
import edu.iu.dsc.tws.comms.op.selectors.LoadBalanceSelector;
import edu.iu.dsc.util.Utils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BPartitionExample extends BenchWorker {
    private static final Logger LOG = Logger.getLogger(BPartitionExample.class.getName());

    private BPartition partition;

    private boolean partitionDone = false;

    @Override
    protected void execute() {
        TaskPlan taskPlan = Utils.createStageTaskPlan(config, workerId,
                jobParameters.getTaskStages(), workerList);

        Set<Integer> sources = new HashSet<>();
        Set<Integer> targets = new HashSet<>();
        Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
        for (int i = 0; i < noOfSourceTasks; i++) {
            sources.add(i);
        }
        Integer noOfTargetTasks = jobParameters.getTaskStages().get(1);
        for (int i = 0; i < noOfTargetTasks; i++) {
            targets.add(noOfSourceTasks + i);
        }

        // create the communication
        partition = new BPartition(communicator, taskPlan, sources, targets,
                MessageType.INTEGER, new PartitionReceiver(), new LoadBalanceSelector(), true);

        Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
                jobParameters.getTaskStages(), 0);
        // now initialize the workers
        for (int t : tasksOfExecutor) {
            // the map thread where data is produced
            Thread mapThread = new Thread(new BenchWorker.MapWorker(t));
            mapThread.start();
        }
    }

    @Override
    public void close() {
        partition.close();
    }

    @Override
    protected void progressCommunication() {
        partition.progress();
    }

    @Override
    protected boolean isDone() {
        return partitionDone && sourcesDone && !partition.hasPending();
    }

    @Override
    protected boolean sendMessages(int task, Object data, int flag) {
        while (!partition.partition(task, data, flag)) {
            // lets wait a litte and try again
            partition.progress();
        }
        return true;
    }

    public class PartitionReceiver implements BulkReceiver {
        private int count = 0;
        private int expected;

        @Override
        public void init(Config cfg, Set<Integer> expectedIds) {
            expected = jobParameters.getIterations();
        }

        @Override
        public boolean receive(int target, Iterator<Object> it) {
            LOG.log(Level.INFO, String.format("%d Received message %d count %d expected %d",
                    workerId, target, Iterators.size(it), expected));
            partitionDone = true;
            return true;
        }
    }

    @Override
    protected void finishCommunication(int src) {
        partition.finish(src);
    }
}
