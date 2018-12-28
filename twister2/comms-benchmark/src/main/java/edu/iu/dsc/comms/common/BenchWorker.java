package edu.iu.dsc.comms.common;

import edu.iu.dsc.tws.api.net.Network;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.exceptions.TimeoutException;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.util.Utils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class BenchWorker implements IWorker {
    private static final Logger LOG = Logger.getLogger(BenchWorker.class.getName());

    private Lock lock = new ReentrantLock();

    protected int workerId;

    protected Config config;

    protected TaskPlan taskPlan;

    protected JobParameters jobParameters;

    protected TWSChannel channel;

    protected Communicator communicator;

    protected Map<Integer, Boolean> finishedSources = new ConcurrentHashMap<>();

    protected boolean sourcesDone = false;

    protected List<JobMasterAPI.WorkerInfo> workerList = null;

    protected ExperimentData experimentData;

    protected long executionTime = 0;

    @Override
    public void execute(Config cfg, int workerID,
                        IWorkerController workerController, IPersistentVolume persistentVolume,
                        IVolatileVolume volatileVolume) {

        // create the job parameters
        executionTime -= System.currentTimeMillis();
        this.jobParameters = JobParameters.build(cfg);
        this.config = cfg;
        this.workerId = workerID;
        try {
            this.workerList = workerController.getAllWorkers();
        } catch (TimeoutException timeoutException) {
            LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
            return;
        }
        // lets create the task plan
        this.taskPlan = Utils.createStageTaskPlan(cfg, workerID,
                jobParameters.getTaskStages(), workerList);
        // create the channel
        channel = Network.initializeChannel(config, workerController);
        // create the communicator
        communicator = new Communicator(cfg, channel);
        //collect experiment data
        experimentData = new ExperimentData();

        // now lets execute
        execute();
        // now communicationProgress
        progress();
        // wait for the sync
        try {
            workerController.waitOnBarrier();
        } catch (TimeoutException timeoutException) {
            LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
        }
        // let allows the specific example to close
        close();
        // lets terminate the communicator
        communicator.close();
        executionTime += System.currentTimeMillis();
        if (workerID == 0) {
            LOG.info("Execution Time : " + executionTime + " ms");
            try {
                LOG.log(Level.INFO, "Writing results to csv");
                CSVWriter.write(
                        "/tmp/results/runtimes.csv",
                        jobParameters,
                        executionTime
                );
            } catch (IOException e) {
                LOG.log(Level.SEVERE, "Failed to write results to file");
            }
        }
    }

    protected abstract void execute();

    protected void progress() {
        int count = 0;
        // we need to progress the communication

        while (!isDone()) {
            // communicationProgress the channel
            channel.progress();
            // we should communicationProgress the communication directive
            progressCommunication();
        }
    }

    protected abstract void progressCommunication();

    protected abstract boolean isDone();

    protected abstract boolean sendMessages(int task, Object data, int flag);

    public void close() {
    }

    protected void finishCommunication(int src) {
    }

    protected Object generateData() {
        return DataGenerator.generateIntData(jobParameters.getSize());
    }

    protected class MapWorker implements Runnable {
        private int task;

        public MapWorker(int task) {
            this.task = task;
        }

        @Override
        public void run() {
            //LOG.log(Level.INFO, "Starting map worker: " + workerId + " task: " + task);
            Object data = generateData();
            experimentData.setInput(data);
            experimentData.setTaskStages(jobParameters.getTaskStages());
            experimentData.setIterations(jobParameters.getIterations());
            if (jobParameters.isStream()) {
                experimentData.setOperationMode(OperationMode.STREAMING);
            } else {
                experimentData.setOperationMode(OperationMode.BATCH);
            }

            for (int i = 0; i < jobParameters.getIterations(); i++) {
                // lets generate a message
                int flag = 0;
                if (i == jobParameters.getIterations() - 1) {
                    flag = MessageFlags.LAST;
                }
                sendMessages(task, data, flag);
            }
            //LOG.info(String.format("%d Done sending", workerId));
            lock.lock();
            boolean allDone = true;
            finishedSources.put(task, true);
            for (Map.Entry<Integer, Boolean> e : finishedSources.entrySet()) {
                if (!e.getValue()) {
                    allDone = false;
                }
            }
            finishCommunication(task);
            sourcesDone = allDone;
            lock.unlock();
        }
    }
}