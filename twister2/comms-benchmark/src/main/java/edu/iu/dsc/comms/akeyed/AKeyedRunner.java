package edu.iu.dsc.comms.akeyed;

import edu.iu.dsc.comms.akeyed.examples.BPartitionExample;
import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;

public class AKeyedRunner {

    private Config config;
    private int containers;
    private JobConfig jobConfig;
    private String clazz;
    private boolean stream = false;
    private String op;
    private int parallelism;

    public AKeyedRunner() {

    }

    public AKeyedRunner(Config config, JobConfig jobConfig, boolean stream, String op, int parallelism) {
        this.config = config;
        this.jobConfig = jobConfig;
        this.stream = stream;
        this.op = op;
        this.parallelism = parallelism;
    }

    public void run(){
        if(!stream) {
            switch (this.op) {
                case "partition":
                    submitJob(config, parallelism, jobConfig, BPartitionExample.class.getName());
                    break;
            }
        } else {

        }

    }

    private static void submitJob(Config config, int containers, JobConfig jobConfig, String clazz) {
        Twister2Job twister2Job;
        twister2Job = Twister2Job.newBuilder()
                .setJobName(clazz)
                .setWorkerClass(clazz)
                .addComputeResource(1, 512, containers)
                .setConfig(jobConfig)
                .build();
        // now submit the job
        Twister2Submitter.submitJob(twister2Job, config);
    }
}
