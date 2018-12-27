package edu.iu.dsc.comms.keyed;

import edu.iu.dsc.comms.common.Submitter;
import edu.iu.dsc.comms.keyed.examples.BKeyedGatherExample;
import edu.iu.dsc.comms.keyed.examples.BKeyedPartitionBasedReduceExample;
import edu.iu.dsc.comms.keyed.examples.BKeyedReduceExample;
import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.common.config.Config;

public class KeyedRunner {

    private Config config;
    private int containers;
    private JobConfig jobConfig;
    private String clazz;
    private boolean stream = false;
    private String op;
    private int parallelism;

    public KeyedRunner(Config config, JobConfig jobConfig, boolean stream, String op, int parallelism) {
        this.config = config;
        this.jobConfig = jobConfig;
        this.stream = stream;
        this.op = op;
        this.parallelism = parallelism;
    }

    public void run() {
        if (!stream) {
            switch (this.op) {
                case "keyedreduce":
                    Submitter.submitJob(config, parallelism, jobConfig, BKeyedReduceExample.class.getName());
                    break;
                case "keyedgather":
                    Submitter.submitJob(config, parallelism, jobConfig, BKeyedGatherExample.class.getName());
                    break;
                case "pkeyedreduce":
                    Submitter.submitJob(config, parallelism, jobConfig, BKeyedPartitionBasedReduceExample.class.getName());
                    break;
            }
        } else {

        }

    }
}
