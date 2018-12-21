package edu.iu.dsc.comms.akeyed;

import edu.iu.dsc.comms.akeyed.examples.BPartitionExample;
import edu.iu.dsc.comms.common.Submitter;
import edu.iu.dsc.tws.api.JobConfig;
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
                    System.out.println("==========Partition Example selected===========");
                    Submitter.submitJob(config, parallelism, jobConfig, BPartitionExample.class.getName());
                    break;
            }
        } else {

        }

    }


}
