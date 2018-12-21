package edu.iu.dsc.comms;

import edu.iu.dsc.comms.akeyed.AKeyedRunner;
import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.common.config.Config;

public class CommsRunner {

    private Config config;
    private int containers;
    private JobConfig jobConfig;
    private boolean stream = false;
    private String op;
    private int parallelism;
    private boolean keyed = false;

    public CommsRunner(Config config, JobConfig jobConfig, boolean stream, String op, int parallelism, boolean keyed) {
        this.config = config;
        this.jobConfig = jobConfig;
        this.stream = stream;
        this.op = op;
        this.parallelism = parallelism;
        this.keyed = keyed;
    }

    public void run() {
        System.out.println("==========comms run===========");
        if(keyed) {

        }else {
            AKeyedRunner aKeyedRunner = new AKeyedRunner(config, jobConfig, stream, op, parallelism);
            aKeyedRunner.run();
        }
    }


}
