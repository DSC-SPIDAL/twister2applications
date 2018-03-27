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
package edu.iu.dsc.tws.apps.terasort;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.basic.job.BasicJob;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class TeraSortJob {
    private static final Logger LOG = Logger.getLogger(TeraSortJob.class.getName());

    private TeraSortJob() {
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("input", true, "Input directory");
        options.addOption("bsize", true, "Block Size");
        options.addOption("totalTasks", true, "Total Tasks");
        options.addOption("taskPerProc", true, "Tasks per container");
        options.addOption("tasksPerNode", true, "Tasks per Node");
        options.addOption("recordLimit", true, "recordLimit");
        options.addOption("maxRecordsInMemory", true, "maxRecordsInMemory");
        options.addOption("tmpFolder", true, "tmpFolder");
        options.addOption("output", true, "Output directory");
        options.addOption("partitionSampleNodes", true, "Number of nodes to choose partition samples");
        options.addOption("partitionSamplesPerNode",
                true, "Number of samples to choose from each node");
        options.addOption("filePrefix", true, "Prefix of the file partition");
        CommandLineParser commandLineParser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = commandLineParser.parse(options, args);
        } catch (ParseException e) {
            LOG.log(Level.SEVERE, "Failed to read the options", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("program", options);
            throw new RuntimeException(e);
        }


        JobConfig jobConfig = new JobConfig();
        jobConfig.put("input", cmd.getOptionValue("input"));
        jobConfig.put("output", cmd.getOptionValue("output"));
        jobConfig.put("bsize", cmd.getOptionValue("bsize"));
        jobConfig.put("totalTasks", cmd.getOptionValue("totalTasks"));
        jobConfig.put("taskPerProc", cmd.getOptionValue("taskPerProc"));
        jobConfig.put("tasksPerNode", cmd.getOptionValue("tasksPerNode"));
        jobConfig.put("recordLimit", cmd.getOptionValue("recordLimit"));
        jobConfig.put("maxRecordsInMemory", cmd.getOptionValue("maxRecordsInMemory"));
        jobConfig.put("tmpFolder", cmd.getOptionValue("tmpFolder"));
        jobConfig.put("partitionSampleNodes",
                cmd.getOptionValue("partitionSampleNodes"));
        jobConfig.put("partitionSamplesPerNode",
                cmd.getOptionValue("partitionSamplesPerNode"));
        jobConfig.put("filePrefix", cmd.getOptionValue("filePrefix"));
        // first load the configurations from command line and config files
        Config config = ResourceAllocator.loadConfig(new HashMap<>());

        // build JobConfig
        BasicJob.BasicJobBuilder jobBuilder = BasicJob.newBuilder();
        jobBuilder.setName("terasort");
        jobBuilder.setContainerClass(TeraSortContainer5.class.getName());
        jobBuilder.setRequestResource(new ResourceContainer(1, 1024), Integer.valueOf(cmd.getOptionValue("totalTasks")));
        jobBuilder.setConfig(jobConfig);

        // now submit the job
        Twister2Submitter.submitContainerJob(jobBuilder.build(), config);
    }
}
