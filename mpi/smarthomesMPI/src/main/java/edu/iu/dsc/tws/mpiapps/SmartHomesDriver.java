package edu.iu.dsc.tws.mpiapps;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.InputFormat;
import edu.iu.dsc.tws.data.api.formatters.TextInputFormatter;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;
import edu.iu.dsc.tws.mpiapps.configuration.ConfigurationMgr;
import edu.iu.dsc.tws.mpiapps.configuration.section.SmartHomeSection;
import mpi.MPIException;
import org.apache.commons.cli.*;

import edu.iu.dsc.tws.data.*;
import com.google.common.base.Optional;

import java.awt.print.PrinterAbortException;
import java.nio.ByteOrder;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by pulasthi on 10/18/17.
 */
public class SmartHomesDriver {
    private static Options programOptions = new Options();
    static {
        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_C),
                Constants.CMD_OPTION_LONG_C, true,
                Constants.CMD_OPTION_DESCRIPTION_C);
        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_N),
                Constants.CMD_OPTION_LONG_N, true,
                Constants.CMD_OPTION_DESCRIPTION_N);
        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_T),
                Constants.CMD_OPTION_LONG_T, true,
                Constants.CMD_OPTION_DESCRIPTION_T);

        programOptions.addOption(Constants.CMD_OPTION_SHORT_MMAPS, true, Constants.CMD_OPTION_DESCRIPTION_MMAPS);
        programOptions.addOption(
                Constants.CMD_OPTION_SHORT_MMAP_SCRATCH_DIR, true,
                Constants.CMD_OPTION_DESCRIPTION_MMAP_SCRATCH_DIR);

        programOptions.addOption(
                Constants.CMD_OPTION_SHORT_BIND_THREADS, true,
                Constants.CMD_OPTION_DESCRIPTION_BIND_THREADS);

        programOptions.addOption(
                Constants.CMD_OPTION_SHORT_CPS, true,
                Constants.CMD_OPTION_DESCRIPTION_CPS);
    }
    public static SmartHomeSection config;

    public static void main(String[] args) {
        Optional<CommandLine> parserResult =
                parseCommandLineArguments(args, programOptions);

        if (!parserResult.isPresent()) {
            System.out.println(Constants.ERR_PROGRAM_ARGUMENTS_PARSING_FAILED);
            new HelpFormatter()
                    .printHelp(Constants.PROGRAM_NAME, programOptions);
            return;
        }

        CommandLine cmd = parserResult.get();
        if (!(cmd.hasOption(Constants.CMD_OPTION_LONG_C) &&
                cmd.hasOption(Constants.CMD_OPTION_LONG_N) &&
                cmd.hasOption(Constants.CMD_OPTION_LONG_T))) {
            System.out.println(Constants.ERR_INVALID_PROGRAM_ARGUMENTS);
            new HelpFormatter()
                    .printHelp(Constants.PROGRAM_NAME, programOptions);
            return;
        }

        readConfiguration(cmd);
        //Initial data read to setup parallism
        // read data using twsiter2 data api
        try {
            ParallelOps.setupParallelism(args);

            Config.Builder builder = new Config.Builder();
            builder.put("input.file.path",config.dataFile);
            Config txtFileConf = builder.build();
            Path path = new Path(config.dataFile);
            InputFormat txtInput = new TextInputFormatter(path);
            txtInput.configure(txtFileConf);
            int minSplits = 1;
            Set<String> plugs = new HashSet<>();
            InputSplit[] inputSplits = txtInput.createInputSplits(minSplits);
            InputSplit cur = inputSplits[0];
            txtInput.open(cur);

            //Data Structures needed
            int currentcounter = 0;
            int totalplugsFound = 0;
            Set<Integer> houseIds = new HashSet<Integer>();
//            int[] currentspaces = new int[ParallelOps.worldProcsCount];
//            currentspaces[ParallelOps.worldProcRank] = ParallelOps.plugNumAssigned;

            Set<Integer> filledNodes = new HashSet<Integer>();
            HashMap<String, Integer> assignedRankbyPlug = new HashMap<String, Integer>();
            HashMap<String, Integer> assignedRankbyhouse = new HashMap<String, Integer>();
            HashMap<Integer, Set<Integer>> housedatamappedRanks = new HashMap<Integer, Set<Integer>>();
            for (int i = 0; i < config.numHouses; i++) {
                housedatamappedRanks.put(i, new HashSet<Integer>());
            }


            String line = "";
            line = (String)txtInput.nextRecord(line);
            Pattern splitpattern = Pattern.compile(",");
            String splits[];
            String plugKey = null;
            int count = 0;
            System.out.println("Plug Spaces left" + Arrays.toString(ParallelOps.assignedPlugs));

            while ((line = (String)txtInput.nextRecord(line)) != null){
                count++;
                splits = splitpattern.split(line);
                plugKey = splits[6] + "-" + splits[5] + "-" + splits[4];
                if(!assignedRankbyPlug.containsKey(plugKey)){
                    if(assignedRankbyhouse.containsKey(splits[6])){
                        int temprank = assignedRankbyhouse.get(splits[6]);
                        if(filledNodes.contains(temprank)){
                            assignedRankbyhouse.remove(splits[6]);
                            temprank = (temprank + 1) % ParallelOps.worldProcsCount;
                            while(filledNodes.contains(temprank)){
                                temprank = (temprank + 1) % ParallelOps.worldProcsCount;
                            }

                        }
                        assignedRankbyPlug.put(plugKey,temprank);
                        ParallelOps.assignedPlugs[temprank] -= 1;
                        if(ParallelOps.assignedPlugs[temprank] == 0) {

                            assignedRankbyhouse.remove(splits[6]);
                            filledNodes.add(temprank);
//                            System.out.println("Plug Spaces left 1111" + Arrays.toString(ParallelOps.assignedPlugs));
//                            System.out.println(currentcounter);
//                            System.out.println(filledNodes.size());
                        }
                    }else{

                        while(filledNodes.contains(currentcounter)){

//                            System.out.println("currentCounter : " + currentcounter + " is already filled");
                            currentcounter = (currentcounter + 1) % ParallelOps.worldProcsCount;
                        }
                        assignedRankbyhouse.put(splits[6],currentcounter);
                        int tempHouseId = Integer.parseInt(splits[6]);
                        houseIds.add(tempHouseId);
                        housedatamappedRanks.get(tempHouseId).add(currentcounter);
                        assignedRankbyPlug.put(plugKey,currentcounter);
                        ParallelOps.assignedPlugs[currentcounter] -= 1;
                        if(ParallelOps.assignedPlugs[currentcounter] == 0) {
                            assignedRankbyhouse.remove(splits[6]);
                            filledNodes.add(currentcounter);
                        }
                        currentcounter = (currentcounter + 1) % ParallelOps.worldProcsCount;
                        while(filledNodes.contains(currentcounter)){
                            currentcounter = (currentcounter + 1) % ParallelOps.worldProcsCount;
                        }

                    }

                }
            }
            System.out.println(assignedRankbyPlug.size());
            System.out.println("Plug Spaces left" + Arrays.toString(ParallelOps.assignedPlugs));

            System.out.println("MPI Rank" + ParallelOps.worldProcRank + " :::: " + line);
            System.out.printf(" house count " + houseIds.size());
            System.out.printf(" house ids 0" + housedatamappedRanks.get(0).toString());
            System.out.printf(" house ids 2" + housedatamappedRanks.get(2).toString());
            System.out.printf(" house ids 4" + housedatamappedRanks.get(4).toString());
            ParallelOps.tearDownParallelism();
            System.out.println(count);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void readConfiguration(CommandLine cmd) {
        config = ConfigurationMgr.LoadConfiguration(
                cmd.getOptionValue(Constants.CMD_OPTION_LONG_C)).smartHomesSection;
        ParallelOps.nodeCount =
                Integer.parseInt(cmd.getOptionValue(Constants.CMD_OPTION_LONG_N));
        ParallelOps.threadCount =
                Integer.parseInt(cmd.getOptionValue(Constants.CMD_OPTION_LONG_T));
    }

    /**
     * Parse command line arguments
     *
     * @param args Command line arguments
     * @param opts Command line options
     * @return An <code>Optional&lt;CommandLine&gt;</code> object
     */
    private static Optional<CommandLine> parseCommandLineArguments(
            String[] args, Options opts) {

        CommandLineParser optParser = new GnuParser();

        try {
            return Optional.fromNullable(optParser.parse(opts, args));
        }
        catch (ParseException e) {
            e.printStackTrace();
        }
        return Optional.fromNullable(null);
    }
}
