package edu.iu.dsc.tws.mpiapps;

import com.google.common.collect.Interner;
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
    private static int startTime = 1377986401;
    private static int basicSlice = 60;
    private static int basicSliceCount = 1440;
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

            //Data structures required for the processing
            // Set of Hashmaps that keeps the median of the older values one hashmap for each slice type
            HashMap<String, Map<Integer,Map<Integer, Double>>> localPlugsMeans = new HashMap<String, Map<Integer,Map<Integer, Double>>>();
            HashMap<String, double[]> localCounters = new HashMap<String, double[]>();

            String line = "";
            line = (String)txtInput.nextRecord(line);
            Pattern splitpattern = Pattern.compile(",");
            String splits[];
            String plugKey = null;
            String houseKey = null;
            int property = 0;
            int timeStamp = 0;
            int count = 0;
            System.out.println("Plug Spaces left" + Arrays.toString(ParallelOps.assignedPlugs));
            int currentTime = startTime;
            double tempLoadsum = 0.0;
            int tempsliceCount = 0;
            while ((line = (String)txtInput.nextRecord(line)) != null){
                count++;
                splits = splitpattern.split(line);
                houseKey = splits[6];
                plugKey = houseKey + "-" + splits[5] + "-" + splits[4];
                property = Integer.valueOf(splits[3]);
                timeStamp = Integer.valueOf(splits[1]);
                // Code that assigns plugs to processes does not need to run once all is done
                if( !(config.numPlugs == assignedRankbyPlug.size()) && !assignedRankbyPlug.containsKey(plugKey)){
                    if(assignedRankbyhouse.containsKey(houseKey)){
                        int temprank = assignedRankbyhouse.get(houseKey);
                        if(filledNodes.contains(temprank)){
                            assignedRankbyhouse.remove(houseKey);
                            temprank = (temprank + 1) % ParallelOps.worldProcsCount;
                            while(filledNodes.contains(temprank)){
                                temprank = (temprank + 1) % ParallelOps.worldProcsCount;
                            }

                        }
                        assignedRankbyPlug.put(plugKey,temprank);
                        if(temprank == ParallelOps.worldProcRank) {
                            localPlugsMeans.put(plugKey,addSliceMeansMap());
                            localCounters.put(plugKey,new double[]{0.0,startTime,tempsliceCount});
                        }
                        ParallelOps.assignedPlugs[temprank] -= 1;
                        if(ParallelOps.assignedPlugs[temprank] == 0) {

                            assignedRankbyhouse.remove(houseKey);
                            filledNodes.add(temprank);
                        }
                    }else{

                        while(filledNodes.contains(currentcounter)){

//                            System.out.println("currentCounter : " + currentcounter + " is already filled");
                            currentcounter = (currentcounter + 1) % ParallelOps.worldProcsCount;
                        }
                        assignedRankbyhouse.put(houseKey,currentcounter);
                        int tempHouseId = Integer.parseInt(houseKey);
                        houseIds.add(tempHouseId);
                        housedatamappedRanks.get(tempHouseId).add(currentcounter);
                        assignedRankbyPlug.put(plugKey,currentcounter);
                        if(currentcounter == ParallelOps.worldProcRank) {
                            localPlugsMeans.put(plugKey,addSliceMeansMap());
                            localCounters.put(plugKey,new double[]{0.0,startTime,tempsliceCount});
                        }
                        ParallelOps.assignedPlugs[currentcounter] -= 1;
                        if(ParallelOps.assignedPlugs[currentcounter] == 0) {
                            assignedRankbyhouse.remove(houseKey);
                            filledNodes.add(currentcounter);
                        }
                        currentcounter = (currentcounter + 1) % ParallelOps.worldProcsCount;
                        while(filledNodes.contains(currentcounter)){
                            currentcounter = (currentcounter + 1) % ParallelOps.worldProcsCount;
                        }

                    }

                }



                //Atcuall processing of each plug data and summarizing
                // ONly hanldes plugs that are assined to it and if property is work no need to process for now

                if(assignedRankbyPlug.get(plugKey) == ParallelOps.worldProcRank && property == 1 ){
                    Map<Integer,Map<Integer, Double>> temp = localPlugsMeans.get(plugKey);
                    double[] countertemp = localCounters.get(plugKey);
                    if(timeStamp - countertemp[1] >= basicSlice){ // TODO: check if this handles missing data
                       // if(timeStamp - currentTime > basicSlice){

                       // }
                        //if(plugKey.equals("0-0-11")) System.out.println("basic slice " + countertemp[2] + "   ++ " + (timeStamp - countertemp[1]));
                        temp.get(config.slices[0]).put((int)countertemp[2],temp.get(config.slices[0]).get((int)countertemp[2]) + countertemp[0]);
                        countertemp[0] = Double.parseDouble(splits[2]);
                        countertemp[1] += basicSlice * (int)(timeStamp - countertemp[1])/basicSlice; // if more than 1 time slices is jumped
                        countertemp[2] = (countertemp[2] + 1) % basicSliceCount;
                        // need to do calculations for other slices
                    }else{

                        countertemp[0] += Double.parseDouble(splits[2]);
                    }

                }
            }

            //Print test counts of means
            System.out.println("ttttttttttttttttttt");
            if(assignedRankbyPlug.get("0-0-11") == ParallelOps.worldProcRank){
                localPlugsMeans.get("0-0-11").get(config.slices[0]).forEach((K,V) -> {
                    System.out.println("slice " + K + " : " + V);
                });
            }

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

    private static Map<Integer,Map<Integer, Double>> addSliceMeansMap(){
        Map<Integer,Map<Integer, Double>> sliceMeans = new HashMap<Integer,Map<Integer, Double>>();
        for (int i = 0; i < config.slices.length; i++) {
            int tempslice = config.slices[i];
            Map<Integer, Double> temp = new HashMap<Integer, Double>();
            int k = 1440/tempslice;
            for (int j = 0; j < k; j++) {
                temp.put(j,0.0);
            }
            sliceMeans.put(tempslice, temp);
        }

        return  sliceMeans;
    }
}
