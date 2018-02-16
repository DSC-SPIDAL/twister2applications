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
import mpi.MPI;
import mpi.MPIException;
import org.apache.commons.cli.*;

import edu.iu.dsc.tws.data.*;
import com.google.common.base.Optional;

import java.awt.print.PrinterAbortException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by pulasthi on 10/18/17.
 */
public class SmartHomesDriver {
    public static Options programOptions = new Options();
    public static int startTime = 1377986401;
    public static int basicSlice = 60;
    public static int basicSliceCount = 1440;
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
            HashMap<String, Map<Integer,Map<Integer, MedianHeap>>> localPlugsMedians = new HashMap<String, Map<Integer,Map<Integer, MedianHeap>>>();
            HashMap<Integer, Map<Integer,Map<Integer, MedianHeap>>> houseMedians = new HashMap<Integer, Map<Integer, Map<Integer,MedianHeap>>>();
            addSliceMedianMapHouses(houseMedians);

            HashMap<String, double[]> localCounters = new HashMap<String, double[]>();
            double[][] houseSumsbySlice = new double[basicSliceCount][config.numHouses];
            double[] houseSumsReducedCurrSlice = new double[config.numHouses];
            double[] houseSumsaccu = new double[config.numHouses*basicSliceCount];
            int houseSumsCount = 1;
            System.out.println(config.houseOutStream);
            BufferedWriter out = new BufferedWriter(new FileWriter(config.houseOutStream), 400000);
            out.write("Test the files write this");
            int houseTime = startTime;
            int houseSliceCount = 0;

            String line = "";
            line = (String)txtInput.nextRecord(line);
            Pattern splitpattern = Pattern.compile(",");
            String splits[];
            String plugKey = null;
            String houseKey = null;
            int houseKeyInt = 0;
            int property = 0;
            int timeStamp = 0;
            int count = 0;
            int currentTime = startTime;
            double tempLoadsum = 0.0;
            double load = 0.0;
            int templastid = 0;
            int tempsliceCount = 0;
            while ((line = (String)txtInput.nextRecord(line)) != null){
                count++;
                splits = splitpattern.split(line);
                houseKey = splits[6];
                houseKeyInt = Integer.parseInt(houseKey);
                plugKey = houseKey + "-" + splits[5] + "-" + splits[4];
                property = Integer.valueOf(splits[3]);
                timeStamp = Integer.valueOf(splits[1]);
                load = Double.parseDouble(splits[2]);
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
                            localPlugsMedians.put(plugKey,addSliceMedianMap());
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
                            localPlugsMedians.put(plugKey,addSliceMedianMap());
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

                if((timeStamp - houseTime) == basicSlice){
                    ParallelOps.allReduceBuff(houseSumsbySlice[houseSliceCount], MPI.SUM, houseSumsReducedCurrSlice);
                    for (int i = 0; i < config.numHouses; i++) {
                        houseMedians.get(houseKeyInt).get(config.slices[0]).get(houseSliceCount).insertElement(houseSumsReducedCurrSlice[i]);
                    }
                    // ParallelOps.allReduceBuff(houseSums, MPI.SUM, houseSumsaccu);// the equal will also go into the house values rather than the next slice need to fix
                    if(ParallelOps.worldProcRank == 0){
                        // if rank is 0 write out the stream to the file.
                        calculateAndWriteHousePred(out, houseTime, houseSliceCount, houseSumsReducedCurrSlice,houseMedians);
                    }
                    houseSliceCount = (houseSliceCount + 1) % basicSliceCount; //need to be sure that seconds are not missing in the data
                    houseTime += basicSlice;
                    houseSumsCount += 1;
                }

                if(assignedRankbyPlug.get(plugKey) == ParallelOps.worldProcRank && property == 1 ){
                    Map<Integer,Map<Integer, MedianHeap>> tempMedian = localPlugsMedians.get(plugKey);
                    double[] countertemp = localCounters.get(plugKey);



                    if(timeStamp - countertemp[1] >= basicSlice){ // TODO: check if this handles missing data
                        tempMedian.get(config.slices[0]).get((int)countertemp[2]).insertElement(countertemp[0]);
                        countertemp[0] = load;
                        houseSumsbySlice[houseSliceCount][houseKeyInt] = load;
                        countertemp[1] += basicSlice * (int)((timeStamp - countertemp[1])/basicSlice); // if more than 1 time slices is jumped
                        countertemp[2] = (countertemp[2] + 1) % basicSliceCount;
                    }else{

                        countertemp[0] += load;
                        houseSumsbySlice[houseSliceCount][houseKeyInt] += load;
                    }

                }


            }
            out.close();
            ParallelOps.tearDownParallelism();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void calculateAndWritePlugPred(BufferedWriter out, int currentSliceTimeStamp, int curSlice, double curLoad,HashMap <Integer, Map<Integer, Map<Integer, MedianHeap>>> plugMedians) throws IOException {
        int predTimeStamp = currentSliceTimeStamp + 2*basicSlice;
        int predSlice = (curSlice + 2) % basicSliceCount;
        double pred = 0.0;
        double tempmeadin = 0.0;

    }
    private static void calculateAndWriteHousePred(BufferedWriter out, int currentSliceTimeStamp, int curSlice, double curLoad[], HashMap<Integer, Map<Integer, Map<Integer, MedianHeap>>> houseMedians) throws IOException {
        int predTimeStamp = currentSliceTimeStamp + 2*basicSlice;
        int predSlice = (curSlice + 2) % basicSliceCount;
        double pred = 0.0;
        double tempmeadin = 0.0;
        for (int i = 0; i < config.numHouses; i++) {
            tempmeadin = houseMedians.get(i).get(config.slices[0]).get(predSlice).getMedian();
            pred = curLoad[i] + tempmeadin; //If we do not have any historical data do we just use the current load?
            pred /= 2;
            out.write(predTimeStamp + "," + i + "," + String.format("%.3f", pred) + "\n");
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

    private static Map<Integer,Map<Integer, double[]>> addSliceMeansMap(){
        Map<Integer,Map<Integer, double[]>> sliceMeans = new HashMap<Integer,Map<Integer, double[]>>();
        for (int i = 0; i < config.slices.length; i++) {
            int tempslice = config.slices[i];
            Map<Integer, double[]> temp = new HashMap<Integer, double[]>();
            int k = 1440/tempslice;
            for (int j = 0; j < k; j++) {
                temp.put(j,new double[2]);
            }
            sliceMeans.put(tempslice, temp);
        }

        return  sliceMeans;
    }

    private static Map<Integer,Map<Integer, MedianHeap>> addSliceMedianMap(){
        Map<Integer,Map<Integer, MedianHeap>> sliceMeans = new HashMap<Integer,Map<Integer, MedianHeap>>();
        for (int i = 0; i < config.slices.length; i++) {
            int tempslice = config.slices[i];
            Map<Integer, MedianHeap> temp = new HashMap<Integer, MedianHeap>();
            int k = 1440/tempslice;
            for (int j = 0; j < k; j++) {
                temp.put(j,new MedianHeap());
            }
            sliceMeans.put(tempslice, temp);
        }

        return  sliceMeans;
    }

    private static void addSliceMedianMapHouses(HashMap<Integer, Map<Integer, Map<Integer, MedianHeap>>> houseMedians){
        for (int i = 0; i < config.numHouses; i++) {
            houseMedians.put(i, addSliceMedianMap());
        }
    }
}
