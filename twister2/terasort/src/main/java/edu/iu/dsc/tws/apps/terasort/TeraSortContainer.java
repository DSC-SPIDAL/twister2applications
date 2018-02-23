package edu.iu.dsc.tws.apps.terasort;//  Licensed under the Apache License, Version 2.0 (the "License");
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

import edu.iu.dsc.tws.apps.terasort.utils.DataLoader;
import edu.iu.dsc.tws.apps.terasort.utils.DataPartitioner;
import edu.iu.dsc.tws.apps.terasort.utils.Record;
import edu.iu.dsc.tws.apps.terasort.utils.Utils;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.*;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.gather.GatherBatchFinalReceiver;
import edu.iu.dsc.tws.comms.mpi.io.gather.GatherBatchPartialReceiver;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import org.apache.hadoop.io.Text;

import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Example that performs tera sort
 */
public class TeraSortContainer implements IContainer {
    private static final Logger LOG = Logger.
            getLogger(TeraSortContainer.class.getName());
    private String inputFolder;
    private String filePrefix;
    private String outputFolder;

    private int partitionSampleNodes;
    private int partitionSamplesPerNode;
    private List<Integer> sampleNodes;

    private int id;

    private Config config;
    private ResourcePlan resourcePlan;
    private static final int NO_OF_TASKS = 8;

    private int noOfTasksPerExecutor = 2;
    private long startTime = 0;

    //Operations
    private DataFlowOperation samplesGather;
    private DataFlowOperation keyBroadCast;
    private DataFlowOperation partitionOp;
    private boolean samplingDone = false;
    private List<Record[]> sampleData;

    private boolean broadcastDone = false;
    private Text[] selectedKeys;
    private PartitionTree tree;

    @Override
    public void init(Config cfg, int containerId, ResourcePlan plan) {
        this.config = cfg;
        this.id = containerId;
        this.resourcePlan = plan;
        sampleNodes = new ArrayList<>();
        this.noOfTasksPerExecutor = NO_OF_TASKS / plan.noOfContainers();
        //Need to get this from the Config
        inputFolder = cfg.getStringValue("input");
        outputFolder = cfg.getStringValue("output");
        partitionSampleNodes = cfg.getIntegerValue("partitionSampleNodes", 1);
        partitionSamplesPerNode = cfg.getIntegerValue("partitionSamplesPerNode", 10000);
        filePrefix = cfg.getStringValue("filePrefix");
        System.out.println(inputFolder + " : " + partitionSampleNodes);
        // lets create the task plan
        TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, plan, NO_OF_TASKS);
        //first get the communication config file
        TWSNetwork network = new TWSNetwork(cfg, taskPlan);

        TWSCommunication channel = network.getDataFlowTWSCommunication();

        Set<Integer> sources = new HashSet<>();
        Set<Integer> dests = new HashSet<>();
        for (int i = 0; i < NO_OF_TASKS; i++) {
            sources.add(i);
            dests.add(i);
        }
        int dest = NO_OF_TASKS;
        Map<String, Object> newCfg = new HashMap<>();

        samplesGather = channel.gather(newCfg, MessageType.OBJECT, 0, sources,
                dest, new GatherBatchFinalReceiver(new SamplesCollectionReceiver()),
                new GatherBatchPartialReceiver(dest));

        for (int i = 0; i < noOfTasksPerExecutor; i++) {
            int taskId = i;
            LOG.info(String.format("%d Starting %d", id, i + id * noOfTasksPerExecutor));
            Thread mapThread = new Thread(new SampleKeyCollection(i + id * noOfTasksPerExecutor, taskId));
            mapThread.start();
        }

        Thread progressSample = new Thread(new ProgressThread(channel, samplesGather));
        progressSample.start();
        Text[] selected = new Text[0];
        if(id == 0){
            while (!samplingDone){
                Thread.yield();
            }
//            System.out.println("Got to results at : " + id );
//            LOG.info("Gather results (only the first int of each array)"
//                    + sampleData.size());
            selected = getSelectedKeys(sampleData);
        }else{
            System.out.println("Other process to results at : " + id );
        }

        //Not lets start the threads to get the records from the previous step

        keyBroadCast = channel.broadCast(newCfg, MessageType.OBJECT, 0, dest,
                sources, new BCastReceive());

        if(id == 0){
            LOG.info(String.format("%d Starting Boardcast thread", id));
            Thread mapThread = new Thread(new BoardCastKeys(NO_OF_TASKS, selected));
            mapThread.start();
        }

        Thread progressBroadcast = new Thread(new ProgressThread(null, keyBroadCast));
        progressBroadcast.start();

        while (!broadcastDone){
            Thread.yield();
        }

        TaskPlan taskPlan2 = Utils.createReduceTaskPlan(cfg, plan, NO_OF_TASKS);

        TWSNetwork network2 = new TWSNetwork(cfg, taskPlan2);

        TWSCommunication channel2 = network2.getDataFlowTWSCommunication();
        //Completed broadbast
        partitionOp = channel2.partition(newCfg,MessageType.OBJECT, 0, sources, dests,new FinalPartitionReceiver());
        // now lets read all the data and distribute them to the correct tasks
        for (int i = 0; i < noOfTasksPerExecutor; i++) {
            int taskId = i;
            LOG.info(String.format("%d Starting Distributer %d", id, i + id * noOfTasksPerExecutor));
            Thread mapThread = new Thread(new DistributeData(i + id * noOfTasksPerExecutor, taskId));
            mapThread.start();
        }


        Thread progressPartition = new Thread(new ProgressThread(channel2, partitionOp));
        progressPartition.start();

        while (true){
            //Waiting to make sure this thread does not die
            Thread.yield();
        }
    }

    private class ProgressThread implements Runnable {
        private TWSCommunication channel;
        private DataFlowOperation operation;

        public ProgressThread(TWSCommunication channel, DataFlowOperation operation) {
            this.channel = channel;
            this.operation = operation;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    // progress the channel
                    if(channel != null){
                        this.channel.progress();
                    }
                    // we should progress the communication directive
                    this.operation.progress();
                    Thread.yield();
                } catch (Throwable t) {
                    LOG.log(Level.SEVERE, "Something bad happened", t);
                }
            }
        }
    }

    private Text[] getSelectedKeys(List<Record[]> records){
        List<Record> partitionRecordList = new ArrayList<>();
        for (Record[] recordList : records) {
            for (Record record : recordList) {
                partitionRecordList.add(record);
            }
        }
//        System.out.println("Total number of sample records : " + partitionRecordList.size());
        int noOfSelectedKeys = NO_OF_TASKS - 1;
//        byte[] selectedKeys = new byte[Record.KEY_SIZE * noOfSelectedKeys];
        Text[] selectedKeys = new Text[noOfSelectedKeys];
        //Sort the collected records
        Collections.sort(partitionRecordList);
        int div = partitionRecordList.size() / NO_OF_TASKS;
        for (int i = 0; i < noOfSelectedKeys; i++) {
//            System.arraycopy(partitionRecordList.get((i + 1) * div).getKey().getBytes(), 0,
//                    selectedKeys, i * Record.KEY_SIZE, Record.KEY_SIZE);
            selectedKeys[i] = partitionRecordList.get((i + 1) * div).getKey();
        }
        return selectedKeys;
    }
    private PartitionTree buildPartitionTree(List<Record[]> records, DataFlowOperation samplesGather) {
        // first create the partitionOp communicator
//        if (rank < partitionSampleNodes) {
//            partitionCom = MPI.COMM_WORLD.split(0, rank);
//        } else {
//            partitionCom = MPI.COMM_WORLD.split(1, rank);
//        }
//
        List<Record> partitionRecordList = new ArrayList<>();
        for (Record[] recordList : records) {
            for (Record record : recordList) {
                partitionRecordList.add(record);
            }
        }
        System.out.println("Total number of partitions : " + partitionRecordList.size());
        DataPartitioner partitioner = new DataPartitioner(samplesGather, partitionSamplesPerNode, NO_OF_TASKS);
        byte[] selectedKeys = partitioner.execute(partitionRecordList);

        int noOfPartitions = NO_OF_TASKS - 1;
        if (selectedKeys.length / Record.KEY_SIZE != noOfPartitions) {
            String msg = "Selected keys( " + selectedKeys.length / Record.KEY_SIZE
                    + " ) generated is not equal to: " + noOfPartitions;
            LOG.log(Level.SEVERE, msg);
            throw new RuntimeException(msg);
        }
        // now build the tree
        Text[] partitions = new Text[noOfPartitions];
        for (int i = 0; i < noOfPartitions; i++) {
            Text t = new Text();
            t.set(selectedKeys, i * Record.KEY_SIZE, Record.KEY_SIZE);

            partitions[i] = t;
        }

        PartitionTree.TrieNode root = PartitionTree.buildTrie(partitions, 0, partitions.length, new Text(), 2);
        return new PartitionTree(root);
    }

    /**
     * This task is used to collect samples from each data partitionOp
     * these collected records are used to create the key based partitiions
     */
    private class SampleKeyCollection implements Runnable {
        private int task = 0;
        private int localId = 0;
        private int sendCount = 0;

        SampleKeyCollection(int task, int local) {
            this.task = task;
            this.localId = local;
        }

        @Override
        public void run() {
            try {
                LOG.log(Level.INFO, "Starting map worker: " + id);
//      MPIBuffer data = new MPIBuffer(1024);
                startTime = System.nanoTime();
                String inputFile = Paths.get(inputFolder, filePrefix
                        + id + "_" + Integer.toString(localId)).toString();
                List<Record> records = DataLoader.load(id, inputFile, partitionSamplesPerNode);
                Record[] partitionRecords = new Record[partitionSamplesPerNode];
                for (int i = 0; i < partitionSamplesPerNode; i++) {
                    partitionRecords[i] = records.get(i);
                }

                int flags = MessageFlags.FLAGS_LAST;
                while (!samplesGather.send(task, partitionRecords, flags)) {
                    // lets wait a litte and try again
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                Thread.yield();
//                System.out.println("######## : Container id : " + id + "local id : "
//                        + localId + " Records : " + records.size());

                LOG.info(String.format("%d Done sending", id));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    private class BoardCastKeys implements Runnable{
        Text[] sendData;

        public BoardCastKeys(int taskId, Text[] data){
            this.sendData = data;
        }
        @Override
        public void run() {
            int flags = MessageFlags.FLAGS_LAST;
            while (!keyBroadCast.send(NO_OF_TASKS, sendData, flags)) {
                // lets wait a litte and try again
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    private class DistributeData implements Runnable{
        private int task = 0;
        private int localId = 0;

        public DistributeData(int task, int localId){
            this.task = task;
            this.localId = localId;
        }
        @Override
        public void run() {
            String inputFile = Paths.get(inputFolder, filePrefix
                    + id + "_" + Integer.toString(localId)).toString();
            String outputFile = Paths.get(outputFolder, filePrefix + Integer.toString(id)).toString();
            List<Record> records = DataLoader.load(id, inputFile);
            //sort the local records
            //Collections.sort(records);

            for (int i = 0; i < records.size(); i++) {
                Record text = records.get(i);
                int partition = tree.getPartition(text.getKey());
                if(i % 7000 == 0){
                    System.out.printf("Task id : %d Record : %d Partition number : %d Key value %s \n", task, i, partition, text.getKey().toString());
                    while (!partitionOp.send(task, text, 0, partition)) {
                        // lets wait a litte and try again
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

        }
    }

    private class SamplesCollectionReceiver implements GatherBatchReceiver {
        // lets keep track of the messages
        // for each task we need to keep track of incoming messages
        private List<Record[]> dataList;

        private int count = 0;

        private long start = System.nanoTime();

        @Override
        public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
            dataList = new ArrayList<Record[]>();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void receive(int target, Iterator<Object> it) {
            int itercount = 0;
            Object temp;

            while (it.hasNext()) {
                itercount++;
                temp = it.next();
                if (temp instanceof List) {
                    List<Object> datalist = (List<Object>) temp;
                    for (Object o : datalist) {
                        Record[] data = (Record[]) o;
                        dataList.add(data);
                    }
                } else {
                    dataList.add((Record[]) temp);
                }
            }
            sampleData = dataList;
            samplingDone = true;
            LOG.info("Gather results (only the first int of each array)"
                    + sampleData.size());
        }

        public void progress() {

        }
    }

    private class BCastReceive implements MessageReceiver {
        public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {

        }

        @Override
        public boolean onMessage(int source, int path, int target, int flags, Object object) {
            selectedKeys = (Text[])object;
//            byte[] byteKeys = new byte[Record.KEY_SIZE * selectedKeys.length];
            //calculate the tire
//            for (int i = 0; i < selectedKeys.length; i++) {
//            System.arraycopy(selectedKeys[i].getBytes(), 0,
//                    byteKeys, i * Record.KEY_SIZE, Record.KEY_SIZE);
//            }
            int noOfPartitions = NO_OF_TASKS - 1;
            if (selectedKeys.length != noOfPartitions) {
                String msg = "Selected keys( " + selectedKeys.length
                        + " ) generated is not equal to: " + noOfPartitions;
                LOG.log(Level.SEVERE, msg);
                throw new RuntimeException(msg);
            }
            // now build the tree
//            Text[] partitions = new Text[noOfPartitions];
//            for (int i = 0; i < noOfPartitions; i++) {
//                Text t = new Text();
//                t.set(byteKeys, i * Record.KEY_SIZE, Record.KEY_SIZE);
//
//                partitions[i] = t;
//            }

            PartitionTree.TrieNode root = PartitionTree.buildTrie(selectedKeys, 0, selectedKeys.length, new Text(), 2);
            tree = new PartitionTree(root);
            broadcastDone = true;
            return true;
        }

        @Override
        public void progress() {
        }
    }

    private class FinalPartitionReceiver implements MessageReceiver {

        private long start = System.nanoTime();

        @Override
        public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
        }

        @Override
        public boolean onMessage(int source, int path, int target, int flags, Object object) {
            // add the object to the map
            System.out.printf("Dest Task %d got message from Task %d with value %s \n", target,
                    source, ((Record) object).getKey().toString());
            return true;
        }

        public void progress() {

        }
    }
}
