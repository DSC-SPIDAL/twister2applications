package edu.iu.dsc.tws.apps.terasort;

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

import edu.iu.dsc.tws.apps.terasort.constants.PartitionType;
import edu.iu.dsc.tws.apps.terasort.utils.*;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.*;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.KeyedContent;
import edu.iu.dsc.tws.comms.mpi.io.gather.GatherBatchFinalReceiver;
import edu.iu.dsc.tws.comms.mpi.io.gather.GatherBatchPartialReceiver;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import mpi.MPI;
import mpi.MPIException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TeraSortApp implements IContainer {
    private static final Logger LOG = Logger.
            getLogger(TeraSortApp.class.getName());
    private String inputFolder;
    private String filePrefix;
    private String outputFolder;
    private int maxRecordsInMemory;
    private String tmpFolder;
    private String partitionType;

    private int partitionSampleNodes;
    private int partitionSamplesPerNode;
    private List<Integer> sampleNodes;

    private int id;
    private int workersPerNode;
    private int workerLocalID;

    private Config config;
    private ResourcePlan resourcePlan;
    private static int NO_OF_TASKS = 320;

    private int noOfTasksPerExecutor = 2;
    private int block_size = 25;
    private int recordLimit = 312500 * 2;
    private long startTime = 0;
    private long startTimePartition = 0;
    private long endTimePartition = 0;
    private long completedTasks = 0;

    //Operations
    private DataFlowOperation samplesGather;
    private DataFlowOperation keyBroadCast;
    private DataFlowOperation partitionOp;
    private boolean samplingDone = false;
    private List<Record[]> sampleData;

    private boolean broadcastDone = false;
    private Text[] selectedKeys;
    private PartitionTree tree;

    private boolean reduceDone = false;

    @Override
    public void init(Config cfg, int containerId, ResourcePlan plan) {
        long startTimeTotal = System.currentTimeMillis();
        this.config = cfg;
        this.id = containerId;
        workersPerNode = cfg.getIntegerValue("tasksPerNode", 20);
        NO_OF_TASKS = cfg.getIntegerValue("totalTasks", 320);
        noOfTasksPerExecutor = cfg.getIntegerValue("taskPerProc", 1);
        block_size = cfg.getIntegerValue("bsize", 1);
        recordLimit = cfg.getIntegerValue("recordLimit", 312500 * 2);
        maxRecordsInMemory = cfg.getIntegerValue("maxRecordsInMemory", 100000);
        tmpFolder = cfg.getStringValue("tmpFolder");
        workerLocalID = containerId % workersPerNode;
        this.resourcePlan = plan;
        sampleNodes = new ArrayList<>();
        this.noOfTasksPerExecutor = NO_OF_TASKS / plan.noOfContainers();
        //Need to get this from the Config
        inputFolder = cfg.getStringValue("input");
        outputFolder = cfg.getStringValue("output");
        partitionSampleNodes = cfg.getIntegerValue("partitionSampleNodes", 1);
        partitionSamplesPerNode = cfg.getIntegerValue("partitionSamplesPerNode", 1000);
        filePrefix = cfg.getStringValue("filePrefix");
        partitionType = cfg.getStringValue("partitionType");
        System.out.println(inputFolder + " : " + partitionSampleNodes);
        // lets create the task plan
        TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, plan, NO_OF_TASKS);
        //first get the communication config file
        TWSNetwork network = new TWSNetwork(cfg, taskPlan);

        TWSCommunication channel = network.getDataFlowTWSCommunication();
        // creating the source task and destinations in communication
        Set<Integer> sources = new HashSet<>();
        Set<Integer> dests = new HashSet<>();
        for (int i = 0; i < NO_OF_TASKS; i++) {
            sources.add(i);
            dests.add(i);
        }
        int dest = NO_OF_TASKS;
        Map<String, Object> newCfg = new HashMap<>();
        newCfg.put("network.mpi.receiveBuffer.count", 32);
        newCfg.put("network.mpi.send.pending.max", 32);
        int edgeCount = 0;
        samplesGather = channel.gather(newCfg, MessageType.OBJECT, edgeCount, sources,
                dest, new GatherBatchFinalReceiver(new SamplesCollectionReceiver()),
                new GatherBatchPartialReceiver(dest));

        for (int i = 0; i < noOfTasksPerExecutor; i++) {
            int taskId = i;
            LOG.info(String.format("%d Starting %d", id, i + id * noOfTasksPerExecutor));
            Thread mapThread = new Thread(new TeraSortApp.SampleKeyCollection(i + id * noOfTasksPerExecutor, taskId));
            mapThread.start();
        }

        Thread progressSample = new Thread(new TeraSortApp.ProgressThreadSG(samplesGather));
        progressSample.start();

        Text[] selected = new Text[0];
        if (id == 0) {
            while (!samplingDone) {
                channel.progress();
            }
        }

        if (id == 0) {
            selected = getSelectedKeys(sampleData);
        }

        //Not lets start the threads to get the records from the previous step
        edgeCount++;
        keyBroadCast = channel.broadCast(newCfg, MessageType.OBJECT, edgeCount, dest,
                sources, new TeraSortApp.BCastReceive());

        if (id == 0) {
            LOG.info(String.format("%d Starting Boardcast thread", id));
            Thread mapThread = new Thread(new TeraSortApp.BoardCastKeys(NO_OF_TASKS, selected));
            mapThread.start();
        }
        samplingDone = true;
        Thread progressBroadcast = new Thread(new TeraSortApp.ProgressThreadB(keyBroadCast));
        progressBroadcast.start();

        while (!broadcastDone) {
            channel.progress();
        }
        LOG.info(String.format("%d Completed Boardcast thread", id));
        //Completed broadbast
        edgeCount++;
        Map<Integer, List<Integer>> expectedIds = new HashMap<>();
        for (int i = 0; i < NO_OF_TASKS; i++) {
            expectedIds.put(i, new ArrayList<>());
            for (int j = 0; j < NO_OF_TASKS; j++) {
                if (!(i == j)) {
                    expectedIds.get(i).add(j);

                }
            }
        }
        newCfg = new HashMap<>();
        startTimePartition = System.currentTimeMillis();
        FinalPartitionReceiver finalPartitionRec = new TeraSortApp.FinalPartitionReceiver();
        partitionOp = channel.partition(newCfg, MessageType.MULTI_FIXED_BYTE, MessageType.MULTI_FIXED_BYTE, edgeCount,
                sources, dests, finalPartitionRec);
        finalPartitionRec.setMap(expectedIds);
        LOG.info(String.format("%d Before partitionOp thread memory Map", id));

        for (int i = 0; i < noOfTasksPerExecutor; i++) {
            int taskId = i;
            LOG.info(String.format("%d Starting Distributer %d", id, i + id * noOfTasksPerExecutor));
            Thread mapThread = new Thread(new TeraSortApp.DistributeData(i + id * noOfTasksPerExecutor, taskId, partitionType));
            mapThread.start();
        }

        LOG.info(String.format("%d After partitionOp thread memory Map", id));

        Thread progressPartition = new Thread(new TeraSortApp.ProgressThreadP(partitionOp));
        progressPartition.start();

        while (!reduceDone) {
            channel.progress();
        }
        endTimePartition = System.currentTimeMillis();
        long endTimeTotal = System.currentTimeMillis();
        System.out.println("Time taken for partition Operation : " + (endTimePartition - startTimePartition));
        System.out.println("====================== Total Time taken : " + (endTimeTotal - startTimeTotal));
        System.out.println("Partition Type: " + partitionType);
        System.out.println("Tera Sort Completed...");
        while (true) {
            //Waiting to make sure this thread does not die
            channel.progress();
        }

    }

    private class ProgressThreadSG implements Runnable {
        private DataFlowOperation operation;

        public ProgressThreadSG(DataFlowOperation operation) {
            this.operation = operation;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    // we should progress the communication directive
                    if (!samplingDone) {
                        this.operation.progress();
                        Thread.yield();
                    } else {
                        this.operation.progress();
                        Thread.sleep(1);
                    }

                } catch (Throwable t) {
                    LOG.log(Level.SEVERE, "Something bad happened", t);
                }
            }
        }
    }

    private class ProgressThreadB implements Runnable {
        private DataFlowOperation operation;

        public ProgressThreadB(DataFlowOperation operation) {
            this.operation = operation;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    if (!broadcastDone) {
                        this.operation.progress();
                        Thread.yield();
                    } else {
                        this.operation.progress();
                        Thread.sleep(1);
                    }
                } catch (Throwable t) {
                    LOG.log(Level.SEVERE, "Something bad happened", t);
                }
            }
        }
    }

    private class ProgressThreadP implements Runnable {
        private DataFlowOperation operation;

        public ProgressThreadP(DataFlowOperation operation) {
            this.operation = operation;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    if (!reduceDone) {
                        this.operation.progress();
                        Thread.yield();
                    } else {
                        this.operation.progress();
                        Thread.yield();
//                        Thread.sleep(1);
                    }
                } catch (Throwable t) {
                    LOG.log(Level.SEVERE, "Something bad happened", t);
                }
            }
        }
    }

    private Text[] getSelectedKeys(List<Record[]> records) {
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

     /**
     * This task is used to collect samples from each datacols partitionOp
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
//      MPIBuffer datacols = new MPIBuffer(1024);
                startTime = System.nanoTime();
                String inputFile = Paths.get(inputFolder, filePrefix
                        + workerLocalID + "_" + Integer.toString(localId)).toString();
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

    private class BoardCastKeys implements Runnable {
        Text[] sendData;

        public BoardCastKeys(int taskId, Text[] data) {
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

    private class DistributeData implements Runnable {
        private int task = 0;
        private int localId = 0;
        private String partitionType = "SIMPLE";

        public DistributeData(int task, int localId) {
            this.task = task;
            this.localId = localId;
        }

        public DistributeData(int task, int localId, String partitionType) {
            this.task = task;
            this.localId = localId;
            this.partitionType = partitionType;
        }

        @Override
        public void run() {
            String inputFile = Paths.get(inputFolder, filePrefix
                    + workerLocalID + "_" + Integer.toString(localId)).toString();
            Map<Integer, List<byte[]>> keyMap = new HashMap<>();
            Map<Integer, List<byte[]>> dataMap = new HashMap<>();
            Map<Integer, Integer> countsMap = new HashMap<>();

            for (int i = 0; i < NO_OF_TASKS; i++) {
                keyMap.put(i, new ArrayList<>(block_size));
                dataMap.put(i, new ArrayList<>(block_size));
                countsMap.put(i, 0);
                for (int k = 0; k < block_size; k++) {
                    keyMap.get(i).add(new byte[1]);
                    dataMap.get(i).add(new byte[1]);
                }
            }

            List<byte[]> keyList = new ArrayList<>(block_size);
            List<byte[]> dataList = new ArrayList<>(block_size);
            List<byte[]> recordsKeys = new ArrayList<>();
            List<byte[]> recordsVals = new ArrayList<>();


            // Init the records set
            for (int i = 0; i < recordLimit; i++) {
                recordsKeys.add(new byte[10]);
                recordsVals.add(new byte[90]);
            }

            boolean done = false;
            int loopCount = 0;
            KeyedContent keyedContent = null;
            int partition;
            int localCount;

            Text tempText;
            int countRecords = 0;
            try {
                DataInputStream in = new DataInputStream(
                        new BufferedInputStream(
                                new FileInputStream(new File(inputFile))));
                while (!done) {
                    countRecords = DataLoader.load(in, recordLimit, id, recordsKeys, recordsVals);
                    if (countRecords == 0) {
                        done = true;
                        break;
                    }
                    loopCount++;
                    for (int i = 0; i < countRecords; i++) {
                        tempText = new Text(recordsKeys.get(i));
                        partition = tree.getPartition(tempText);
                        localCount = countsMap.get(partition);
                        countsMap.put(partition, (localCount + 1) % block_size);

                        keyMap.get(partition).set(localCount, recordsKeys.get(i));
                        dataMap.get(partition).set(localCount, recordsVals.get(i));
                        if (localCount == (block_size - 1)) {
                            keyedContent = new KeyedContent(keyMap.get(partition), dataMap.get(partition),
                                    MessageType.MULTI_FIXED_BYTE, MessageType.MULTI_FIXED_BYTE);
                            while (!partitionOp.send(task, keyedContent, 0, partition)) {
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
            } catch (IOException e) {
                LOG.log(Level.SEVERE, "Failed to read the file: " + id, e);
                throw new RuntimeException(e);
            }

            /*
            * The following logic checkes whether there are messages which are not yet transmitted to the
            * corresponding nodes. If the message stacks are not filled with the expected buffered content
            * and if all the messages are already read, the messages left in these buffering queues are being
            * sent to their destinations. This part must be abstracted in the communication layer.
            * **/


            //Send messages to all tasks to let them know that the messages are finished


            int flags = MessageFlags.FLAGS_LAST;
            // for (int i = 0; i < NO_OF_TASKS; i++) {
            int sendCount = 0;
            int start = id;
            // this.partitionType.equals(PartitionType.SIMPLE)
            System.out.println("Partition Type: " + this.partitionType);
            if(this.partitionType.equals(PartitionType.SIMPLE.toString())){

                while (sendCount < NO_OF_TASKS) {
                    //int i = start % NO_OF_TASKS;
                    PartitionCommunication partitionCommunication = new PartitionCommunication();
                    int i = partitionCommunication.onSimpleSelection(start, NO_OF_TASKS);
                    start++;
                    sendCount++;
                    if (i == task) {
                        continue;
                    }
                    int tempCount = countsMap.get(i);
                    if (tempCount == 0 || tempCount == (block_size - 1)) {
                        keyList = new ArrayList<>();
                        dataList = new ArrayList<>();
                        keyList.add(new byte[10]);
                        dataList.add(new byte[90]);
                        keyedContent = new KeyedContent(keyList, dataList,
                                MessageType.MULTI_FIXED_BYTE, MessageType.MULTI_FIXED_BYTE);
                        while (!partitionOp.send(task, keyedContent, flags, i)) {
                            // lets wait a litte and try again
                            partitionOp.progress();
                        }
                    } else {
                        keyList = keyMap.get(i).subList(0, tempCount);
                        dataList = dataMap.get(i).subList(0, tempCount);
                        keyedContent = new KeyedContent(keyList, dataList,
                                MessageType.MULTI_FIXED_BYTE, MessageType.MULTI_FIXED_BYTE);
                        while (!partitionOp.send(task, keyedContent, flags, i)) {
                            // lets wait a litte and try again
                            partitionOp.progress();
                        }
                    }


                }

            }else if(this.partitionType.equals(PartitionType.RANDOM.toString())){

                /*
                TODO: add the random partition logic
                * */

            }else if(this.partitionType.equals(PartitionType.RING.toString())){
                 /*
                TODO: Implement ring partition mechanism
                * */
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
            selectedKeys = (Text[]) object;
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
        private Map<Integer, Map<Integer, Boolean>> finished;
        private String outputFile;
        private long start = System.nanoTime();
        int count = 0;
        long adddatatime = 0;
        FSMergeSorter sorter;
        KeyedContent temp;
        List<ImmutablePair<byte[], byte[]>> tempList;

        @Override
        public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
            finished = new ConcurrentHashMap<>();
            //TODO need to remove last record otherwise valsort will not show correct order
            outputFile = Paths.get(outputFolder, filePrefix + Integer.toString(id)).toString();
            sorter = new FSMergeSorter(id, maxRecordsInMemory, outputFolder, tmpFolder);

            for (Integer integer : expectedIds.keySet()) {
                Map<Integer, Boolean> perTarget = new ConcurrentHashMap<>();
                for (Integer integer1 : expectedIds.get(integer)) {
                    perTarget.put(integer1, false);
                }
                finished.put(integer, perTarget);
            }
        }

        @Override
        public boolean onMessage(int source, int path, int target, int flags, Object object) {
            // add the object to the map
            //LOG.info("On message");
            long stime1 = System.currentTimeMillis();
            if ((flags & MessageFlags.FLAGS_LAST) == MessageFlags.FLAGS_LAST) {
                if (object instanceof KeyedContent) {
                    temp = (KeyedContent) object;
                    sorter.addData(temp);
                } else if (object instanceof List) {
                    tempList = (List<ImmutablePair<byte[], byte[]>>) object;
                    sorter.addData(tempList);
                }
                finished.get(target).put(source, true);
            } else {
                if (object instanceof KeyedContent) {
                    temp = (KeyedContent) object;
                    sorter.addData(temp);

                } else if (object instanceof List) {
                    tempList = (List<ImmutablePair<byte[], byte[]>>) object;
                    sorter.addData(tempList);
                }
            }
            adddatatime += System.currentTimeMillis() - stime1;

            if (((flags & MessageFlags.FLAGS_LAST) == MessageFlags.FLAGS_LAST) && isAllFinished(target)) {
                completedTasks++;
                if (completedTasks == noOfTasksPerExecutor) {
                    /* sorter.doneReceive();
                    long stime = System.currentTimeMillis();
                    sorter.merge();
                    long etime = System.currentTimeMillis();
                    System.out.println("Sort time " + id + " : " + (etime - stime));
                    System.out.println("Add Data time " + id + " : " + adddatatime);
                    */
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            sorter.doneReceive();
                            long stime = System.currentTimeMillis();
                            long sortTime = sorter.merge();
                            long etime = System.currentTimeMillis();
                            System.out.println("Sort time " + id + " : " + (sortTime + (etime - stime)));
                            System.out.println("Add Data time " + id + " : " + adddatatime);
                            reduceDone = true;
                        }
                    }).start();

                }
            }
            count++;

            if (count % 1000 == 0) {
                LOG.info(String.format("%d received %d", id, count));
            }
            return true;
        }

        public void save(Record[] records, String outFileName) {
            DataOutputStream os;
            try {
                os = new DataOutputStream(new FileOutputStream(outFileName));
                for (int i = 0; i < records.length; i++) {
                    Record r = records[i];
                    os.write(r.getKey().getBytes(), 0, Record.KEY_SIZE);
                    os.write(r.getText().getBytes(), 0, Record.DATA_SIZE);
                }
                os.close();
            } catch (IOException e) {
                LOG.log(Level.SEVERE, "Failed write to disc", e);
                throw new RuntimeException(e);
            }
        }

        private boolean isAllFinished(int target) {
            boolean isDone = true;
            for (Boolean bol : finished.get(target).values()) {
                isDone &= bol;
            }
            return isDone;
        }

        public void progress() {

        }

        public void setMap(Map<Integer, List<Integer>> expectedIds) {
            for (Integer integer : expectedIds.keySet()) {
                Map<Integer, Boolean> perTarget = new ConcurrentHashMap<>();
                for (Integer integer1 : expectedIds.get(integer)) {
                    perTarget.put(integer1, false);
                }
                finished.put(integer, perTarget);
            }
        }


    }
}
