package edu.iu.dsc.tws.apps.stream;

import edu.iu.dsc.tws.apps.common.RandomString;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.apps.utils.Utils;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.KeyedContent;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Gather implements IContainer {
  private static final Logger LOG = Logger.getLogger(Gather.class.getName());

  private DataFlowOperation aggregate;
  private int id;
  private static final int NO_OF_TASKS = 8;
  private RandomString randomString;
  private long startTime = 0;

  @Override
  public void init(Config cfg, int containerId, ResourcePlan plan) {
    JobParameters jobParameters = JobParameters.build(cfg);
    this.id = containerId;
    int noOfTasksPerExecutor = NO_OF_TASKS / plan.noOfContainers();
    this.randomString = new RandomString(128000, new Random(), RandomString.alphanum);

    // lets create the task plan
    TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, plan, jobParameters.getTaskStages());
    //first get the communication config file
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);

    TWSCommunication channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    for (int i = 0; i < NO_OF_TASKS; i++) {
      sources.add(i);
    }
    int dest = NO_OF_TASKS;
    Map<String, Object> newCfg = new HashMap<>();

    try {
      // this method calls the init method
      // I think this is wrong
      aggregate = channel.gather(newCfg, MessageType.OBJECT, MessageType.INTEGER,  0, sources,
          dest, new FinalGatherReceive());

      for (int i = 0; i < noOfTasksPerExecutor; i++) {
        // the map thread where data is produced
        LOG.info(String.format("%d Starting %d", id, i + id * noOfTasksPerExecutor));
        Thread mapThread = new Thread(new MapWorker(i + id * noOfTasksPerExecutor));
        mapThread.start();
      }
      // we need to progress the communication
      while (true) {
        try {
          // progress the channel
          channel.progress();
          // we should progress the communication directive
          aggregate.progress();
          Thread.yield();
        } catch (Throwable t) {
          t.printStackTrace();
        }
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  /**
   * We are running the map in a separate thread
   */
  private class MapWorker implements Runnable {
    private int task = 0;
    MapWorker(int task) {
      this.task = task;
    }

    @Override
    public void run() {
      try {
        LOG.log(Level.INFO, "Starting map worker: " + id);
        startTime = System.nanoTime();
        for (int i = 0; i < 1; i++) {
          String data = generateStringData();
          // lets generate a message
          KeyedContent mesage = new KeyedContent(task, data,
              MessageType.INTEGER, MessageType.OBJECT);
          while (!aggregate.send(task, mesage, 0)) {
            // lets wait a litte and try again
            try {
              Thread.sleep(1);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          Thread.yield();
        }
        LOG.info(String.format("%d Done sending", id));
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

  private class FinalGatherReceive implements MessageReceiver {
    // lets keep track of the messages
    // for each task we need to keep track of incoming messages
    private Map<Integer, Map<Integer, List<Object>>> messages = new HashMap<>();
    private Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();

    private int count = 0;

    private long start = System.nanoTime();

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        Map<Integer, List<Object>> messagesPerTask = new HashMap<>();
        Map<Integer, Integer> countsPerTask = new HashMap<>();

        for (int i : e.getValue()) {
          messagesPerTask.put(i, new ArrayList<Object>());
          countsPerTask.put(i, 0);
        }

        LOG.info(String.format("%d Final Task %d receives from %s",
            id, e.getKey(), e.getValue().toString()));

        messages.put(e.getKey(), messagesPerTask);
        counts.put(e.getKey(), countsPerTask);
      }
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      // add the object to the map
      boolean canAdd = true;
      if (count == 0) {
        start = System.nanoTime();
      }

      try {
        List<Object> m = messages.get(target).get(source);
        if (messages.get(target) == null) {
          throw new RuntimeException(String.format("%d Partial receive error %d", id, target));
        }
        Integer c = counts.get(target).get(source);
        if (m.size() > 16) {
          LOG.info(String.format("%d Final true: target %d source %d %s",
              id, target, source, counts));
          canAdd = false;
        } else {
          LOG.info(String.format("%d Final false: target %d source %d %s",
              id, target, source, counts));
          m.add(object);
          counts.get(target).put(source, c + 1);
        }

        return canAdd;
      } catch (Throwable t) {
        t.printStackTrace();
      }
      return true;
    }

    public void progress() {
      for (int t : messages.keySet()) {
        boolean canProgress = true;
        while (canProgress) {
          // now check weather we have the messages for this source
          Map<Integer, List<Object>> map = messages.get(t);
          Map<Integer, Integer> cMap = counts.get(t);
          boolean found = true;
          Object o = null;
          for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
            if (e.getValue().size() == 0) {
              found = false;
              canProgress = false;
            } else {
              o = e.getValue().get(0);
            }
          }
          if (found) {
            for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
              o = e.getValue().remove(0);
            }
            for (Map.Entry<Integer, Integer> e : cMap.entrySet()) {
              Integer i = e.getValue();
              cMap.put(e.getKey(), i - 1);
            }
            if (o != null) {
              count++;
              if (count % 1 == 0) {
                LOG.info(String.format("%d Last %d count: %d %s",
                    id, t, count, counts));
              }
              if (count >= 1) {
                LOG.info("Total time: " + (System.nanoTime() - start) / 1000000
                    + " Count: " + count + " total: " + (System.nanoTime() - startTime));
              }
            } else {
              LOG.severe("We cannot find an object and this is not correct");
            }
          }
        }
      }
    }
  }

  private String generateStringData() {
    return randomString.nextString();
  }
}
