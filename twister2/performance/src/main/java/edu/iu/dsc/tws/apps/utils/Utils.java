package edu.iu.dsc.tws.apps.utils;

import edu.iu.dsc.tws.apps.data.DataGenerator;
import edu.iu.dsc.tws.apps.data.DataType;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlanUtils;
import org.apache.commons.cli.Option;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class Utils {
  private static final Logger LOG = Logger.getLogger(Utils.class.getName());

  private Utils() {
  }

  public static TaskPlan createReduceTaskPlan(Config cfg, ResourcePlan plan, List<Integer> noOfTaskEachStage) {
    int noOfContainers = plan.noOfContainers();
    Map<Integer, Set<Integer>> executorToGraphNodes = new HashMap<>();
    Map<Integer, Set<Integer>> groupsToExeuctors = new HashMap<>();
    int thisExecutor = plan.getThisId();

    List<ResourceContainer> containers = plan.getContainers();
    Map<String, List<ResourceContainer>> containersPerNode = ResourcePlanUtils.getContainersPerNode(containers);

    int totalTasksPreviously = 0;
    for (int noOfTasks : noOfTaskEachStage) {
      int currentExecutorId = 0;
      for (int i = 0; i < noOfTasks; i++) {
        Set<Integer> nodesOfExecutor;
        if (executorToGraphNodes.get(currentExecutorId) == null) {
          nodesOfExecutor = new HashSet<>();
        } else {
          nodesOfExecutor = executorToGraphNodes.get(currentExecutorId);
        }
        nodesOfExecutor.add(totalTasksPreviously + i);
        executorToGraphNodes.put(currentExecutorId, nodesOfExecutor);
        // we go to the next executor
        currentExecutorId = nextExecutorId(currentExecutorId, noOfContainers);
      }
      totalTasksPreviously += noOfTasks;
    }

    int i = 0;
    for (Map.Entry<String, List<ResourceContainer>> entry : containersPerNode.entrySet()) {
      Set<Integer> executorsOfGroup = new HashSet<>();
      for (ResourceContainer c : entry.getValue()) {
        executorsOfGroup.add(c.getId());
      }
      groupsToExeuctors.put(i, executorsOfGroup);
      i++;
    }

    return new TaskPlan(executorToGraphNodes, groupsToExeuctors, thisExecutor);
  }

  /**
   * Let assume we have 2 tasks per container and one additional for first container,
   * which will be the destination
   * @param plan the resource plan from scheduler
   * @return task plan
   */
  public static TaskPlan createReduceTaskPlan(Config cfg, ResourcePlan plan, int noOfTasks) {
    int noOfProcs = plan.noOfContainers();
    LOG.log(Level.INFO, "No of containers: " + noOfProcs);
    Map<Integer, Set<Integer>> executorToGraphNodes = new HashMap<>();
    Map<Integer, Set<Integer>> groupsToExeuctors = new HashMap<>();
    int thisExecutor = plan.getThisId();

    List<ResourceContainer> containers = plan.getContainers();
    Map<String, List<ResourceContainer>> containersPerNode = new HashMap<>();
    for (ResourceContainer c : containers) {
      String name = (String) c.getProperty(SchedulerContext.WORKER_NAME);
      List<ResourceContainer> containerList;
      if (!containersPerNode.containsKey(name)) {
        containerList = new ArrayList<>();
        containersPerNode.put(name, containerList);
      } else {
        containerList = containersPerNode.get(name);
      }
      containerList.add(c);
    }

    int taskPerExecutor = noOfTasks / noOfProcs;
    for (int i = 0; i < noOfProcs; i++) {
      Set<Integer> nodesOfExecutor = new HashSet<>();
      for (int j = 0; j < taskPerExecutor; j++) {
        nodesOfExecutor.add(i * taskPerExecutor + j);
      }
      if (i == 0) {
        nodesOfExecutor.add(noOfTasks);
      }
      executorToGraphNodes.put(i, nodesOfExecutor);
    }

    int i = 0;
    // we take each container as an executor
    for (Map.Entry<String, List<ResourceContainer>> e : containersPerNode.entrySet()) {
      Set<Integer> executorsOfGroup = new HashSet<>();
      for (ResourceContainer c : e.getValue()) {
        executorsOfGroup.add(c.getId());
      }
      groupsToExeuctors.put(i, executorsOfGroup);
      i++;
    }

    // now lets create the task plan of this, we assume we have map tasks in all the processes
    // and reduce task in 0th process
    return new TaskPlan(executorToGraphNodes, groupsToExeuctors, thisExecutor);
  }

  public static Option createOption(String opt, boolean hasArg, String description, boolean required) {
    Option symbolListOption = new Option(opt, hasArg, description);
    symbolListOption.setRequired(required);
    return symbolListOption;
  }

  private static int nextExecutorId(int current, int noOfContainers) {
    if (current < noOfContainers - 1) {
      return ++current;
    } else {
      return 0;
    }
  }

  public static Set<Integer> getTasksOfExecutor(int exec, TaskPlan plan, List<Integer> noOfTaskEachStage, int stage) {
    Set<Integer> out = new HashSet<>();
    int noOfTasks = noOfTaskEachStage.get(stage);
    int total = 0;
    for (int i = 0; i < stage; i++) {
      total += noOfTaskEachStage.get(i);
    }

    Set<Integer> tasksOfExec = plan.getChannelsOfExecutor(exec);
    for (int i = 0; i < noOfTasks; i++) {
      if (tasksOfExec.contains(i + total)) {
        out.add(i + total);
      }
    }
    return out;
  }

  public synchronized static long getTime() {
    return System.nanoTime();
  }

  public static MessageType getMessageTupe(String type) {
    if (type.equals("int")) {
      return MessageType.INTEGER;
    } else if (type.equals("byte")) {
      return MessageType.BYTE;
    } else if (type.equals("object")) {
      return MessageType.OBJECT;
    }
    return MessageType.OBJECT;
  }

  public static DataType getDataType(String type) {
    if (type.equals("int")) {
      return DataType.INT_ARRAY;
    } else if (type.equals("byte")) {
      return DataType.BYTE_ARRAY;
    } else if (type.equals("object")) {
      return DataType.INT_OBJECT;
    }
    return DataType.INT_OBJECT;
  }

  public static Object generateData(DataType genString, DataGenerator generator) {
    Object data;
    if (genString == DataType.STRING) {
      data = generator.generateStringData();
    } else if (genString == DataType.INT_OBJECT) {
      data = generator.generateData();
    } else if (genString == DataType.BYTE_ARRAY) {
      data = generator.generateByteData();
    } else if (genString == DataType.INT_ARRAY) {
      data = generator.generateIntData(1);
    } else {
      throw new RuntimeException("Un-expected data type");
    }

    return data;
  }
}