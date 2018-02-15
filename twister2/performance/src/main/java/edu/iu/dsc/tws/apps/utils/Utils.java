package edu.iu.dsc.tws.apps.utils;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class Utils {
  private static final Logger LOG = Logger.getLogger(Utils.class.getName());

  private Utils() {
  }

  /**
   * Let assume we have 1 task per container
   * @param plan the resource plan from scheduler
   * @return task plan
   */
  public static TaskPlan createTaskPlan(Config cfg, ResourcePlan plan) {
    int noOfProcs = plan.noOfContainers();
    LOG.log(Level.INFO, "No of containers: " + noOfProcs);
    Map<Integer, Set<Integer>> executorToGraphNodes = new HashMap<>();
    Map<Integer, Set<Integer>> groupsToExeuctors = new HashMap<>();
    int thisExecutor = plan.getThisId();

    List<ResourceContainer> containers = plan.getContainers();
    Map<String, List<ResourceContainer>> containersPerNode = new HashMap<>();
    for (ResourceContainer c : containers) {
      String name = (String) c.getProperty("PROCESS_NAME");
      List<ResourceContainer> containerList;
      if (!containersPerNode.containsKey(name)) {
        containerList = new ArrayList<>();
        containersPerNode.put(name, containerList);
      } else {
        containerList = containersPerNode.get(name);
      }
      containerList.add(c);
    }

    for (int i = 0; i < noOfProcs; i++) {
      Set<Integer> nodesOfExecutor = new HashSet<>();
      nodesOfExecutor.add(i);
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

    String print = printMap(executorToGraphNodes);
    LOG.fine("Executor To Graph: " + print);
    print = printMap(groupsToExeuctors);
    LOG.fine("Groups to executors: " + print);
    // now lets create the task plan of this, we assume we have map tasks in all the processes
    // and reduce task in 0th process
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
      String name = (String) c.getProperty("PROCESS_NAME");
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

    String print = printMap(executorToGraphNodes);
    LOG.fine("Executor To Graph: " + print);
    print = printMap(groupsToExeuctors);
    LOG.fine("Groups to executors: " + print);
    // now lets create the task plan of this, we assume we have map tasks in all the processes
    // and reduce task in 0th process
    return new TaskPlan(executorToGraphNodes, groupsToExeuctors, thisExecutor);
  }

  public static String printMap(Map<Integer, Set<Integer>> map) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Integer, Set<Integer>> e : map.entrySet()) {
      sb.append(e.getKey() + " : ");
      for (Integer i : e.getValue()) {
        sb.append(i).append(" ");
      }
      sb.append("\n");
    }
    return sb.toString();
  }
}