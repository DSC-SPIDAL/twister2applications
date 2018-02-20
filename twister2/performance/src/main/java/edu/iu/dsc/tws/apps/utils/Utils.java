package edu.iu.dsc.tws.apps.utils;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

import java.util.*;
import java.util.logging.Logger;

public final class Utils {
  private static final Logger LOG = Logger.getLogger(Utils.class.getName());

  private Utils() {
  }

  public static TaskPlan createReduceTaskPlan(Config cfg, ResourcePlan plan, int noOfTasks) {
    int noOfContainers = plan.noOfContainers();
    Map<Integer, Set<Integer>> executorToGraphNodes = new HashMap<>();
    Map<Integer, Set<Integer>> groupsToExeuctors = new HashMap<>();
    int thisExecutor = plan.getThisId();

    List<ResourceContainer> containers = plan.getContainers();
    Map<String, List<ResourceContainer>> containersPerNode = new HashMap<>();
    for (ResourceContainer c : containers) {
      String processName = (String) c.getProperty("PROCESS_NAME");
      List<ResourceContainer> containerList;
      if (!containersPerNode.containsKey(processName)) {
        containerList = new ArrayList<>();
        containersPerNode.put(processName, containerList);
      } else {
        containerList = containersPerNode.get(processName);
      }
      containerList.add(c);
    }

    int taskPerExecutor = noOfTasks / noOfContainers;
    for (int i = 0; i < noOfContainers; i++) {
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
}