package edu.iu.dsc.tws.apps.utils;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlanUtils;

import java.util.*;
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

  private static int nextExecutorId(int current, int noOfContainers) {
    if (current < noOfContainers - 1) {
      return ++current;
    } else {
      return 0;
    }
  }
}