package edu.iu.dsc.tws.apps.kmeans.utils;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.WorkerResourceUtils;
import edu.iu.dsc.tws.comms.core.TaskPlan;
//import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
//import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
//import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlanUtils;

import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import org.apache.commons.cli.Option;

import java.util.*;

public final class Utils {
  private Utils() {
  }

  /*public static TaskPlan createReduceTaskPlan(Config cfg, ResourcePlan plan, List<Integer> noOfTaskEachStage) {
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
  }*/

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

  public static Option createOption(String opt, boolean hasArg, String description, boolean required) {
    Option symbolListOption = new Option(opt, hasArg, description);
    symbolListOption.setRequired(required);
    return symbolListOption;
  }

  public static TaskPlan createStageTaskPlan(Config cfg, int workerID,
                                             List<Integer> noOfTaskEachStage,
                                             List<JobMasterAPI.WorkerInfo> workerList) {
    int noOfContainers = workerList.size();
    Map<Integer, Set<Integer>> executorToGraphNodes = new HashMap<>();
    Map<Integer, Set<Integer>> groupsToExeuctors = new HashMap<>();
    int thisExecutor = workerID;

    Map<String, List<JobMasterAPI.WorkerInfo>> containersPerNode =
            WorkerResourceUtils.getWorkersPerNode(workerList);

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
    for (Map.Entry<String, List<JobMasterAPI.WorkerInfo>> entry : containersPerNode.entrySet()) {
      Set<Integer> executorsOfGroup = new HashSet<>();
      for (JobMasterAPI.WorkerInfo workerInfo : entry.getValue()) {
        executorsOfGroup.add(workerInfo.getWorkerID());
      }
      groupsToExeuctors.put(i, executorsOfGroup);
      i++;
    }
//    groupsToExeuctors.put(0, new HashSet<>(Arrays.asList(1)));
//    groupsToExeuctors.put(1, new HashSet<>(Arrays.asList(2)));
//    groupsToExeuctors.put(2, new HashSet<>(Arrays.asList(0)));
//    groupsToExeuctors.put(3, new HashSet<>(Arrays.asList(3)));

    return new TaskPlan(executorToGraphNodes, groupsToExeuctors, thisExecutor);
  }
}
