/*
 * Copyright 2014 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.ha.common;

import io.hops.exception.StorageException;
import io.hops.ha.common.FiCaSchedulerAppInfo;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.AppSchedulingInfoDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.fair.LocalityLevelDataAccess;
import io.hops.metadata.yarn.dal.fair.PreemptionMapDataAccess;
import io.hops.metadata.yarn.dal.fair.RunnableAppsDataAccess;
import io.hops.metadata.yarn.entity.AppSchedulingInfo;
import io.hops.metadata.yarn.entity.Resource;
import io.hops.metadata.yarn.entity.fair.LocalityLevel;
import io.hops.metadata.yarn.entity.fair.PreemptionMap;
import io.hops.metadata.yarn.entity.fair.RunnableApps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerApp;


/**
 *
 * @author Nikos Stanogias <niksta@sics.se>
 */
public class FairSchedulerAppInfo extends FiCaSchedulerAppInfo {

  private static final Log LOG = LogFactory.getLog(FairSchedulerAppInfo.class);

  private FSSchedulerApp fairSchedulerApp;

  private boolean isRunnable;

  private Map<RMContainer, Long> preemtiveContToAdd;
  private List<RMContainer> preemtiveContToRemove;

  private Map<Priority, NodeType> allowedLocalityLevelToAdd;

  protected String schedulerAppAttemptIdToRemove;
  protected String queuenameToRemove;
  
  public FairSchedulerAppInfo(ApplicationAttemptId appAttemptId) {
    super(appAttemptId);
  }

  public FairSchedulerAppInfo(SchedulerApplicationAttempt schedulerApp) {
    super(schedulerApp.getApplicationAttemptId());
    this.fairSchedulerApp = (FSSchedulerApp) schedulerApp;
  }

  public void updateAppInfo(SchedulerApplicationAttempt schedulerApp) {
    this.fairSchedulerApp = (FSSchedulerApp) schedulerApp;
    //update = true;
  }

  public void persist() throws StorageException {
    persistAppAttemptId();
//    persistToUpdateResources();
//    persistLiveContainersToAdd();
//    persistLiveContainersToRemove();
//    persistNewlyAllocatedContainersToAdd();
//    persistNewlyAllocatedContainersToRemove();
//    persistRequestsToAdd();
//    persistRequestsToRemove();
//    persistBlackListsToAdd();
//    persistBlackListsToRemove();

    persistPreemtiveContToAdd();
    //persistPreemtiveContToRemove();

//    persistReservedContainersToAdd();
//    persistReservedContainersToRemove();
//    persistAllowedLocalityLevelToAdd();
//    persistLastScheduledContainersToAdd();
//    persistSchedulingOpportunitiesToAdd();
//    persistReReservations();
    persistRunnableAppsToRemove();
  }

  public void setFairSchedulerApp(FSSchedulerApp fairSchedulerApp) {
    this.fairSchedulerApp = fairSchedulerApp;
  }

  public void setIsRunnable(boolean value) {
    this.isRunnable = value;
    System.out.println("isRunnable = " + isRunnable);
  }

  public void removeRunnableApp(String queuename, String appAttemptId) {
    schedulerAppAttemptIdToRemove = appAttemptId;
    queuenameToRemove = queuename;
  }

  public void addLocalityLevel(Priority p, NodeType n) {
    if (allowedLocalityLevelToAdd == null) {
      allowedLocalityLevelToAdd = new HashMap<Priority, NodeType>();
    }
    allowedLocalityLevelToAdd.put(p, n);
  }

  public void addPreemptiveContainer(RMContainer cont, Long l) {
    if (preemtiveContToAdd == null) {
      preemtiveContToAdd = new HashMap<RMContainer, Long>();
    }
    preemtiveContToAdd.put(cont, l);
  }

  public void removePreemptiveContainer(RMContainer cont) {
    if (preemtiveContToRemove == null) {
      preemtiveContToRemove = new ArrayList<RMContainer>();
    }
    preemtiveContToRemove.add(cont);
  }

  private void persistAppAttemptId() throws StorageException {
    if (fairSchedulerApp != null) {

      //Persist Resources
      ResourceDataAccess resourceDA = (ResourceDataAccess) RMStorageFactory.getDataAccess(ResourceDataAccess.class);
      List<Resource> toAddResources = new ArrayList<Resource>();
      if (fairSchedulerApp.getCurrentConsumption() != null) {
        toAddResources.add(new Resource(applicationAttemptId.toString(), Resource.CURRENTCONSUMPTION, Resource.SCHEDULERAPPLICATIONATTEMPT,
                fairSchedulerApp.getCurrentConsumption().getMemory(), fairSchedulerApp.getCurrentConsumption().getVirtualCores()));
      }
      if (fairSchedulerApp.getCurrentReservation() != null) {
        toAddResources.add(new Resource(applicationAttemptId.toString(), Resource.CURRENTRESERVATION, Resource.SCHEDULERAPPLICATIONATTEMPT,
                fairSchedulerApp.getCurrentReservation().getMemory(), fairSchedulerApp.getCurrentReservation().getVirtualCores()));
      }
      if (fairSchedulerApp.getResourceLimit() != null) {
        toAddResources.add(new Resource(applicationAttemptId.toString(), Resource.RESOURCELIMIT, Resource.SCHEDULERAPPLICATIONATTEMPT,
                fairSchedulerApp.getResourceLimit().getMemory(), fairSchedulerApp.getResourceLimit().getVirtualCores()));
      }
      resourceDA.addAll(toAddResources);

      //Persist AppSchedulingInfo
      AppSchedulingInfoDataAccess asinfoDA = (AppSchedulingInfoDataAccess) RMStorageFactory.getDataAccess(AppSchedulingInfoDataAccess.class);
      AppSchedulingInfo hopAppSchedulingInfo = new AppSchedulingInfo(applicationAttemptId.toString(),
              fairSchedulerApp.getApplicationId().toString(),
              fairSchedulerApp.getQueueName(),
              fairSchedulerApp.getUser(), 0,
              fairSchedulerApp.isPending(),
              fairSchedulerApp.isStopped());
      asinfoDA.add(hopAppSchedulingInfo);

      //Persist Runnable and NonRunnable Apps
      RunnableAppsDataAccess runAppsDA = (RunnableAppsDataAccess) RMStorageFactory.getDataAccess(RunnableAppsDataAccess.class);
      RunnableApps hopRunnableApp = new RunnableApps(fairSchedulerApp.getQueueName(),
              applicationAttemptId.toString(), isRunnable);
      runAppsDA.add(hopRunnableApp);
    }
  }

  private void persistPreemtiveContToAdd() throws StorageException {
    if (preemtiveContToAdd != null) {
      PreemptionMapDataAccess pmDA = (PreemptionMapDataAccess) RMStorageFactory.getDataAccess(PreemptionMapDataAccess.class);
      List<PreemptionMap> toAddPreemtpionMap = new ArrayList<PreemptionMap>();

      RMContainerDataAccess rmCDA = (RMContainerDataAccess) RMStorageFactory.getDataAccess(RMContainerDataAccess.class);
      List<io.hops.metadata.yarn.entity.RMContainer> toAddRMContainers = new ArrayList<io.hops.metadata.yarn.entity.RMContainer>();
      for (RMContainer rmContainer : preemtiveContToAdd.keySet()) {
        //Persist PreemptionMap
        toAddPreemtpionMap.add(new PreemptionMap(applicationAttemptId.toString(), rmContainer.getContainerId().toString(), preemtiveContToAdd.get(rmContainer)));

        boolean isReserved = (rmContainer.getReservedNode() != null)
                && (rmContainer.getReservedPriority() != null);

        String reservedNode = isReserved ? rmContainer.getReservedNode().toString() : null;
        int reservedPriority = isReserved ? rmContainer.getReservedPriority().getPriority() : Integer.MIN_VALUE;
        int reservedMemory = isReserved ? rmContainer.getReservedResource().getMemory() : 0;
        int reservedVCores = isReserved ? rmContainer.getReservedResource().getVirtualCores() : 0;
        String reservedHost = isReserved ? rmContainer.getReservedNode().getHost() : null;
        int reservedPort = isReserved ? rmContainer.getReservedNode().getPort() : 0;

        //Persist RMContainer
        toAddRMContainers.add(new io.hops.metadata.yarn.entity.RMContainer(rmContainer.getContainerId().toString(),
                rmContainer.getApplicationAttemptId().toString(),
                rmContainer.getNodeId().toString(),
                rmContainer.getUser(),
                reservedNode,
                reservedPriority,
                reservedMemory,
                reservedVCores,
                rmContainer.getStartTime(),
                rmContainer.getFinishTime(),
                rmContainer.getState().toString(),
                reservedHost,
                reservedPort,
                ((RMContainerImpl) rmContainer).getContainerState().toString(),
                ((RMContainerImpl) rmContainer).getContainerExitStatus()
        ));
      }

      pmDA.addAll(toAddPreemtpionMap);
      rmCDA.addAll(toAddRMContainers);
    }
  }

  private void persistPreemtiveContToRemove() throws StorageException {
    if (preemtiveContToRemove != null && !preemtiveContToRemove.isEmpty()) {
      PreemptionMapDataAccess pmDA = (PreemptionMapDataAccess) RMStorageFactory.getDataAccess(PreemptionMapDataAccess.class);
      List<PreemptionMap> toRemovePreemtpionMap = new ArrayList<PreemptionMap>();

      RMContainerDataAccess rmCDA = (RMContainerDataAccess) RMStorageFactory.getDataAccess(RMContainerDataAccess.class);
      List<io.hops.metadata.yarn.entity.RMContainer> toRemoveRMContainers = new ArrayList<io.hops.metadata.yarn.entity.RMContainer>();
      for (RMContainer rmContainer : preemtiveContToRemove) {
        //Remove PreemptionMap
        toRemovePreemtpionMap.add(new PreemptionMap(applicationAttemptId.toString(), rmContainer.getContainerId().toString(), 0));

        boolean isReserved = (rmContainer.getReservedNode() != null)
                && (rmContainer.getReservedPriority() != null);

        String reservedNode = isReserved ? rmContainer.getReservedNode().toString() : null;
        int reservedPriority = isReserved ? rmContainer.getReservedPriority().getPriority() : Integer.MIN_VALUE;
        int reservedMemory = isReserved ? rmContainer.getReservedResource().getMemory() : 0;
        int reservedVCores = isReserved ? rmContainer.getReservedResource().getVirtualCores() : 0;
        String reservedHost = isReserved ? rmContainer.getReservedNode().getHost() : null;
        int reservedPort = isReserved ? rmContainer.getReservedNode().getPort() : 0;

        //Remove RMContainer
        toRemoveRMContainers.add(new io.hops.metadata.yarn.entity.RMContainer(rmContainer.getContainerId().toString(),
                rmContainer.getApplicationAttemptId().toString(),
                rmContainer.getNodeId().toString(),
                rmContainer.getUser(),
                reservedNode,
                reservedPriority,
                reservedMemory,
                reservedVCores,
                rmContainer.getStartTime(),
                rmContainer.getFinishTime(),
                rmContainer.getState().toString(),
                reservedHost,
                reservedPort,
                ((RMContainerImpl) rmContainer).getContainerState().toString(),
                ((RMContainerImpl) rmContainer).getContainerExitStatus()
        ));
      }
      pmDA.removeAll(toRemovePreemtpionMap);
      rmCDA.removeAll(toRemoveRMContainers);
    }
  }

  protected void persistAllowedLocalityLevelToAdd() throws StorageException {
    if (allowedLocalityLevelToAdd != null) {
      LocalityLevelDataAccess llDA = (LocalityLevelDataAccess) RMStorageFactory.getDataAccess(LocalityLevelDataAccess.class);
      List<LocalityLevel> toAddLocalityLevel = new ArrayList<LocalityLevel>();

      for (Priority p : allowedLocalityLevelToAdd.keySet()) {
        //Persist LocalityLevel
        toAddLocalityLevel.add(new LocalityLevel(applicationAttemptId.toString(), p.getPriority(), allowedLocalityLevelToAdd.get(p).name()));
      }
      llDA.addAll(toAddLocalityLevel);
    }
  }

  private void persistRunnableAppsToRemove() throws StorageException {
    if(schedulerAppAttemptIdToRemove != null) {
      RunnableAppsDataAccess DA = (RunnableAppsDataAccess) RMStorageFactory.getDataAccess(RunnableAppsDataAccess.class);
      List<RunnableApps> toRemoveRunnableApps = new ArrayList<RunnableApps>();
      
      toRemoveRunnableApps.add(new RunnableApps(queuenameToRemove, schedulerAppAttemptIdToRemove, isRunnable));
      DA.removeAll(toRemoveRunnableApps);
    }
  }
}
