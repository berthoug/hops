/*
 * Copyright (C) 2015 hops.io.
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
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.NextHeartbeatDataAccess;
import io.hops.metadata.yarn.dal.NodeHBResponseDataAccess;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.entity.ContainerId;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.FinishedApplications;
import io.hops.metadata.yarn.entity.JustLaunchedContainers;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import io.hops.metadata.yarn.entity.NodeHBResponse;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

public class RMNodeInfo {

  private static final Log LOG = LogFactory.getLog(RMNodeInfo.class);
  private String rmnodeId = null;
  private Set<org.apache.hadoop.yarn.api.records.ContainerId>
      containerToCleanToAdd;
  private Set<String> containerToCleanToRemove;
  private Map<org.apache.hadoop.yarn.api.records.ContainerId, org.apache.hadoop.yarn.api.records.ContainerStatus>
      justLaunchedContainersToAdd;
  private Map<String, String> justLaunchedContainersToRemove;
  private ConcurrentLinkedQueue<org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo>
      nodeUpdateQueueToAdd;
  private ConcurrentSkipListSet<org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo>
      nodeUpdateQueueToRemove = new ConcurrentSkipListSet<org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo>();
  private List<ApplicationId> finishedApplicationsToAdd;
  private List<String> finishedApplicationsToRemove;
  private NodeHeartbeatResponse latestNodeHeartBeatResponse;
  private NextHeartbeat nextHeartbeat;

  public RMNodeInfo(String rmnodeId) {
    this.rmnodeId = rmnodeId;
  }

  public void persist(NodeHBResponseDataAccess hbDA,
      ContainerIdToCleanDataAccess cidToCleanDA,
      JustLaunchedContainersDataAccess justLaunchedContainersDA,
      UpdatedContainerInfoDataAccess updatedContainerInfoDA,
      FinishedApplicationsDataAccess faDA, ContainerStatusDataAccess csDA, int tsid)
      throws StorageException {
    persistJustLaunchedContainersToAdd(justLaunchedContainersDA, csDA, tsid);
    persistJustLaunchedContainersToRemove(justLaunchedContainersDA,tsid);
    persistContainerToCleanToAdd(cidToCleanDA, tsid);
    persistContainerToCleanToRemove(cidToCleanDA, tsid);
    persistFinishedApplicationToAdd(faDA, tsid);
    persistFinishedApplicationToRemove(faDA, tsid);
    persistNodeUpdateQueueToAdd(updatedContainerInfoDA, csDA, tsid);
    persistNodeUpdateQueueToRemove(updatedContainerInfoDA, csDA, tsid);
    persistLatestHeartBeatResponseToAdd(hbDA);
    persistNextHeartbeat();
  }

  public String getRmnodeId() {
    return rmnodeId;
  }

  public void toAddJustLaunchedContainers(
      org.apache.hadoop.yarn.api.records.ContainerId key,
      org.apache.hadoop.yarn.api.records.ContainerStatus val) {
    if (this.justLaunchedContainersToAdd == null) {
      this.justLaunchedContainersToAdd =
          new HashMap<org.apache.hadoop.yarn.api.records.ContainerId, org.apache.hadoop.yarn.api.records.ContainerStatus>(
              1);
    }
    this.justLaunchedContainersToAdd.put(key, val);

  }

  public void toRemoveJustLaunchedContainers(String key) {
    if (this.justLaunchedContainersToRemove == null) {
      this.justLaunchedContainersToRemove = new HashMap<String, String>(1);
    }
    this.justLaunchedContainersToRemove.put(key, null);
  }

  public void toAddContainerToClean(
      org.apache.hadoop.yarn.api.records.ContainerId toAdd) {
    if (this.containerToCleanToAdd == null) {
      this.containerToCleanToAdd =
          new TreeSet<org.apache.hadoop.yarn.api.records.ContainerId>();
    }
    this.containerToCleanToAdd.add(toAdd);
  }


  public void toRemoveContainerToClean(String toRemove) {
    if (this.containerToCleanToRemove == null) {
      this.containerToCleanToRemove = new TreeSet<String>();
    }
    this.containerToCleanToRemove.remove(toRemove);
  }

  public void toAddNodeUpdateQueue(
      org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo uci, int tsid) {
    if (this.nodeUpdateQueueToAdd == null) {
      this.nodeUpdateQueueToAdd =
          new ConcurrentLinkedQueue<org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo>();
    }
    for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus : uci
              .getNewlyLaunchedContainers()) {
      LOG.info(tsid + " adding uci to be added " + containerStatus.getContainerId().
                    toString() + " " + uci.getUpdatedContainerInfoId());
    }
    for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus : uci
              .getCompletedContainers()) {
      LOG.info(tsid + " adding uci to be added " + containerStatus.getContainerId().
                    toString() + " " + uci.getUpdatedContainerInfoId());
    }
    this.nodeUpdateQueueToAdd.add(uci);
  }

  public void toRemoveNodeUpdateQueue(
      org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo uci, int tsid) {
    for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus : uci
              .getNewlyLaunchedContainers()) {
    LOG.info(tsid + " adding uci to be removed " + containerStatus.getContainerId().
                    toString()+ " " + uci.getUpdatedContainerInfoId()
            );
    }
    for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus : uci
              .getCompletedContainers()) {
    LOG.info(tsid + " adding uci to be removed " + containerStatus.getContainerId().
                    toString()+ " " + uci.getUpdatedContainerInfoId()
            );
    }
    this.nodeUpdateQueueToRemove.add(uci);
  }

  public void toRemoveNodeUpdateQueue(
      ConcurrentLinkedQueue<org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo> uci, int tsid) {
    for(org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo c: uci){
      for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus : c
              .getNewlyLaunchedContainers()) {
              LOG.info(tsid + " adding uci to be removed " + containerStatus.getContainerId().
                    toString()+ " " + c.getUpdatedContainerInfoId()
            );
    }
         for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus : c
              .getCompletedContainers()) {
              LOG.info(tsid + " adding uci to be removed " + containerStatus.getContainerId().
                    toString()+ " " + c.getUpdatedContainerInfoId()
            );
    }
    }
    this.nodeUpdateQueueToRemove.addAll(uci);
  }

  public void toAddFinishedApplications(ApplicationId app) {
    if (this.finishedApplicationsToAdd == null) {
      this.finishedApplicationsToAdd = new ArrayList<ApplicationId>();
    }
    this.finishedApplicationsToAdd.add(app);
  }


  public void toRemoveFinishedApplications(String app) {
    if (this.finishedApplicationsToRemove == null) {
      this.finishedApplicationsToRemove = new ArrayList<String>();
    }
    this.finishedApplicationsToRemove.add(app);
  }

  public void persistContainerToCleanToAdd(
      ContainerIdToCleanDataAccess cidToCleanDA, int tsid) throws StorageException {
    if (containerToCleanToAdd != null) {
      ArrayList<ContainerId> toAddHopContainerIdToClean =
          new ArrayList<ContainerId>(containerToCleanToAdd.size());
      for (org.apache.hadoop.yarn.api.records.ContainerId cid : containerToCleanToAdd) {
        if(!containerToCleanToRemove.remove(cid)){
        LOG.info(tsid +" adding container to clean for node " + rmnodeId);
        toAddHopContainerIdToClean
            .add(new ContainerId(rmnodeId, cid.toString()));}
      }
      cidToCleanDA.addAll(toAddHopContainerIdToClean);
    }
  }

  public void persistContainerToCleanToRemove(
      ContainerIdToCleanDataAccess cidToCleanDA, int tsid) throws StorageException {
    if (containerToCleanToRemove != null) {
      ArrayList<ContainerId> toRemoveHopContainerIdToClean =
          new ArrayList<ContainerId>(containerToCleanToRemove.size());
      for (String cid : containerToCleanToRemove) {
        LOG.info(tsid + " remove container to clean node " + rmnodeId + " " + cid);
        toRemoveHopContainerIdToClean.add(new ContainerId(rmnodeId, cid));
      }
      cidToCleanDA.removeAll(toRemoveHopContainerIdToClean);
    }
  }

  public void persistJustLaunchedContainersToAdd(
      JustLaunchedContainersDataAccess justLaunchedContainersDA,
      ContainerStatusDataAccess csDA, int tsid) throws StorageException {
    if (justLaunchedContainersToAdd != null &&
        !justLaunchedContainersToAdd.isEmpty()) {
      List<JustLaunchedContainers> toAddHopJustLaunchedContainers =
          new ArrayList<JustLaunchedContainers>();
      List<ContainerStatus> toAddContainerStatus =
          new ArrayList<ContainerStatus>();
      for (org.apache.hadoop.yarn.api.records.ContainerStatus value : justLaunchedContainersToAdd
          .values()) {
        if (justLaunchedContainersToRemove == null ||
            justLaunchedContainersToRemove.remove(value.
                getContainerId().toString()) == null) {
          LOG.info(tsid + " adding just launched container " + rmnodeId + " " + value.
              getContainerId().toString());
          toAddHopJustLaunchedContainers.add(
              new JustLaunchedContainers(rmnodeId,
                  value.getContainerId().toString()));
          toAddContainerStatus.add(
              new ContainerStatus(value.getContainerId().toString(),
                  value.getState().toString(), value.getDiagnostics(),
                  value.getExitStatus(), rmnodeId));
        }
      }
      csDA.addAll(toAddContainerStatus);
      justLaunchedContainersDA.addAll(toAddHopJustLaunchedContainers);
      //Persist ContainerId and ContainerStatus

    }
  }


  public void persistJustLaunchedContainersToRemove(
      JustLaunchedContainersDataAccess justLaunchedContainersDA, int tsid)
      throws StorageException {
    if (justLaunchedContainersToRemove != null &&
        !justLaunchedContainersToRemove.isEmpty()) {
      List<JustLaunchedContainers> toRemoveHopJustLaunchedContainers =
          new ArrayList<JustLaunchedContainers>();
      for (String key : justLaunchedContainersToRemove.keySet()) {
        LOG.info(tsid + " remove just launched container " + rmnodeId + " " + key);
        toRemoveHopJustLaunchedContainers
            .add(new JustLaunchedContainers(rmnodeId, key));
      }
      justLaunchedContainersDA.removeAll(toRemoveHopJustLaunchedContainers);
    }
  }


  public void persistNodeUpdateQueueToAdd(
      UpdatedContainerInfoDataAccess updatedContainerInfoDA,
      ContainerStatusDataAccess csDA, int tsid) throws StorageException {
    if (nodeUpdateQueueToAdd != null && !nodeUpdateQueueToAdd.isEmpty()) {
      //Add row at ha_updatedcontainerinfo
      ArrayList<UpdatedContainerInfo> uciToAdd = new ArrayList<UpdatedContainerInfo>();
      ArrayList<ContainerStatus> containerStatusToAdd =
          new ArrayList<ContainerStatus>();
      for (org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo uci : nodeUpdateQueueToAdd) {
        if(!nodeUpdateQueueToRemove.remove(uci)){

        //Add rows at NEW & COMPLETE updatedcontainerinfo_containers
        if (uci.getNewlyLaunchedContainers() != null &&
            !uci.getNewlyLaunchedContainers().isEmpty()) {
          for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus : uci
              .getNewlyLaunchedContainers()) {
            UpdatedContainerInfo hopUCI = new UpdatedContainerInfo(rmnodeId,
                containerStatus.getContainerId().
                    toString(), uci.
                getUpdatedContainerInfoId());
            LOG.info(tsid +" adding uci " + hopUCI.getRmnodeid() + " " +
                hopUCI.getContainerId());
            uciToAdd.add(hopUCI);


            //Persist ContainerStatus, ContainerId, ApplicationAttemptId, ApplicationId
            ContainerStatus hopConStatus =
                new ContainerStatus(containerStatus.getContainerId().toString(),
                    containerStatus.getState().toString(),
                    containerStatus.getDiagnostics(),
                    containerStatus.getExitStatus(), rmnodeId);
            //TODO batch
            containerStatusToAdd.add(hopConStatus);
          }
        }
        if (uci.getCompletedContainers() != null &&
            !uci.getCompletedContainers().isEmpty()) {
          for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus : uci
              .getCompletedContainers()) {
            UpdatedContainerInfo hopUCI = new UpdatedContainerInfo(rmnodeId,
                containerStatus.getContainerId().
                    toString(), uci.
                getUpdatedContainerInfoId());
            LOG.info(tsid + " adding uci " + hopUCI.getRmnodeid() + " " +
                hopUCI.getContainerId());
            uciToAdd.add(hopUCI);

            //Persist ContainerStatus, ContainerId, ApplicationAttemptId, ApplicationId
            ContainerStatus hopConStatus =
                new ContainerStatus(containerStatus.getContainerId().toString(),
                    containerStatus.getState().toString(),
                    containerStatus.getDiagnostics(),
                    containerStatus.getExitStatus(), rmnodeId);
            //TODO batch
            containerStatusToAdd.add(hopConStatus);
          }
        }
        }
      }
      csDA.addAll(containerStatusToAdd);
      updatedContainerInfoDA.addAll(uciToAdd);
    }
  }


  public void persistNodeUpdateQueueToRemove(
      UpdatedContainerInfoDataAccess updatedContainerInfoDA,
      ContainerStatusDataAccess csDA, int tsid) throws StorageException {
    if (nodeUpdateQueueToRemove != null && !nodeUpdateQueueToRemove.isEmpty()) {
      Set<UpdatedContainerInfo> uciToRemove = null;
      for (org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo uci : nodeUpdateQueueToRemove) {

        if (uciToRemove == null) {
          uciToRemove = new HashSet<UpdatedContainerInfo>();
        }

        //Remove rows for NEW & COMPLETE updatedcontainerinfo_containers
        if (uci.getNewlyLaunchedContainers() != null &&
            !uci.getNewlyLaunchedContainers().isEmpty()) {

          for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus : uci
              .getNewlyLaunchedContainers()) {
            UpdatedContainerInfo hopUCI = new UpdatedContainerInfo(rmnodeId,
                containerStatus.getContainerId().
                    toString(), uci.
                getUpdatedContainerInfoId());
            LOG.info(tsid + " remove uci " + hopUCI.getRmnodeid() + " " +
                hopUCI.getContainerId());
            uciToRemove.add(hopUCI);
          }
        }
        if (uci.getCompletedContainers() != null &&
            !uci.getCompletedContainers().isEmpty()) {

          for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus : uci
              .getCompletedContainers()) {
            UpdatedContainerInfo hopUCI = new UpdatedContainerInfo(rmnodeId,
                containerStatus.getContainerId().
                    toString(), uci.
                getUpdatedContainerInfoId());
            LOG.info(tsid + " remove uci " + hopUCI.getRmnodeid() + " " +
                hopUCI.getContainerId());
            uciToRemove.add(hopUCI);
          }
        }
      }
      updatedContainerInfoDA.removeAll(uciToRemove);
    }
  }

  public List<ApplicationId> getFinishedApplicationsToAdd() {
    return this.finishedApplicationsToAdd;
  }

  public void persistFinishedApplicationToAdd(
      FinishedApplicationsDataAccess faDA, int tsid) throws StorageException {
    if (finishedApplicationsToAdd != null && !finishedApplicationsToAdd.
        isEmpty()) {
      ArrayList<FinishedApplications> toAddHopFinishedApplications =
          new ArrayList<FinishedApplications>();
      for (ApplicationId appId : finishedApplicationsToAdd) {
        if (finishedApplicationsToRemove == null ||
            !finishedApplicationsToRemove.remove(appId.toString())) {
          LOG.info(tsid +
              " add finished app " + appId.toString() + " on node " + rmnodeId);
          FinishedApplications hopFinishedApplications =
              new FinishedApplications(rmnodeId, appId.toString());
          LOG.info(tsid + " adding ha_rmnode_finishedapplications " +
              hopFinishedApplications.getRMNodeID());
          toAddHopFinishedApplications.add(hopFinishedApplications);
        }
      }
      faDA.addAll(toAddHopFinishedApplications);
    }
  }

  public List<String> getFinishedApplicationsToRemove() {
    return this.finishedApplicationsToRemove;
  }

  public void persistFinishedApplicationToRemove(
      FinishedApplicationsDataAccess faDA, int tsid) throws StorageException {
    if (finishedApplicationsToRemove != null &&
        !finishedApplicationsToRemove.isEmpty()) {
      ArrayList<FinishedApplications> toRemoveHopFinishedApplications =
          new ArrayList<FinishedApplications>();
      for (String appId : finishedApplicationsToRemove) {
        FinishedApplications hopFinishedApplications =
            new FinishedApplications(rmnodeId, appId);
        LOG.info(tsid + " remove finished app " +
            hopFinishedApplications.getApplicationId() + " on node " +
            hopFinishedApplications.getRMNodeID());
        toRemoveHopFinishedApplications.add(hopFinishedApplications);
      }
      faDA.removeAll(toRemoveHopFinishedApplications);
    }
  }

  public void toAddLatestNodeHeartBeatResponse(NodeHeartbeatResponse resp) {
    this.latestNodeHeartBeatResponse = resp;
  }

  public void persistLatestHeartBeatResponseToAdd(NodeHBResponseDataAccess hbDA)
      throws StorageException {
    if (latestNodeHeartBeatResponse != null) {
      NodeHBResponse toAdd;
      //Check if it is not a mock, otherwise a ClassCastException would be thrown
      if (latestNodeHeartBeatResponse instanceof NodeHeartbeatResponsePBImpl) {
        toAdd = new NodeHBResponse(rmnodeId,
            ((NodeHeartbeatResponsePBImpl) latestNodeHeartBeatResponse)
                .getProto().toByteArray());
      } else {
        toAdd = new NodeHBResponse(rmnodeId, null);
      }
      hbDA.add(toAdd);
    }
  }

  public void toAddNextHeartbeat(String rmnodeid, boolean nextHeartbeat) {
    LOG.debug(
        "HOP :: toAddNextHeartbeat-START:" + rmnodeid + "," + nextHeartbeat);
    this.nextHeartbeat = new NextHeartbeat(rmnodeid, nextHeartbeat);
    LOG.debug(
        "HOP :: toAddNextHeartbeat-FINISH:" + rmnodeid + "," + nextHeartbeat);
  }

  public void persistNextHeartbeat() throws StorageException {
    NextHeartbeatDataAccess nextHeartbeatDA =
        (NextHeartbeatDataAccess) RMStorageFactory
            .getDataAccess(NextHeartbeatDataAccess.class);
    LOG.debug("HOP :: persistNextHeartbeat-START:" + nextHeartbeat);
    if (nextHeartbeat != null) {
      nextHeartbeatDA.updateNextHeartbeat(nextHeartbeat.getRmnodeid(),
          nextHeartbeat.isNextheartbeat());
    }
    LOG.debug("HOP :: persistNextHeartbeat-FINISH:" + nextHeartbeat);
  }
}
