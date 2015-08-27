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
import io.hops.metadata.yarn.entity.UpdatedContainerInfoToAdd;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
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
import java.util.concurrent.locks.Lock;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;

public class RMNodeInfo {

  private static final Log LOG = LogFactory.getLog(RMNodeInfo.class);
  private String rmnodeId = null;
  private Set<org.apache.hadoop.yarn.api.records.ContainerId>
      containerToCleanToAdd =
          new TreeSet<org.apache.hadoop.yarn.api.records.ContainerId>();
  private Set< org.apache.hadoop.yarn.api.records.ContainerId> containerToCleanToRemove= new TreeSet< org.apache.hadoop.yarn.api.records.ContainerId>();
  private Map<org.apache.hadoop.yarn.api.records.ContainerId, ContainerStatus>
      justLaunchedContainersToAdd =
          new HashMap<org.apache.hadoop.yarn.api.records.ContainerId, ContainerStatus>();
  private Set<org.apache.hadoop.yarn.api.records.ContainerId> justLaunchedContainersToRemove = new TreeSet<org.apache.hadoop.yarn.api.records.ContainerId>();
  private Set<UpdatedContainerInfoToAdd>
      nodeUpdateQueueToAdd =
          new ConcurrentSkipListSet<UpdatedContainerInfoToAdd>();;
  private Set<UpdatedContainerInfoToAdd>
      nodeUpdateQueueToRemove = new ConcurrentSkipListSet<UpdatedContainerInfoToAdd>();
  private Set<ApplicationId> finishedApplicationsToAdd = new ConcurrentSkipListSet<ApplicationId>();;
  private Set<ApplicationId> finishedApplicationsToRemove = new ConcurrentSkipListSet<ApplicationId>();;
  private NodeHBResponse latestNodeHeartBeatResponse;
  private NextHeartbeat nextHeartbeat;

  public RMNodeInfo(String rmnodeId) {
    this.rmnodeId = rmnodeId;
  }

  public void agregate(RMNodeInfoAgregate agregate){
    agregateJustLaunchedContainersToAdd(agregate);
    agregateJustLaunchedContainersToRemove(agregate);
    agregateContainerToCleanToAdd(agregate);
    agregateContainerToCleanToRemove(agregate);
    agregateFinishedApplicationToAdd(agregate);
    agregateFinishedApplicationToRemove(agregate);
    agregateNodeUpdateQueueToAdd(agregate);
    agregateNodeUpdateQueueToRemove(agregate);
    agregateLatestHeartBeatResponseToAdd(agregate);
    agregateNextHeartbeat(agregate);
  }
  
  public String getRmnodeId() {
    return rmnodeId;
  }

  public void toAddJustLaunchedContainers(
      org.apache.hadoop.yarn.api.records.ContainerId key,
      org.apache.hadoop.yarn.api.records.ContainerStatus val) {
    ContainerStatus toAdd = new ContainerStatus(val.getContainerId().toString(),
                  val.getState().toString(), val.getDiagnostics(),
                  val.getExitStatus(), rmnodeId);
    this.justLaunchedContainersToAdd.put(key, toAdd);
    justLaunchedContainersToRemove.remove(key);
  }

  public void toRemoveJustLaunchedContainers(org.apache.hadoop.yarn.api.records.ContainerId key) {
    if(justLaunchedContainersToAdd.remove(key)==null){
      this.justLaunchedContainersToRemove.add(key);
    }
  }

  public void toAddContainerToClean(
      org.apache.hadoop.yarn.api.records.ContainerId toAdd) {
    this.containerToCleanToAdd.add(toAdd);
    this.containerToCleanToRemove.remove(toAdd);
  }


  public void toRemoveContainerToClean(org.apache.hadoop.yarn.api.records.ContainerId toRemove) {
    if(!containerToCleanToAdd.remove(toRemove)){
      this.containerToCleanToRemove.add(toRemove);
    }
  }

  public void toAddNodeUpdateQueue(
      org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo uci) {
    
    if (uci.getNewlyLaunchedContainers() != null &&
            !uci.getNewlyLaunchedContainers().isEmpty()) {
          for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus : uci
              .getNewlyLaunchedContainers()) {
            UpdatedContainerInfo hopUCI = new UpdatedContainerInfo(rmnodeId,
                containerStatus.getContainerId().
                    toString(), uci.
                getUpdatedContainerInfoId());

            ContainerStatus hopConStatus =
                new ContainerStatus(containerStatus.getContainerId().toString(),
                    containerStatus.getState().toString(),
                    containerStatus.getDiagnostics(),
                    containerStatus.getExitStatus(), rmnodeId);
           
                UpdatedContainerInfoToAdd uciToAdd = new UpdatedContainerInfoToAdd(hopUCI, hopConStatus);
                this.nodeUpdateQueueToAdd.add(uciToAdd);
                this.nodeUpdateQueueToRemove.remove(uciToAdd);
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
           
            ContainerStatus hopConStatus =
                new ContainerStatus(containerStatus.getContainerId().toString(),
                    containerStatus.getState().toString(),
                    containerStatus.getDiagnostics(),
                    containerStatus.getExitStatus(), rmnodeId);
            
            UpdatedContainerInfoToAdd uciToAdd = new UpdatedContainerInfoToAdd(hopUCI, hopConStatus);
            this.nodeUpdateQueueToAdd.add(uciToAdd);
            this.nodeUpdateQueueToRemove.remove(uciToAdd );
          }
        }
        
  }

  public void toRemoveNodeUpdateQueue(
          org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo uci) {

    Set<org.apache.hadoop.yarn.api.records.ContainerId> alreadyRemoved = new HashSet<org.apache.hadoop.yarn.api.records.ContainerId>();
    if (uci.getNewlyLaunchedContainers() != null && !uci.
            getNewlyLaunchedContainers().isEmpty()) {
      for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus
              : uci
              .getNewlyLaunchedContainers()) {
        UpdatedContainerInfo hopUCI = new UpdatedContainerInfo(rmnodeId,
                containerStatus.getContainerId().
                toString(), uci.
                getUpdatedContainerInfoId());
        
        UpdatedContainerInfoToAdd uciToRemove = new UpdatedContainerInfoToAdd(
                hopUCI, null);
        boolean flag = !this.nodeUpdateQueueToAdd.remove(uciToRemove);
        if (flag & alreadyRemoved.add(containerStatus.getContainerId())) {
          this.nodeUpdateQueueToRemove.add(uciToRemove);
        }
      }
    }
    if (uci.getCompletedContainers() != null && !uci.getCompletedContainers().
            isEmpty()) {

      for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus
              : uci.getCompletedContainers()) {
        UpdatedContainerInfo hopUCI = new UpdatedContainerInfo(rmnodeId,
                containerStatus.getContainerId().
                toString(), uci.
                getUpdatedContainerInfoId());

        UpdatedContainerInfoToAdd uciToRemove = new UpdatedContainerInfoToAdd(
                hopUCI, null);
         boolean flag = !this.nodeUpdateQueueToAdd.remove(uciToRemove);
        if (flag & alreadyRemoved.add(containerStatus.getContainerId())) {
          this.nodeUpdateQueueToRemove.add(uciToRemove);
        }
      }
    }
  }

  public void toRemoveNodeUpdateQueue(
      ConcurrentLinkedQueue<org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo> uci) {
    
    for(org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo c: uci){
      toRemoveNodeUpdateQueue(c);
    }
  }

  public void toAddFinishedApplications(ApplicationId app) {
    this.finishedApplicationsToAdd.add(app);
    this.finishedApplicationsToRemove.remove(app);
  }


  public void toRemoveFinishedApplications(ApplicationId app) {
    if(!finishedApplicationsToAdd.remove(app)){
    this.finishedApplicationsToRemove.add(app);
    }
  }

  public void agregateContainerToCleanToAdd(RMNodeInfoAgregate agregate) {
    if (containerToCleanToAdd != null) {
      ArrayList<ContainerId> toAddHopContainerIdToClean =
          new ArrayList<ContainerId>(containerToCleanToAdd.size());
      for (org.apache.hadoop.yarn.api.records.ContainerId cid : containerToCleanToAdd) {
        if(!containerToCleanToRemove.remove(cid)){
        toAddHopContainerIdToClean
            .add(new ContainerId(rmnodeId, cid.toString()));}
      }
      agregate.addAllContainersToCleanToAdd(toAddHopContainerIdToClean);
    }
  }
  
 

   public void agregateContainerToCleanToRemove(RMNodeInfoAgregate agregate){
    if (containerToCleanToRemove != null) {
      ArrayList<ContainerId> toRemoveHopContainerIdToClean =
          new ArrayList<ContainerId>(containerToCleanToRemove.size());
      for (org.apache.hadoop.yarn.api.records.ContainerId cid : containerToCleanToRemove) {
        toRemoveHopContainerIdToClean.add(new ContainerId(rmnodeId, cid.toString()));
      }
      agregate.addAllContainerToCleanToRemove(toRemoveHopContainerIdToClean);
    }
  }
   


  public void agregateJustLaunchedContainersToAdd(RMNodeInfoAgregate agregate) {
    if (justLaunchedContainersToAdd != null &&
        !justLaunchedContainersToAdd.isEmpty()) {
      List<JustLaunchedContainers> toAddHopJustLaunchedContainers =
          new ArrayList<JustLaunchedContainers>();
      List<ContainerStatus> toAddContainerStatus =
          new ArrayList<ContainerStatus>();
      for (ContainerStatus value : justLaunchedContainersToAdd
          .values()) {

          toAddHopJustLaunchedContainers.add(
              new JustLaunchedContainers(rmnodeId,
                  value.getContainerid()));
          toAddContainerStatus.add(value);
      }
      agregate.addAllContainersStatusToAdd(toAddContainerStatus);
      agregate.addAllJustLaunchedContainersToAdd(toAddHopJustLaunchedContainers);
      //Persist ContainerId and ContainerStatus

    }
  }
  



    public void agregateJustLaunchedContainersToRemove(RMNodeInfoAgregate agregate){
    if (justLaunchedContainersToRemove != null &&
        !justLaunchedContainersToRemove.isEmpty()) {
      List<JustLaunchedContainers> toRemoveHopJustLaunchedContainers =
          new ArrayList<JustLaunchedContainers>();
      for (org.apache.hadoop.yarn.api.records.ContainerId key : justLaunchedContainersToRemove) {
        toRemoveHopJustLaunchedContainers
            .add(new JustLaunchedContainers(rmnodeId, key.toString()));
      }
      agregate.addAllJustLaunchedContainersToRemove(toRemoveHopJustLaunchedContainers);
    }
  }
    



  public void agregateNodeUpdateQueueToAdd(RMNodeInfoAgregate agregate){
    if (nodeUpdateQueueToAdd != null && !nodeUpdateQueueToAdd.isEmpty()) {
      //Add row at ha_updatedcontainerinfo
      ArrayList<UpdatedContainerInfo> uciToAdd = new ArrayList<UpdatedContainerInfo>();
      ArrayList<ContainerStatus> containerStatusToAdd =
          new ArrayList<ContainerStatus>();
      for (UpdatedContainerInfoToAdd uci : nodeUpdateQueueToAdd) {
        uciToAdd.add(uci.getUci());
        containerStatusToAdd.add(uci.getContainerStatus());
      }
      agregate.addAllContainersStatusToAdd(containerStatusToAdd);
      agregate.addAllUpdatedContainerInfoToAdd(uciToAdd);
    }
  }
  
 


   public void agregateNodeUpdateQueueToRemove(RMNodeInfoAgregate agregate) {
    if (nodeUpdateQueueToRemove != null && !nodeUpdateQueueToRemove.isEmpty()) {
      Set<UpdatedContainerInfo> uciToRemove = new HashSet<UpdatedContainerInfo>();
      for (UpdatedContainerInfoToAdd uci : nodeUpdateQueueToRemove) {
        uciToRemove.add(uci.getUci());
      }
      agregate.addAllUpdatedContainerInfoToRemove(uciToRemove);
    }
  }
   
 


   public void agregateFinishedApplicationToAdd(RMNodeInfoAgregate agregate){
    if (finishedApplicationsToAdd != null && !finishedApplicationsToAdd.
        isEmpty()) {
      ArrayList<FinishedApplications> toAddHopFinishedApplications =
          new ArrayList<FinishedApplications>();
      for (ApplicationId appId : finishedApplicationsToAdd) {
        
          FinishedApplications hopFinishedApplications =
              new FinishedApplications(rmnodeId, appId.toString());
          toAddHopFinishedApplications.add(hopFinishedApplications);
        
      }
      agregate.addAllFinishedAppToAdd(toAddHopFinishedApplications);
    }
  }
   
 

public void agregateFinishedApplicationToRemove(RMNodeInfoAgregate agregate){
    if (finishedApplicationsToRemove != null &&
        !finishedApplicationsToRemove.isEmpty()) {
      ArrayList<FinishedApplications> toRemoveHopFinishedApplications =
          new ArrayList<FinishedApplications>();
      for (ApplicationId appId : finishedApplicationsToRemove) {
        FinishedApplications hopFinishedApplications =
            new FinishedApplications(rmnodeId, appId.toString());
        toRemoveHopFinishedApplications.add(hopFinishedApplications);
      }
      agregate.addAllFinishedAppToRemove(toRemoveHopFinishedApplications);
    }
  }



  public void toAddLatestNodeHeartBeatResponse(NodeHeartbeatResponse resp) {
    if (resp instanceof NodeHeartbeatResponsePBImpl) {
    this.latestNodeHeartBeatResponse = new NodeHBResponse(rmnodeId,
            ((NodeHeartbeatResponsePBImpl) resp)
                .getProto().toByteArray());
    }else{
      this.latestNodeHeartBeatResponse =new NodeHBResponse(rmnodeId, null);
    }
  }

   public void agregateLatestHeartBeatResponseToAdd(RMNodeInfoAgregate agregate){
    if (latestNodeHeartBeatResponse != null) {
      agregate.addLastHeartbeatResponse(latestNodeHeartBeatResponse);
    }
  }
   
 

  public void toAddNextHeartbeat(String rmnodeid, boolean nextHeartbeat) {
    LOG.debug(
        "HOP :: toAddNextHeartbeat-START:" + rmnodeid + "," + nextHeartbeat);
    this.nextHeartbeat = new NextHeartbeat(rmnodeid, nextHeartbeat);
    LOG.debug(
        "HOP :: toAddNextHeartbeat-FINISH:" + rmnodeid + "," + nextHeartbeat);
  }

  public void agregateNextHeartbeat(RMNodeInfoAgregate agregate){
    NextHeartbeatDataAccess nextHeartbeatDA =
        (NextHeartbeatDataAccess) RMStorageFactory
            .getDataAccess(NextHeartbeatDataAccess.class);
    if (nextHeartbeat != null) {
      agregate.addNextHeartbeat(nextHeartbeat);
    }
    LOG.debug("HOP :: persistNextHeartbeat-FINISH:" + nextHeartbeat);
  }
 
}
