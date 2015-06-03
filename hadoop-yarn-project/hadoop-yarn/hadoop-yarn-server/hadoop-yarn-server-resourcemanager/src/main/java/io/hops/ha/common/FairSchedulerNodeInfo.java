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
import io.hops.metadata.yarn.dal.LaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.fair.AppSchedulableDataAccess;
import io.hops.metadata.yarn.dal.fair.FSSchedulerNodeDataAccess;
import io.hops.metadata.yarn.entity.LaunchedContainers;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AppSchedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerNode;

public class FairSchedulerNodeInfo {
    private static final Log LOG = LogFactory.getLog(FairSchedulerNodeInfo.class);

  private Map<NodeId, FSSchedulerNode> fsschedulerNodesToAdd;
  private Map<NodeId, FSSchedulerNode> fsschedulerNodesToRemove;

  //when we update values related directly to hopFSSchedulerNode, we use this variable to refer to the right node
  private FSSchedulerNode fairSchedulerNode;
  //otherwise, (e.g. for resources and launchedContainers, we use this variable
  private String fairSchedulerNodeId;

  private Map<String, String> launchedContainersToAdd;
  private Set<String> launchedContainersToRemove;

  private RMContainer reservedRMContainerToRemove;
  private AppSchedulable reservedappSchedulableToRemove;

  private Map<Integer, Resource> resourcesToUpdate;

  void persist(FSSchedulerNodeDataAccess FSSNodeDA, ResourceDataAccess resourceDA,
          RMContainerDataAccess rmcontainerDA, LaunchedContainersDataAccess launchedContainersDA, AppSchedulableDataAccess appSDA) throws StorageException {
    persistFSSchedulerNodesToAdd(FSSNodeDA, resourceDA);
    persistFSSchedulerNodesToRemove(FSSNodeDA, resourceDA, launchedContainersDA, rmcontainerDA, appSDA);
    persistFFSchdulerNodesToUpate(FSSNodeDA, rmcontainerDA, appSDA);
    persistFSSchedulerNodeId(resourceDA, launchedContainersDA);
  }

  private void persistFSSchedulerNodesToAdd(FSSchedulerNodeDataAccess FSSNodeDA, ResourceDataAccess resourceDA) throws StorageException {
    if (fsschedulerNodesToAdd != null) {
      List<io.hops.metadata.yarn.entity.fair.FSSchedulerNode> toAddFSSchedulerNodes = new ArrayList<io.hops.metadata.yarn.entity.fair.FSSchedulerNode>();
      List<io.hops.metadata.yarn.entity.Resource> toAddResources = new ArrayList<io.hops.metadata.yarn.entity.Resource>();
      for (NodeId nodeId : fsschedulerNodesToAdd.keySet()) {
        FSSchedulerNode fssnode = fsschedulerNodesToAdd.get(nodeId);
        toAddFSSchedulerNodes.add(new io.hops.metadata.yarn.entity.fair.FSSchedulerNode(nodeId.toString(), fssnode.getNumContainers(),
                null, null));
                                                //fssnode.getReservedContainer().toString(), fssnode.getReservedAppSchedulable().toString()));

        //Add Resources
        if (fssnode.getAvailableResource() != null) {
          toAddResources.add(new io.hops.metadata.yarn.entity.Resource(nodeId.toString(), io.hops.metadata.yarn.entity.Resource.AVAILABLE, io.hops.metadata.yarn.entity.Resource.FSSCHEDULERNODE,
                  fssnode.getAvailableResource().getMemory(),
                  fssnode.getAvailableResource().getVirtualCores()));
        }
        if (fssnode.getTotalResource() != null) {
          toAddResources.add(new io.hops.metadata.yarn.entity.Resource(nodeId.toString(), io.hops.metadata.yarn.entity.Resource.TOTAL_CAPABILITY, io.hops.metadata.yarn.entity.Resource.FSSCHEDULERNODE,
                  fssnode.getTotalResource().getMemory(),
                  fssnode.getTotalResource().getVirtualCores()));
        }
        if (fssnode.getUsedResource() != null) {
          toAddResources.add(new io.hops.metadata.yarn.entity.Resource(nodeId.toString(), io.hops.metadata.yarn.entity.Resource.USED, io.hops.metadata.yarn.entity.Resource.FSSCHEDULERNODE,
                  fssnode.getUsedResource().getMemory(),
                  fssnode.getUsedResource().getVirtualCores()));
        }
        //For persistance of reservedContainer, reservedAppSchedulable and launchedContainers
        //see method persistToAddFiCaSchedulerNode of class FiCaSchedulerNodeInfo
        //here we don't persist these values as they are all null when the node is created
      }

      FSSNodeDA.addAll(toAddFSSchedulerNodes);
      resourceDA.addAll(toAddResources);
    }
  }

  private void persistFSSchedulerNodesToRemove(FSSchedulerNodeDataAccess FSSNodeDA, ResourceDataAccess resourceDA,
          LaunchedContainersDataAccess launchedContainersDA, RMContainerDataAccess rmcontainerDA,
          AppSchedulableDataAccess appSDA) throws StorageException {
    if (fsschedulerNodesToRemove != null) {
      List<io.hops.metadata.yarn.entity.fair.FSSchedulerNode> toRemoveFSSchedulerNodes = new ArrayList<io.hops.metadata.yarn.entity.fair.FSSchedulerNode>();
      List<io.hops.metadata.yarn.entity.Resource> toRemoveResources = new ArrayList<io.hops.metadata.yarn.entity.Resource>();
      List<io.hops.metadata.yarn.entity.RMContainer> toRemoveContainer = null;
      List<LaunchedContainers> toRemoveLaunchedContainers = null;
      List<io.hops.metadata.yarn.entity.fair.AppSchedulable> toRemoveAppSchedulable = null;
      for (NodeId nodeId : fsschedulerNodesToRemove.keySet()) {
        FSSchedulerNode fssnode = fsschedulerNodesToRemove.get(nodeId);
        toRemoveFSSchedulerNodes.add(new io.hops.metadata.yarn.entity.fair.FSSchedulerNode(nodeId.toString(), fssnode.getNumContainers(),
                null, null));
        //fssnode.getReservedContainer().toString(), fssnode.getReservedAppSchedulable().toString()));
        //Remove Resources
        toRemoveResources.add(new io.hops.metadata.yarn.entity.Resource(nodeId.toString(), io.hops.metadata.yarn.entity.Resource.TOTAL_CAPABILITY, io.hops.metadata.yarn.entity.Resource.FSSCHEDULERNODE, 0, 0));
        toRemoveResources.add(new io.hops.metadata.yarn.entity.Resource(nodeId.toString(), io.hops.metadata.yarn.entity.Resource.AVAILABLE, io.hops.metadata.yarn.entity.Resource.FSSCHEDULERNODE, 0, 0));
        toRemoveResources.add(new io.hops.metadata.yarn.entity.Resource(nodeId.toString(), io.hops.metadata.yarn.entity.Resource.USED, io.hops.metadata.yarn.entity.Resource.FSSCHEDULERNODE, 0, 0));

        //Remove reservedContainer
        RMContainer container = fssnode.getReservedContainer();
        if (container != null) {
          if (toRemoveContainer == null) {
            toRemoveContainer = new ArrayList<io.hops.metadata.yarn.entity.RMContainer>();
          }

          boolean isReserved = (fairSchedulerNode.getReservedContainer().getReservedNode() != null)
                  && (fairSchedulerNode.getReservedContainer().getReservedPriority() != null);

          String reservedNode = isReserved ? container.getReservedNode().toString() : null;
          int reservedPriority = isReserved ? container.getReservedPriority().getPriority() : Integer.MIN_VALUE;
          int reservedMemory = isReserved ? container.getReservedResource().getMemory() : 0;
          int reservedVCores = isReserved ? container.getReservedResource().getVirtualCores() : 0;
          String reservedHost = isReserved ? container.getReservedNode().getHost() : null;
          int reservedPort = isReserved ? container.getReservedNode().getPort() : 0;

          toRemoveContainer.add(new io.hops.metadata.yarn.entity.RMContainer(container.getContainerId().toString(),
                  container.getApplicationAttemptId().toString(),
                  container.getNodeId().toString(),
                  container.getUser(),
                  reservedNode,
                  reservedPriority,
                  reservedMemory,
                  reservedVCores,
                  container.getStartTime(),
                  container.getFinishTime(),
                  container.getState().toString(),
                  reservedHost,
                  reservedPort,
                  ((RMContainerImpl) container).getContainerState().toString(),
                  ((RMContainerImpl) container).getContainerExitStatus()));
        }

        //Remove launched containers
        if (fssnode.getRunningContainers() != null) {
          if (toRemoveLaunchedContainers == null) {
            toRemoveLaunchedContainers = new ArrayList<LaunchedContainers>();
          }
          for (RMContainer rmContainer : fssnode.getRunningContainers()) {
            toRemoveLaunchedContainers.add(new LaunchedContainers(nodeId.toString(), rmContainer.getContainerId().toString(), null));
          }
        }

        //Remove reservedAppSchedulable
        if (fssnode.getReservedAppSchedulable() != null) {
          if (toRemoveAppSchedulable == null) {
            toRemoveAppSchedulable = new ArrayList<io.hops.metadata.yarn.entity.fair.AppSchedulable>();
          }

          toRemoveAppSchedulable.add(new io.hops.metadata.yarn.entity.fair.AppSchedulable(fssnode.getReservedAppSchedulable().getName(),
                  fssnode.getReservedAppSchedulable().getStartTime(),
                  fssnode.getReservedAppSchedulable().getQueuename()));
        }
      }

      FSSNodeDA.removeAll(toRemoveFSSchedulerNodes);
      resourceDA.removeAll(toRemoveResources);
      rmcontainerDA.removeAll( toRemoveContainer);
      launchedContainersDA.removeAll( toRemoveLaunchedContainers);
      appSDA.removeAll( toRemoveAppSchedulable);
    }
  }

  private void persistFFSchdulerNodesToUpate(FSSchedulerNodeDataAccess fSSNodeDA, RMContainerDataAccess rmcontainerDA, AppSchedulableDataAccess appSDA) throws StorageException {
    if (fairSchedulerNode != null) {
      List<io.hops.metadata.yarn.entity.fair.FSSchedulerNode> updateFSSNode = new ArrayList<io.hops.metadata.yarn.entity.fair.FSSchedulerNode>();
      List<io.hops.metadata.yarn.entity.RMContainer> rmContainerToAdd = new ArrayList<io.hops.metadata.yarn.entity.RMContainer>();
      List<io.hops.metadata.yarn.entity.fair.AppSchedulable> appschedulableToAdd = null;

      String reservedContainerId, reservedSchedulableId;

      reservedContainerId = (fairSchedulerNode.getReservedContainer() != null) ? fairSchedulerNode.getReservedContainer().getContainerId().toString() : null;
      reservedSchedulableId = (fairSchedulerNode.getReservedAppSchedulable() != null) ? fairSchedulerNode.getReservedAppSchedulable().getName() : null;

      updateFSSNode.add(new io.hops.metadata.yarn.entity.fair.FSSchedulerNode(fairSchedulerNode.getRMNode().getNodeID().toString(),
              fairSchedulerNode.getNumContainers(),
              reservedContainerId,
              reservedSchedulableId));
      LOG.debug("Trying to persist node with values " + fairSchedulerNode.getRMNode().getNodeID().toString()
              + " " + fairSchedulerNode.getNumContainers() + " " + reservedContainerId + " " + reservedSchedulableId);

      if (reservedContainerId != null) {

        boolean isReserved = (fairSchedulerNode.getReservedContainer().getReservedNode() != null)
                && (fairSchedulerNode.getReservedContainer().getReservedPriority() != null);

        String reservedNode = isReserved ? fairSchedulerNode.getReservedContainer().getReservedNode().toString() : null;
        int reservedPriority = isReserved ? fairSchedulerNode.getReservedContainer().getReservedPriority().getPriority() : Integer.MIN_VALUE;
        int reservedMemory = isReserved ? fairSchedulerNode.getReservedContainer().getReservedResource().getMemory() : 0;
        int reservedVCores = isReserved ? fairSchedulerNode.getReservedContainer().getReservedResource().getVirtualCores() : 0;
        String reservedHost = isReserved ? fairSchedulerNode.getReservedContainer().getReservedNode().getHost() : null;
        int reservedPort = isReserved ? fairSchedulerNode.getReservedContainer().getReservedNode().getPort() : 0;

        rmContainerToAdd.add(new io.hops.metadata.yarn.entity.RMContainer(fairSchedulerNode.getReservedContainer().getContainerId().toString(),
                fairSchedulerNode.getReservedContainer().getApplicationAttemptId().toString(),
                fairSchedulerNode.getReservedContainer().getNodeId().toString(),
                fairSchedulerNode.getReservedContainer().getUser(),
                reservedNode,
                reservedPriority,
                reservedMemory,
                reservedVCores,
                fairSchedulerNode.getReservedContainer().getStartTime(),
                fairSchedulerNode.getReservedContainer().getFinishTime(),
                fairSchedulerNode.getReservedContainer().getState().toString(),
                reservedHost,
                reservedPort,
                ((RMContainerImpl) fairSchedulerNode.getReservedContainer()).getContainerState().toString(),
                ((RMContainerImpl) fairSchedulerNode.getReservedContainer()).getContainerExitStatus()));
      }

      if (reservedSchedulableId != null) {
        if (appschedulableToAdd == null) {
          appschedulableToAdd = new ArrayList<io.hops.metadata.yarn.entity.fair.AppSchedulable>();
        }
        appschedulableToAdd.add(new io.hops.metadata.yarn.entity.fair.AppSchedulable(fairSchedulerNode.getReservedAppSchedulable().getName(),
                fairSchedulerNode.getReservedAppSchedulable().getStartTime(),
                fairSchedulerNode.getReservedAppSchedulable().getQueuename()));
      }

      //WE dont remove the container twice. It is already remove in FairSchedulerAppInfo
      //persistRMContainerToRemove(rmcontainerDA);
      persistAppSchedulableToRemove(appSDA);
      fSSNodeDA.addAll(updateFSSNode);
      rmcontainerDA.addAll(rmContainerToAdd);
      appSDA.addAll(appschedulableToAdd);
    }
  }

//    private void persistRMContainerToRemove(RMContainerDataAccess rmcontainerDA) throws StorageException {
//        if (reservedRMContainerToRemove != null) {
//            List<io.hops.metadata.yarn.entity.RMContainer> rmcontainerToRemove = new ArrayList<io.hops.metadata.yarn.entity.RMContainer>();
//            rmcontainerToRemove.add(new io.hops.metadata.yarn.entity.RMContainer(reservedRMContainerToRemove.getContainerId().toString(),
//                                        reservedRMContainerToRemove.getApplicationAttemptId().toString(),
//                                        reservedRMContainerToRemove.getNodeId().toString(),
//                                        reservedRMContainerToRemove.getUser(),
//                                        reservedRMContainerToRemove.getStartTime(),
//                                        reservedRMContainerToRemove.getFinishTime(),
//                                        reservedRMContainerToRemove.getState().toString(),
//                                        ((RMContainerImpl) reservedRMContainerToRemove).getContainerState().toString(),
//                                        ((RMContainerImpl) reservedRMContainerToRemove).getContainerExitStatus()));
//            
//            LOG.debug("In FairNodeInfo " + reservedRMContainerToRemove.getContainerId().toString() + " " + reservedRMContainerToRemove.getState().toString());
//            rmcontainerDA.prepare(null, rmcontainerToRemove);
//        }
//    }
  private void persistAppSchedulableToRemove(AppSchedulableDataAccess appSDA) throws StorageException {
    if (reservedappSchedulableToRemove != null) {
      List<io.hops.metadata.yarn.entity.fair.AppSchedulable> appSchedulableToRemove = new ArrayList<io.hops.metadata.yarn.entity.fair.AppSchedulable>();
      appSchedulableToRemove.add(new io.hops.metadata.yarn.entity.fair.AppSchedulable(reservedappSchedulableToRemove.getName(),
              reservedappSchedulableToRemove.getStartTime(),
              reservedappSchedulableToRemove.getQueuename()));
      appSDA.removeAll(appSchedulableToRemove);
    }
  }

  private void persistFSSchedulerNodeId(ResourceDataAccess resourceDA, LaunchedContainersDataAccess launchedContainersDA) throws StorageException {
    if (fairSchedulerNodeId != null) {
      persistLaunchedContainersToAdd(launchedContainersDA);
      persistLaunchedContainersToRemove(launchedContainersDA);
      peristResourcesToUpdate(resourceDA);
    }
  }

  private void persistLaunchedContainersToAdd(LaunchedContainersDataAccess launchedContainersDA) throws StorageException {
    if (launchedContainersToAdd != null) {
      List<LaunchedContainers> toAddLaunchedContainers = new ArrayList<LaunchedContainers>();
      for (String key : launchedContainersToAdd.keySet()) {
        String val = launchedContainersToAdd.get(key);
        toAddLaunchedContainers.add(new LaunchedContainers(fairSchedulerNodeId, key, val));
      }
      launchedContainersDA.addAll(toAddLaunchedContainers);
    }
  }

  private void persistLaunchedContainersToRemove(LaunchedContainersDataAccess launchedContainersDA) throws StorageException {
    if (launchedContainersToRemove != null) {
      List<LaunchedContainers> toRemoveLaunchedContainers = new ArrayList<LaunchedContainers>();
      for (String key : launchedContainersToRemove) {
        String val = key;
        toRemoveLaunchedContainers.add(new LaunchedContainers(fairSchedulerNodeId, key, val));
      }
      launchedContainersDA.removeAll(toRemoveLaunchedContainers);
    }
  }

  private void peristResourcesToUpdate(ResourceDataAccess resourceDA) throws StorageException {
    if (resourcesToUpdate != null) {
      List<io.hops.metadata.yarn.entity.Resource> toUpdateResources = new ArrayList<io.hops.metadata.yarn.entity.Resource>();
      for (Integer type : resourcesToUpdate.keySet()) {
        Resource val = resourcesToUpdate.get(type);
        toUpdateResources.add(new io.hops.metadata.yarn.entity.Resource(fairSchedulerNodeId, type,
                io.hops.metadata.yarn.entity.Resource.FSSCHEDULERNODE, val.getMemory(), val.getVirtualCores()));
      }
      resourceDA.addAll(toUpdateResources);
    }
  }

  public void addLaunchedContainer(String containerId, String rmContainerId) {
    if (launchedContainersToAdd == null) {
      launchedContainersToAdd = new HashMap<String, String>();
    }
    launchedContainersToAdd.put(containerId, rmContainerId);
  }

  public void removeLaunchedContainer(String rmContainerId) {
    if (launchedContainersToRemove == null) {
      launchedContainersToRemove = new HashSet<String>();
    }
    launchedContainersToRemove.add(rmContainerId);
  }

  public void addFSSchedulerNode(NodeId nodeId, FSSchedulerNode fsnode) {
    if (fsschedulerNodesToAdd == null) {
      fsschedulerNodesToAdd = new HashMap<NodeId, FSSchedulerNode>();
    }
    fsschedulerNodesToAdd.put(nodeId, fsnode);
  }

  public void updateResources(Integer type, Resource res) {
    if (resourcesToUpdate == null) {
      resourcesToUpdate = new HashMap<Integer, Resource>();
    }
    resourcesToUpdate.put(type, res);
  }

  public void removeFSSchedulerNode(NodeId nodeId, FSSchedulerNode fsnode) {
    if (fsschedulerNodesToRemove == null) {
      fsschedulerNodesToRemove = new HashMap<NodeId, FSSchedulerNode>();
    }
    fsschedulerNodesToRemove.put(nodeId, fsnode);
  }

  public void removeRMContainer(RMContainer rmCont) {
    this.reservedRMContainerToRemove = rmCont;
  }

  public void removeAppSchedulable(AppSchedulable apps) {
    this.reservedappSchedulableToRemove = apps;
  }

  public void toUpdateFSSchedulerNode(FSSchedulerNode fssNode) {
    this.fairSchedulerNode = fssNode;
  }

  public void toUpdateFSSchedulerNodeId(String id) {
    fairSchedulerNodeId = id;
  }
}
