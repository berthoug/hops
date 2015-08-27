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
import io.hops.metadata.yarn.dal.FiCaSchedulerNodeDataAccess;
import io.hops.metadata.yarn.dal.LaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.entity.FiCaSchedulerNode;
import io.hops.metadata.yarn.entity.LaunchedContainers;
import io.hops.metadata.yarn.entity.RMContainer;
import io.hops.metadata.yarn.entity.Resource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Contains FiCaSchedulerNode related classes, used mainly by scheduler.
 */
public class FiCaSchedulerNodeInfoToUpdate {

  private static final Log LOG =
      LogFactory.getLog(FiCaSchedulerNodeInfoToUpdate.class);
  
  private final TransactionStateImpl transactionState;
  private FiCaSchedulerNode
      infoToUpdate;
  private final String id;
  private Map<String, String> launchedContainersToAdd=new HashMap<String, String>();
  private Set<String> launchedContainersToRemove = new HashSet<String>();
  private RMContainer
      reservedRMContainerToRemove;
  private RMContainer reservedContainerToUpdate;
  private Map<Integer, Resource>
      toUpdateResources = new HashMap<Integer, Resource>();

  public FiCaSchedulerNodeInfoToUpdate(String id, TransactionStateImpl transactionState) {
    this.id = id;
    this.transactionState = transactionState;
  }

  public void persist(ResourceDataAccess resourceDA,
      FiCaSchedulerNodeDataAccess ficaNodeDA,
      RMContainerDataAccess rmcontainerDA,
      LaunchedContainersDataAccess launchedContainersDA)
      throws StorageException {

    persistToUpdateFicaSchedulerNode(ficaNodeDA, rmcontainerDA);
    persistRmContainerToRemove(rmcontainerDA);
    persistToUpdateFiCaSchedulerNodeId(resourceDA, launchedContainersDA);
  }

  public void toUpdateResource(Integer type,
      org.apache.hadoop.yarn.api.records.Resource res) {

    toUpdateResources.put(type, new Resource(id, type, Resource.FICASCHEDULERNODE,
            res.getMemory(),
            res.getVirtualCores(),0));
  }

  public void updateReservedContainer(
          org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node) {
    transactionState.addRMContainerToUpdate((RMContainerImpl) node.getReservedContainer());
  }

  public void toRemoveRMContainer(
      org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer rmContainer) {
    reservedRMContainerToRemove = new RMContainer(
          rmContainer.getContainer().getId().toString());
  }

  public void toAddLaunchedContainers(String cid, String rmcon) {
    launchedContainersToAdd.put(cid, rmcon);
    launchedContainersToRemove.remove(cid);
  }

  public void toRemoveLaunchedContainers(String cid) {
    if (launchedContainersToAdd.remove(cid)==null){
    launchedContainersToRemove.add(cid);
    }
  }

  public void infoToUpdate(
          org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node) {
    String reservedContainer = node.getReservedContainer() != null ? node.
            getReservedContainer().toString() : null;

    infoToUpdate = new FiCaSchedulerNode(node.getNodeID().toString(),
            node.getNodeName(), node.
            getNumContainers(), reservedContainer);
  }

  public void persistToUpdateResources(ResourceDataAccess resourceDA)
          throws StorageException {
    if (toUpdateResources != null) {
      resourceDA.addAll(toUpdateResources.values());
    }
  }

  public void persistToUpdateFiCaSchedulerNodeId(ResourceDataAccess resourceDA,
      LaunchedContainersDataAccess launchedContainersDA)
      throws StorageException {
    persistLaunchedContainersToAdd(launchedContainersDA);
    persistLaunchedContainersToRemove(launchedContainersDA);
    persistToUpdateResources(resourceDA);
  }

  private void persistRmContainerToRemove(RMContainerDataAccess rmcontainerDA)
      throws StorageException {
    if (reservedRMContainerToRemove != null) {
      rmcontainerDA.remove(reservedRMContainerToRemove);
    }
  }

  private void persistLaunchedContainersToAdd(
      LaunchedContainersDataAccess launchedContainersDA)
      throws StorageException {
    if (launchedContainersToAdd != null) {
      ArrayList<LaunchedContainers> toAddLaunchedContainers =
          new ArrayList<LaunchedContainers>();
      for (String key : launchedContainersToAdd.keySet()) {
        if (launchedContainersToRemove == null || !launchedContainersToRemove.
            remove(key)) {

          String val = launchedContainersToAdd.get(key);
          toAddLaunchedContainers.add(new LaunchedContainers(id, key, val));
        }
      }
      launchedContainersDA.addAll(toAddLaunchedContainers);
    }
  }

  private void persistLaunchedContainersToRemove(
      LaunchedContainersDataAccess launchedContainersDA)
      throws StorageException {
    if (launchedContainersToRemove != null) {
      ArrayList<LaunchedContainers> toRemoveLaunchedContainers =
          new ArrayList<LaunchedContainers>();
      for (String key : launchedContainersToRemove) {
        toRemoveLaunchedContainers.add(new LaunchedContainers(id, key, null));
      }
      launchedContainersDA.removeAll(toRemoveLaunchedContainers);
    }
  }

  public void persistToUpdateFicaSchedulerNode(
          FiCaSchedulerNodeDataAccess ficaNodeDA,
          RMContainerDataAccess rmcontainerDA) throws StorageException {
    if (infoToUpdate != null) {

      ficaNodeDA.add(infoToUpdate);
    }
  }
}
