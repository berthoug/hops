/*
 * Copyright 2015 Apache Software Foundation.
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FiCaSchedulerNodeInfoAgregate {

  public static final Log LOG = LogFactory.getLog(
          FiCaSchedulerNodeInfoAgregate.class);
  private List<FiCaSchedulerNode> infosToUpdate
          = new ArrayList<FiCaSchedulerNode>();
  private List<RMContainer> reservedContainersToRemove
          = new ArrayList<RMContainer>();
  ArrayList<LaunchedContainers> toAddLaunchedContainers
          = new ArrayList<LaunchedContainers>();
  ArrayList<LaunchedContainers> toRemoveLaunchedContainers
          = new ArrayList<LaunchedContainers>();
  List<Resource> toUpdateResources = new ArrayList<Resource>();

  public void addToUpdateFiCaSchedulerNode(FiCaSchedulerNode infoToUpdate) {
    infosToUpdate.add(infoToUpdate);
  }

  public void addReservedRMContainerToRemove(
          RMContainer reservedRMContainerToRemove) {
    reservedContainersToRemove.add(reservedRMContainerToRemove);
  }

  public void addAlllaunchedContainersToAdd(
          ArrayList<LaunchedContainers> toAddLaunchedContainers) {
    this.toAddLaunchedContainers.addAll(toAddLaunchedContainers);
  }

  public void addAllLaunchedContainersToRemove(
          ArrayList<LaunchedContainers> toRemoveLaunchedContainers) {
    this.toRemoveLaunchedContainers.addAll(toAddLaunchedContainers);
  }

  public void addAllResourcesToUpdate(Collection<Resource> toUpdateResources) {
    this.toUpdateResources.addAll(toUpdateResources);
  }

  static double totalt1 = 0;
  static double totalt2 = 0;
  static double totalt3 = 0;
  static double totalt4 = 0;
  static double totalt5 = 0;
  static long nbFinish = 0;

  public void persist(ResourceDataAccess resourceDA,
          FiCaSchedulerNodeDataAccess ficaNodeDA,
          RMContainerDataAccess rmcontainerDA,
          LaunchedContainersDataAccess launchedContainersDA)
          throws StorageException {
    Long start = System.currentTimeMillis();
    persistToUpdateFicaSchedulerNode(ficaNodeDA, rmcontainerDA);
    totalt1 = totalt1 + System.currentTimeMillis() - start;
    persistRmContainerToRemove(rmcontainerDA);
    totalt2 = totalt2 + System.currentTimeMillis() - start;
    persistLaunchedContainersToAdd(launchedContainersDA);
    totalt3 = totalt3 + System.currentTimeMillis() - start;
    persistLaunchedContainersToRemove(launchedContainersDA);
    totalt4 = totalt4 + System.currentTimeMillis() - start;
    persistToUpdateResources(resourceDA);
    totalt5 = totalt5 + System.currentTimeMillis() - start;
    nbFinish++;
    if (nbFinish % 100 == 0) {
      double avgt1 = totalt1 / nbFinish;
      double avgt2 = totalt2 / nbFinish;
      double avgt3 = totalt3 / nbFinish;
      double avgt4 = totalt4 / nbFinish;
      double avgt5 = totalt5 / nbFinish;
      LOG.info("avg time commit FiCASchedulerNodeInfo: " + avgt1 + ", " + avgt2
              + ", " + avgt3 + ", " + avgt4 + ", " + avgt5);
    }
  }

  public void persistToUpdateFicaSchedulerNode(
          FiCaSchedulerNodeDataAccess ficaNodeDA,
          RMContainerDataAccess rmcontainerDA) throws StorageException {
    if (!infosToUpdate.isEmpty()) {

      ficaNodeDA.addAll(infosToUpdate);
    }
  }

  private void persistRmContainerToRemove(RMContainerDataAccess rmcontainerDA)
          throws StorageException {
    if (!reservedContainersToRemove.isEmpty()) {
      rmcontainerDA.removeAll(reservedContainersToRemove);
    }
  }

  private void persistLaunchedContainersToAdd(
          LaunchedContainersDataAccess launchedContainersDA)
          throws StorageException {
    if (!toAddLaunchedContainers.isEmpty()) {
      launchedContainersDA.addAll(toAddLaunchedContainers);
    }
  }

  private void persistLaunchedContainersToRemove(
          LaunchedContainersDataAccess launchedContainersDA)
          throws StorageException {
    if (!toRemoveLaunchedContainers.isEmpty()) {
      launchedContainersDA.removeAll(toRemoveLaunchedContainers);
    }
  }

  public void persistToUpdateResources(ResourceDataAccess resourceDA)
          throws StorageException {
    if (!toUpdateResources.isEmpty()) {
      resourceDA.addAll(toUpdateResources);
    }
  }
}
