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

import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.AppSchedulingInfoBlacklistDataAccess;
import io.hops.metadata.yarn.dal.AppSchedulingInfoDataAccess;
import io.hops.metadata.yarn.dal.ContainerDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppLastScheduledContainerDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppLiveContainersDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppNewlyAllocatedContainersDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppReservationsDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppSchedulingOpportunitiesDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.ResourceRequestDataAccess;
import io.hops.metadata.yarn.dal.capacity.FiCaSchedulerAppReservedContainersDataAccess;
import io.hops.metadata.yarn.entity.AppSchedulingInfo;
import io.hops.metadata.yarn.entity.AppSchedulingInfoBlacklist;
import io.hops.metadata.yarn.entity.Container;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppContainer;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppLastScheduledContainer;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppReservedContainerInfo;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppSchedulingOpportunities;
import io.hops.metadata.yarn.entity.Resource;
import io.hops.metadata.yarn.entity.ResourceRequest;
import io.hops.metadata.yarn.entity.SchedulerAppReservations;
import io.hops.metadata.yarn.entity.ToPersistContainersInfo;
import io.hops.metadata.yarn.entity.capacity.FiCaSchedulerAppReservedContainers;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Priority;

public class AgregatedAppInfo {

  private static final Log LOG = LogFactory.getLog(AgregatedAppInfo.class);

  List<AppSchedulingInfo> appSchedulingInfoToPersist
          = new ArrayList<AppSchedulingInfo>();
  List<AppSchedulingInfo> appSchedulingInfoToRemove
          = new ArrayList<AppSchedulingInfo>();
  Set<Resource> resourcesToPersist = new HashSet<Resource>();
  Set<Resource> resourcesToRemove = new HashSet<Resource>();
  List<FiCaSchedulerAppReservedContainers> toAddReservedContainers
          = new ArrayList<FiCaSchedulerAppReservedContainers>();
  Set<Container> toAddContainers = new HashSet<Container>();
  List<FiCaSchedulerAppReservedContainers> toRemoveReservedContainers
          = new ArrayList<FiCaSchedulerAppReservedContainers>();
  List<io.hops.metadata.yarn.entity.RMContainer> toRemoveRMContainers
          = new ArrayList<io.hops.metadata.yarn.entity.RMContainer>();
  List<FiCaSchedulerAppLastScheduledContainer> toAddLastScheduledCont
          = new ArrayList<FiCaSchedulerAppLastScheduledContainer>();
  List<FiCaSchedulerAppSchedulingOpportunities> toAddSO
          = new ArrayList<FiCaSchedulerAppSchedulingOpportunities>();
  List<SchedulerAppReservations> toAddReReservations
          = new ArrayList<SchedulerAppReservations>();
  List<FiCaSchedulerAppContainer> toAddNewlyAllocatedContainersList
          = new ArrayList<FiCaSchedulerAppContainer>();
  List<FiCaSchedulerAppContainer> toRemoveNewlyAllocatedContainersList
          = new ArrayList<FiCaSchedulerAppContainer>();
  List<FiCaSchedulerAppContainer> toAddLiveContainers
          = new ArrayList<FiCaSchedulerAppContainer>();
  List<FiCaSchedulerAppContainer> toRemoveLiveContainers
          = new ArrayList<FiCaSchedulerAppContainer>();
  List<ResourceRequest> toAddResourceRequests = new ArrayList<ResourceRequest>();
  List<ResourceRequest> toRemoveResourceRequests
          = new ArrayList<ResourceRequest>();
  List<AppSchedulingInfoBlacklist> toAddblackListed
          = new ArrayList<AppSchedulingInfoBlacklist>();
  List<AppSchedulingInfoBlacklist> toRemoveblackListed
          = new ArrayList<AppSchedulingInfoBlacklist>();

  public void addFiCaSchedulerApp(AppSchedulingInfo appInfo) {
    appSchedulingInfoToPersist.add(appInfo);
  }

  public void addFiCaSchedulerAppToRemove(AppSchedulingInfo appInfo) {
    appSchedulingInfoToRemove.add(appInfo);
  }

  public void addAllResources(Collection<Resource> r) {
    resourcesToPersist.addAll(r);
  }

  public void addAllResourcesToRemove(Collection<Resource> r) {
    resourcesToRemove.addAll(r);
  }

  public void addAllReservedContainers(
          List<FiCaSchedulerAppReservedContainers> reserved) {
    toAddReservedContainers.addAll(reserved);
  }

  public void addAllContainers(List<Container> containers) {
    toAddContainers.addAll(containers);
  }

  public void addAllReservedContainersToRemove(
          List<FiCaSchedulerAppReservedContainers> toRemove) {
    toRemoveReservedContainers.addAll(toRemove);
  }

  public void addAllRMContainersToRemove(
          List<io.hops.metadata.yarn.entity.RMContainer> toRemove) {
    toRemoveRMContainers.addAll(toRemove);
  }

  public void addAllLastScheduerContainersToAdd(
          List<FiCaSchedulerAppLastScheduledContainer> toAdd) {
    toAddLastScheduledCont.addAll(toAdd);
  }

  public void addAllSchedulingOportunitiesToAdd(
          List<FiCaSchedulerAppSchedulingOpportunities> toAdd) {
    toAddSO.addAll(toAdd);
  }

  public void addAllReReservateion(List<SchedulerAppReservations> toAdd) {
    toAddReReservations.addAll(toAdd);
  }

  public void addAllNewlyAllocatedcontainersToAdd(
          List<FiCaSchedulerAppContainer> toAdd) {
    toAddNewlyAllocatedContainersList.addAll(toAdd);
  }

  public void addAllNewlyAllocatedContainersToRemove(
          List<FiCaSchedulerAppContainer> toRemove) {
    toRemoveNewlyAllocatedContainersList.addAll(toRemove);
  }

  public void addAllLiveContainersToAdd(List<FiCaSchedulerAppContainer> toAdd) {
    toAddLiveContainers.addAll(toAdd);
  }

  public void addAllLiveContainersToRemove(
          List<FiCaSchedulerAppContainer> toRemove) {
    toRemoveLiveContainers.addAll(toRemove);
  }

  public void addAllResourceRequest(List<ResourceRequest> toAdd) {
    toAddResourceRequests.addAll(toAdd);
  }

  public void addAllResourceRequestsToRemove(List<ResourceRequest> toRemove) {
    toRemoveResourceRequests.addAll(toRemove);
  }

  public void addAllBlackListToAdd(List<AppSchedulingInfoBlacklist> toAddb) {
    toAddblackListed.addAll(toAddb);
  }

  public void addAllBlackListToRemove(List<AppSchedulingInfoBlacklist> toRemove) {
    toRemoveblackListed.addAll(toRemove);
  }

  static double totalt1 = 0;
  static double totalt2 = 0;
  static double totalt3 = 0;
  static double totalt4 = 0;
  static double totalt5 = 0;
  static double totalt6 = 0;
  static double totalt7 = 0;
  static double totalt8 = 0;
  static double totalt9 = 0;
  static double totalt10 = 0;
  static double totalt11 = 0;
  static double avgt1 = 0;
  static double avgt2 = 0;
  static double avgt3 = 0;
  static double avgt4 = 0;
  static double avgt5 = 0;
  static double avgt6 = 0;
  static double avgt7 = 0;
  static double avgt8 = 0;
  static double avgt9 = 0;
  static double avgt10 = 0;
  static double avgt11 = 0;
  static double nbFinish =0;
  public void persist() throws StorageException {
    long start = System.currentTimeMillis();
    persistApplicationToAdd();
    long t1 = System.currentTimeMillis() - start;
    persistContainers();
    long t2 = System.currentTimeMillis() - start;
    persistReservedContainersToAdd();
    persistReservedContainersToRemove();
    long t3 = System.currentTimeMillis() - start;
    //TORECOVER FAIR used only in fair scheduler
//    persistLastScheduledContainersToAdd();
    persistSchedulingOpportunitiesToAdd();
    long t4 = System.currentTimeMillis() - start;
    persistReReservations();
    long t5 = System.currentTimeMillis() - start;
    persistNewlyAllocatedContainersToAdd();
    persistNewlyAllocatedContainersToRemove();
    long t6 = System.currentTimeMillis() - start;
    persistLiveContainersToAdd();
    persistLiveContainersToRemove();
    long t7 = System.currentTimeMillis() - start;
    persistRequestsToAdd();
    persistRequestsToRemove();
    long t8 = System.currentTimeMillis() - start;
    persistBlackListsToAdd();
    persistBlackListsToRemove();
    long t9 = System.currentTimeMillis() - start;
    persistToUpdateResources();
    long t10 = System.currentTimeMillis() - start;
    persistRemoval();
    long t11 = System.currentTimeMillis() - start;
    if (t11 > 100) {
      LOG.error("persist agregated app info too long: " + t1 + ", " + t2 + ", "
              + t3 + ", " + t4 + ", " + t5 + ", " + t6 + ", " + t7 + ", " + t8
              + ", " + t9 + ", " + t10 + ", " + t11);
    }
    totalt1 += t1;
    totalt2 += t2;
    totalt3 += t3;
    totalt4 += t4;
    totalt5 += t5;
    totalt6 += t6;
    totalt7 += t7;
    totalt8 += t8;
    totalt9 += t9;
    totalt10 += t10;
    totalt11 += t11;
    avgt1=totalt1/nbFinish;
    avgt2=totalt2/nbFinish;
    avgt3=totalt3/nbFinish;
    avgt4=totalt4/nbFinish;
    avgt5=totalt5/nbFinish;
    avgt6=totalt6/nbFinish;
    avgt7=totalt7/nbFinish;
    avgt8=totalt8/nbFinish;
    avgt9=totalt9/nbFinish;
    avgt10=totalt10/nbFinish;
    avgt11=totalt11/nbFinish;
    nbFinish++;
    if(nbFinish%100==0){
      LOG.info("avg time in agregated app info persist: " + avgt1 + ", " + avgt2 + ", " + avgt3 + ", " + avgt4 + ", " + avgt5 + ", " + avgt6 + ", " + avgt7 + ", " + avgt8 + ", " + avgt9 + ", " + avgt10 + ", " + avgt11);
    }
  }

  private void persistApplicationToAdd() throws StorageException {
    AppSchedulingInfoDataAccess asinfoDA
            = (AppSchedulingInfoDataAccess) RMStorageFactory
            .getDataAccess(AppSchedulingInfoDataAccess.class);

    asinfoDA.addAll(appSchedulingInfoToPersist);
  }

  protected void persistContainers() throws StorageException {
    ContainerDataAccess cDA = (ContainerDataAccess) RMStorageFactory.
            getDataAccess(ContainerDataAccess.class);
    cDA.addAll(toAddContainers);
  }

  protected void persistReservedContainersToAdd() throws StorageException {
    FiCaSchedulerAppReservedContainersDataAccess reservedContDA
            = (FiCaSchedulerAppReservedContainersDataAccess) RMStorageFactory.
            getDataAccess(FiCaSchedulerAppReservedContainersDataAccess.class);

    reservedContDA.addAll(toAddReservedContainers);
  }

  protected void persistReservedContainersToRemove() throws StorageException {
    FiCaSchedulerAppReservedContainersDataAccess reservedContDA
            = (FiCaSchedulerAppReservedContainersDataAccess) RMStorageFactory.
            getDataAccess(FiCaSchedulerAppReservedContainersDataAccess.class);

    RMContainerDataAccess rmcDA = (RMContainerDataAccess) RMStorageFactory.
            getDataAccess(RMContainerDataAccess.class);

    reservedContDA.removeAll(toRemoveReservedContainers);
    rmcDA.removeAll(toRemoveRMContainers);
  }

  protected void persistLastScheduledContainersToAdd() throws StorageException {
    FiCaSchedulerAppLastScheduledContainerDataAccess lsDA
            = (FiCaSchedulerAppLastScheduledContainerDataAccess) RMStorageFactory.
            getDataAccess(
                    FiCaSchedulerAppLastScheduledContainerDataAccess.class);
    lsDA.addAll(toAddLastScheduledCont);
  }

  protected void persistSchedulingOpportunitiesToAdd() throws StorageException {
    FiCaSchedulerAppSchedulingOpportunitiesDataAccess soDA
            = (FiCaSchedulerAppSchedulingOpportunitiesDataAccess) RMStorageFactory.
            getDataAccess(
                    FiCaSchedulerAppSchedulingOpportunitiesDataAccess.class);
    soDA.addAll(toAddSO);
  }

  protected void persistReReservations() throws StorageException {
    FiCaSchedulerAppReservationsDataAccess reservationsDA
            = (FiCaSchedulerAppReservationsDataAccess) RMStorageFactory.
            getDataAccess(FiCaSchedulerAppReservationsDataAccess.class);

    reservationsDA.addAll(toAddReReservations);
  }

  private void persistNewlyAllocatedContainersToAdd() throws StorageException {
    //Persist NewllyAllocatedContainers list
    FiCaSchedulerAppNewlyAllocatedContainersDataAccess fsanDA
            = (FiCaSchedulerAppNewlyAllocatedContainersDataAccess) RMStorageFactory.
            getDataAccess(
                    FiCaSchedulerAppNewlyAllocatedContainersDataAccess.class);
    fsanDA.addAll(toAddNewlyAllocatedContainersList);
  }

  private void persistNewlyAllocatedContainersToRemove()
          throws StorageException {
    //Remove NewllyAllocatedContainers list
    FiCaSchedulerAppNewlyAllocatedContainersDataAccess fsanDA
            = (FiCaSchedulerAppNewlyAllocatedContainersDataAccess) RMStorageFactory.
            getDataAccess(
                    FiCaSchedulerAppNewlyAllocatedContainersDataAccess.class);
    fsanDA.removeAll(toRemoveNewlyAllocatedContainersList);
  }

  private void persistLiveContainersToAdd() throws StorageException {
    //Persist LiveContainers
    FiCaSchedulerAppLiveContainersDataAccess fsalcDA
            = (FiCaSchedulerAppLiveContainersDataAccess) RMStorageFactory.
            getDataAccess(FiCaSchedulerAppLiveContainersDataAccess.class);
    fsalcDA.addAll(toAddLiveContainers);

  }

  private void persistLiveContainersToRemove() throws StorageException {
    FiCaSchedulerAppLiveContainersDataAccess fsalcDA
            = (FiCaSchedulerAppLiveContainersDataAccess) RMStorageFactory.
            getDataAccess(FiCaSchedulerAppLiveContainersDataAccess.class);
    fsalcDA.removeAll(toRemoveLiveContainers);
  }

  private void persistRequestsToAdd() throws StorageException {
    //Persist AppSchedulingInfo requests map and ResourceRequest
    ResourceRequestDataAccess resRequestDA
            = (ResourceRequestDataAccess) RMStorageFactory
            .getDataAccess(ResourceRequestDataAccess.class);
    resRequestDA.addAll(toAddResourceRequests);
  }

  private void persistRequestsToRemove() throws StorageException {
    //Remove AppSchedulingInfo requests map and ResourceRequest
    ResourceRequestDataAccess resRequestDA
            = (ResourceRequestDataAccess) RMStorageFactory
            .getDataAccess(ResourceRequestDataAccess.class);
    resRequestDA.removeAll(toRemoveResourceRequests);
  }

  private void persistBlackListsToAdd() throws StorageException {
    AppSchedulingInfoBlacklistDataAccess blDA
            = (AppSchedulingInfoBlacklistDataAccess) RMStorageFactory.
            getDataAccess(AppSchedulingInfoBlacklistDataAccess.class);
    blDA.addAll(toAddblackListed);
  }

  private void persistBlackListsToRemove() throws StorageException {
    AppSchedulingInfoBlacklistDataAccess blDA
            = (AppSchedulingInfoBlacklistDataAccess) RMStorageFactory.
            getDataAccess(AppSchedulingInfoBlacklistDataAccess.class);
    blDA.removeAll(toRemoveblackListed);
  }

  private void persistToUpdateResources() throws StorageException {
    ResourceDataAccess resourceDA = (ResourceDataAccess) RMStorageFactory
            .getDataAccess(ResourceDataAccess.class);
    resourceDA.addAll(resourcesToPersist);
  }

  private void persistRemoval() throws StorageException {
    AppSchedulingInfoDataAccess asinfoDA
            = (AppSchedulingInfoDataAccess) RMStorageFactory
            .getDataAccess(AppSchedulingInfoDataAccess.class);
    ResourceDataAccess resourceDA = (ResourceDataAccess) RMStorageFactory.
            getDataAccess(ResourceDataAccess.class);

    asinfoDA.removeAll(appSchedulingInfoToRemove);

    resourceDA.removeAll(resourcesToRemove);
  }
}
