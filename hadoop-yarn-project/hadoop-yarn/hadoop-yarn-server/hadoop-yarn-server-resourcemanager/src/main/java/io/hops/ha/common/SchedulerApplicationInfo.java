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

import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.util.HopYarnAPIUtilities;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.QueueMetricsDataAccess;
import io.hops.metadata.yarn.dal.SchedulerApplicationDataAccess;
import io.hops.metadata.yarn.entity.QueueMetrics;
import io.hops.metadata.yarn.entity.SchedulerApplication;
import io.hops.metadata.yarn.entity.SchedulerApplicationInfoToAdd;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerApp;

/**
 * Contains scheduler specific information about Applications.
 */
public class SchedulerApplicationInfo {

  private static final Log LOG =
      LogFactory.getLog(SchedulerApplicationInfo.class);
  
  private final TransactionStateImpl transactionState;
  private Map<ApplicationId, SchedulerApplicationInfoToAdd>
      schedulerApplicationsToAdd =
      new HashMap<ApplicationId, SchedulerApplicationInfoToAdd>();
  private Set<ApplicationId> applicationsIdToRemove =
      new HashSet<ApplicationId>();
  Lock fiCaSchedulerAppInfoLock = new ReentrantLock();
  private Map<String, Map<String, FiCaSchedulerAppInfo>> fiCaSchedulerAppInfo =
      new HashMap<String, Map<String, FiCaSchedulerAppInfo>>();
  
  public SchedulerApplicationInfo(TransactionStateImpl transactionState){
    this.transactionState = transactionState;
  }
  
  static double totalt1 = 0;
  static double totalt2 = 0;
  static double totalt3 = 0;
  static long nbFinish = 0;
  
  public void persist(QueueMetricsDataAccess QMDA, StorageConnector connector) throws StorageException {
    //TODO: The same QueueMetrics (DEFAULT_QUEUE) is persisted with every app. Its extra overhead. We can persist it just once
    long start = System.currentTimeMillis();
    persistApplicationIdToAdd(QMDA);
    totalt1 = totalt1 +  System.currentTimeMillis()-start;
    persistFiCaSchedulerAppInfo(connector);
    totalt2 = totalt2 + System.currentTimeMillis()-start;
    persistApplicationIdToRemove();
    totalt3 = totalt3 + System.currentTimeMillis()-start;
    nbFinish++;
    if (nbFinish % 100 == 0) {
      double avgt1 = totalt1 / nbFinish;
      double avgt2 = totalt2 / nbFinish;
      double avgt3 = totalt3 / nbFinish;
      LOG.debug("avg time commit scheduler app info: " + avgt1 + ", " + avgt2
              + ", " + avgt3);
    }
  }

  private void persistApplicationIdToAdd(QueueMetricsDataAccess QMDA)
      throws StorageException {
    if (!schedulerApplicationsToAdd.isEmpty()) {
      SchedulerApplicationDataAccess sappDA =
          (SchedulerApplicationDataAccess) RMStorageFactory
              .getDataAccess(SchedulerApplicationDataAccess.class);
      List<SchedulerApplication> toAddSchedulerApp =
          new ArrayList<SchedulerApplication>();
      for (SchedulerApplicationInfoToAdd appInfo : schedulerApplicationsToAdd.values()) {
        

          LOG.debug("adding scheduler app " + appInfo.getSchedulerApplication().getAppid());


          toAddSchedulerApp.add(appInfo.getSchedulerApplication());
        
      }
      sappDA.addAll(toAddSchedulerApp);
    }
  }

  private void persistApplicationIdToRemove() throws StorageException {
    if (!applicationsIdToRemove.isEmpty()) {
      SchedulerApplicationDataAccess sappDA =
          (SchedulerApplicationDataAccess) RMStorageFactory
              .getDataAccess(SchedulerApplicationDataAccess.class);
      List<SchedulerApplication> applicationsToRemove =
          new ArrayList<SchedulerApplication>();
      for (ApplicationId appId : applicationsIdToRemove) {
        applicationsToRemove
            .add(new SchedulerApplication(appId.toString(), null, null));
      }
      sappDA.removeAll(applicationsToRemove);
      //TORECOVER OPT clean the table that depend on this one
    }
  }

  public void setSchedulerApplicationtoAdd(
          org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication schedulerApplicationToAdd,
          ApplicationId applicationIdToAdd) {
    //TODO the queue metrics are shared by several nodes they should not be persisted for each nodes.
//    org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics toAddQM
//            = schedulerApplicationToAdd.getQueue().getMetrics();
//    QueueMetrics toAdHopQueueMetrics = new QueueMetrics(toAddQM.getQueueName(),
//            toAddQM.getAppsSubmitted(), toAddQM.getAppsRunning(),
//            toAddQM.getAppsPending(), toAddQM.getAppsCompleted(),
//            toAddQM.getAppsKilled(), toAddQM.getAppsFailed(),
//            toAddQM.getAllocatedMB(), toAddQM.getAllocatedVirtualCores(),
//            toAddQM.getAllocatedContainers(),
//            toAddQM.getAggregateContainersAllocated(),
//            toAddQM.getaggregateContainersReleased(),
//            toAddQM.getAvailableMB(), toAddQM.getAvailableVirtualCores(),
//            toAddQM.getPendingMB(), toAddQM.getPendingVirtualCores(),
//            toAddQM.getPendingContainers(), toAddQM.getReservedMB(),
//            toAddQM.getReservedVirtualCores(),
//            toAddQM.getReservedContainers(), toAddQM.getActiveUsers(),
//            toAddQM.getActiveApps(), 0);

    //Persist SchedulerApplication - Value of applications Map
    SchedulerApplication toAddSchedulerApplication = new SchedulerApplication(
            applicationIdToAdd.toString(),
            schedulerApplicationToAdd.getUser(),
            schedulerApplicationToAdd.getQueue().getQueueName());

    SchedulerApplicationInfoToAdd appInfo = new SchedulerApplicationInfoToAdd(
            toAddSchedulerApplication);

    this.schedulerApplicationsToAdd
            .put(applicationIdToAdd, appInfo);
    applicationsIdToRemove.remove(applicationIdToAdd);
  }

  public void setApplicationIdtoRemove(ApplicationId applicationIdToRemove) {
    if(schedulerApplicationsToAdd.remove(applicationIdToRemove)==null){
      this.applicationsIdToRemove.add(applicationIdToRemove);
    }
  }

  public FiCaSchedulerAppInfo getFiCaSchedulerAppInfo(
      ApplicationAttemptId appAttemptId) {
    fiCaSchedulerAppInfoLock.lock();
    try{
    ApplicationId appId = appAttemptId.getApplicationId();
    if(fiCaSchedulerAppInfo.get(appId.toString())==null){
      fiCaSchedulerAppInfo.put(appId.toString(), new HashMap<String, FiCaSchedulerAppInfo>());
    }
    if (fiCaSchedulerAppInfo.get(appId.toString()).get(appAttemptId.toString()) == null) {
      Map<String, FiCaSchedulerAppInfo> map = fiCaSchedulerAppInfo.get(appId.toString());
      String appAttemptIdString = appAttemptId.toString();
      FiCaSchedulerAppInfo appInfo = new FiCaSchedulerAppInfo(appAttemptId, transactionState);
      map.put(appAttemptIdString, appInfo);
//      fiCaSchedulerAppInfo.get(appId.toString())
//          .put(appAttemptId.toString(), new FiCaSchedulerAppInfo(appAttemptId));
    }
    Map<String, FiCaSchedulerAppInfo> map = fiCaSchedulerAppInfo.get(appId.toString());
    String appAttemptIdString = appAttemptId.toString();
    return map.get(appAttemptIdString);
//    return fiCaSchedulerAppInfo.get(appId.toString()).get(appAttemptId.toString());
    }finally{
      fiCaSchedulerAppInfoLock.unlock();
    }
  }

  private void persistFiCaSchedulerAppInfo(StorageConnector connector) throws StorageException {
    if(!fiCaSchedulerAppInfo.isEmpty()){
    long start = System.currentTimeMillis();
    AgregatedAppInfo agregatedAppInfo = new AgregatedAppInfo();
    for (Map<String,FiCaSchedulerAppInfo> map : fiCaSchedulerAppInfo.values()) {
      for(FiCaSchedulerAppInfo appInfo: map.values()){
        appInfo.agregate(agregatedAppInfo);
      }
    }
    long t1=System.currentTimeMillis()-start;
    agregatedAppInfo.persist();
    long t2 = System.currentTimeMillis()-start;
    if(t2>100){
      LOG.error("persist fica scheduler app info too long: " + t1 + ", " + t2);
    }
    }
  }

  public void setFiCaSchedulerAppInfo(
      SchedulerApplicationAttempt schedulerApp) {
    
    ApplicationId appId = schedulerApp.getApplicationId();
    FiCaSchedulerAppInfo ficaInfo = new FiCaSchedulerAppInfo(schedulerApp, transactionState);
    fiCaSchedulerAppInfoLock.lock();
    if(fiCaSchedulerAppInfo.get(appId.toString())==null){
      fiCaSchedulerAppInfo.put(appId.toString(), new HashMap<String, FiCaSchedulerAppInfo>());
    }
    fiCaSchedulerAppInfo.get(appId.toString())
        .put(schedulerApp.getApplicationAttemptId().toString(), ficaInfo);
    fiCaSchedulerAppInfoLock.unlock();
  }

}
